// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/queue"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

const (
	testLocalPrincipal = "atom-audit-publisher"
	testCredentialFP   = "credential-fingerprint"
	testPermissionsFP  = "permissions-fingerprint"
	testCertificateURI = "spiffe://absmach/atom/audit-publisher"
	testConnectionID   = "test-conn"
	testAuditQueue     = "atom-audit"
)

type localAuthenticatorStub struct {
	mu             sync.Mutex
	calls          int
	clientID       string
	username       string
	secret         string
	peer           VerifiedPeerIdentity
	principalID    string
	role           LocalPrincipalRole
	credentialFP   string
	permissionsFP  string
	certificateURI string
	authenticated  bool
	err            error
}

func (s *localAuthenticatorStub) AuthenticateLocal(_ context.Context, clientID, username, secret string, peer VerifiedPeerIdentity) (LocalAuthentication, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.clientID = clientID
	s.username = username
	s.secret = secret
	s.peer = peer
	return LocalAuthentication{
		PrincipalID:            s.principalID,
		Role:                   s.role,
		CredentialFingerprint:  s.credentialFP,
		PermissionsFingerprint: s.permissionsFP,
		CertificateURI:         s.certificateURI,
	}, s.authenticated, s.err
}

type localAuthorizerStub struct {
	allowExchange         string
	allowRoutingKey       string
	allowRoutingKeyPrefix string
	allowQueue            string
	retired               bool
	lastIdentity          LocalSessionIdentity
	calls                 int
	subscribeCalls        int
}

func (s *localAuthorizerStub) CanPublishLocal(identity LocalSessionIdentity, exchange, routingKey string) LocalPublishGrant {
	s.calls++
	s.lastIdentity = identity
	if s.retired {
		return LocalPublishGrantNone
	}
	if exchange == s.allowExchange && routingKey == s.allowRoutingKey {
		return LocalPublishGrantExactTarget
	}
	if s.allowRoutingKeyPrefix != "" && exchange == "" && strings.HasPrefix(routingKey, s.allowRoutingKeyPrefix) {
		return LocalPublishGrantPrefix
	}
	return LocalPublishGrantNone
}

func (s *localAuthorizerStub) CanSubscribeLocal(identity LocalSessionIdentity, queue string) bool {
	s.subscribeCalls++
	s.lastIdentity = identity
	return !s.retired && queue != "" && queue == s.allowQueue
}

func (s *localAuthorizerStub) IsSessionActive(identity LocalSessionIdentity) bool {
	s.lastIdentity = identity
	return !s.retired
}

type externalAuthenticatorStub struct {
	calls int
}

func (s *externalAuthenticatorStub) Authenticate(_, _, _ string) (*corebroker.AuthnResult, error) {
	s.calls++
	return &corebroker.AuthnResult{Authenticated: true, ID: "external-principal"}, nil
}

type hookCounter struct {
	calls int
}

func (h *hookCounter) HandleHook(_ context.Context, _ corebroker.BlockingHookRequest) (corebroker.BlockingHookResult, error) {
	h.calls++
	return corebroker.BlockingHookResult{Allowed: true}, nil
}

type memoryConn struct {
	bytes.Buffer
	closed bool
}

func (c *memoryConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *memoryConn) Close() error                     { c.closed = true; return nil }
func (c *memoryConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *memoryConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *memoryConn) SetDeadline(time.Time) error      { return nil }
func (c *memoryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memoryConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func newPolicyTestConnection(t *testing.T, policy *ConnectionPolicy) (*Connection, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Connection{
		broker:   New(nil, logger),
		ctx:      context.Background(),
		policy:   policy,
		writer:   bufio.NewWriter(buf),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   testConnectionID,
		channels: make(map[uint16]*Channel),
		peer: VerifiedPeerIdentity{
			URISANs:                []string{testCertificateURI},
			CertificateFingerprint: "certificate-fingerprint",
		},
	}, buf
}

func bindLocalIdentity(conn *Connection) {
	bindLocalIdentityAs(conn, LocalRolePublisher)
}

// bindLocalIdentityAs binds an authenticated identity carrying role, which is
// what decides a session's capability now that listeners no longer do.
func bindLocalIdentityAs(conn *Connection, role LocalPrincipalRole) {
	conn.localIdentity = &LocalSessionIdentity{
		Role:                   role,
		PrincipalID:            testLocalPrincipal,
		CredentialFingerprint:  testCredentialFP,
		PermissionsFingerprint: testPermissionsFP,
		CertificateURI:         testCertificateURI,
		CertificateFingerprint: "certificate-fingerprint",
	}
}

func decodeSingleChannelClose(t *testing.T, buf *bytes.Buffer) *codec.ChannelClose {
	t.Helper()
	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected one frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode channel close: %v", err)
	}
	closeMethod, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	return closeMethod
}

func TestLocalAuthenticationBindsVerifiedIdentity(t *testing.T) {
	authn := &localAuthenticatorStub{
		principalID:    testLocalPrincipal,
		credentialFP:   testCredentialFP,
		permissionsFP:  testPermissionsFP,
		certificateURI: testCertificateURI,
		authenticated:  true,
	}
	authz := &localAuthorizerStub{}
	conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(authn, authz, authz, 0))
	globalExternal := &externalAuthenticatorStub{}
	conn.broker.SetAuthEngine(corebroker.NewAuthEngine(globalExternal, nil))
	start := &codec.ConnectionStartOk{Mechanism: saslMechanismPlain, Response: "\x00atom-audit-publisher\x00secret"}
	if err := conn.authenticate(start); err != nil {
		t.Fatalf("authenticate failed: %v", err)
	}
	if authn.calls != 1 || authn.username != testLocalPrincipal || authn.secret != "secret" {
		t.Fatalf("unexpected local auth call: %+v", authn)
	}
	if globalExternal.calls != 0 {
		t.Fatalf("global external authenticator calls = %d, want 0", globalExternal.calls)
	}
	if got := conn.broker.stats.GetLocalAuthSuccess(); got != 1 {
		t.Fatalf("local auth successes = %d, want 1", got)
	}
	if got := conn.broker.stats.GetLocalAuthFailures(); got != 0 {
		t.Fatalf("local auth failures = %d, want 0", got)
	}
	identity, ok := conn.localSessionIdentity()
	if !ok {
		t.Fatal("expected local session identity")
	}
	if identity.PrincipalID != testLocalPrincipal || identity.CredentialFingerprint != testCredentialFP || identity.PermissionsFingerprint != testPermissionsFP || identity.CertificateURI != testCertificateURI {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestLocalAuthenticationRejectsUnverifiedSelectedURI(t *testing.T) {
	authn := &localAuthenticatorStub{
		principalID:    testLocalPrincipal,
		credentialFP:   testCredentialFP,
		permissionsFP:  testPermissionsFP,
		certificateURI: "spiffe://attacker.invalid/publisher",
		authenticated:  true,
	}
	authz := &localAuthorizerStub{}
	conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(authn, authz, authz, 0))
	start := &codec.ConnectionStartOk{Mechanism: saslMechanismPlain, Response: "\x00atom-audit-publisher\x00secret"}
	if err := conn.authenticate(start); err == nil {
		t.Fatal("expected authentication rejection")
	}
	if _, ok := conn.localSessionIdentity(); ok {
		t.Fatal("rejected authentication must not bind an identity")
	}
	if got := conn.broker.stats.GetLocalAuthFailures(); got != 1 {
		t.Fatalf("local auth failures = %d, want 1", got)
	}
}

func TestExternalPolicyUsesOnlyExternalAuth(t *testing.T) {
	external := &externalAuthenticatorStub{}
	engine := corebroker.NewAuthEngine(external, nil)
	conn, _ := newPolicyTestConnection(t, NewExternalConnectionPolicy(engine, nil, 0))
	globalExternal := &externalAuthenticatorStub{}
	conn.broker.SetAuthEngine(corebroker.NewAuthEngine(globalExternal, nil))
	start := &codec.ConnectionStartOk{Mechanism: saslMechanismPlain, Response: "\x00remote-user\x00remote-secret"}
	if err := conn.authenticate(start); err != nil {
		t.Fatalf("external authentication failed: %v", err)
	}
	if external.calls != 1 {
		t.Fatalf("external authenticator calls = %d, want 1", external.calls)
	}
	if globalExternal.calls != 0 {
		t.Fatalf("broker-global authenticator calls = %d, want 0", globalExternal.calls)
	}
	if _, ok := conn.localSessionIdentity(); ok {
		t.Fatal("external policy must not bind a local identity")
	}
}

func TestLocalPublishOnlyMethodAllowlist(t *testing.T) {
	denied := []struct {
		name   string
		method any
		class  uint16
		id     uint16
	}{
		{"exchange declare", &codec.ExchangeDeclare{Exchange: "events"}, codec.ClassExchange, codec.MethodExchangeDeclare},
		{"queue declare", &codec.QueueDeclare{Queue: testAuditQueue}, codec.ClassQueue, codec.MethodQueueDeclare},
		{"consume", &codec.BasicConsume{Queue: testAuditQueue}, codec.ClassBasic, codec.MethodBasicConsume},
		{"get", &codec.BasicGet{Queue: testAuditQueue}, codec.ClassBasic, codec.MethodBasicGet},
		{"transaction", &codec.TxSelect{}, codec.ClassTx, codec.MethodTxSelect},
	}
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	for _, tc := range denied {
		t.Run(tc.name, func(t *testing.T) {
			conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
			bindLocalIdentity(conn)
			ch := newChannel(conn, 1)
			if err := ch.handleMethod(tc.method); err != nil {
				t.Fatalf("handle method: %v", err)
			}
			got := decodeSingleChannelClose(t, buf)
			if got.ReplyCode != codec.AccessRefused || got.ClassID != tc.class || got.MethodID != tc.id {
				t.Fatalf("unexpected denial: %+v", got)
			}
			if count := conn.broker.stats.GetLocalOperationDenials(); count != 1 {
				t.Fatalf("operation denials = %d, want 1", count)
			}
		})
	}
}

func TestServerClosedChannelIgnoresPublishAfterDeniedMethod(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	ch := newChannel(conn, 1)

	if err := ch.handleMethod(&codec.QueueDeclare{Queue: testAuditQueue}); err != nil {
		t.Fatalf("deny queue declare: %v", err)
	}
	if !ch.serverClosing.Load() {
		t.Fatal("channel did not enter server-closing state after denial")
	}
	if err := ch.handleMethod(&codec.BasicPublish{RoutingKey: testAuditQueue}); err != nil {
		t.Fatalf("publish while closing: %v", err)
	}
	if ch.pendingMethod != nil {
		t.Fatal("publish entered content state after server sent Channel.Close")
	}

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want only the original Channel.Close", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode channel close: %v", err)
	}
	if _, ok := decoded.(*codec.ChannelClose); !ok {
		t.Fatalf("frame = %T, want ChannelClose", decoded)
	}
}

func TestLocalPublishRequiresExactExchangeAndRoutingKey(t *testing.T) {
	authz := &localAuthorizerStub{allowExchange: "", allowRoutingKey: testAuditQueue}
	tests := []struct {
		name       string
		exchange   string
		routingKey string
		allowed    bool
	}{
		{"exact target", "", testAuditQueue, true},
		// amq.default names the same default exchange the router resolves "" to,
		// so the ACL must reach the same decision for both spellings.
		{"explicit default alias", "amq.default", testAuditQueue, true},
		{"wrong routing key", "", testOtherTarget, false},
		{"wrong exchange", "events", testAuditQueue, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
			bindLocalIdentity(conn)
			ch := newChannel(conn, 1)
			err := ch.handleMethod(&codec.BasicPublish{Exchange: tc.exchange, RoutingKey: tc.routingKey})
			if err != nil {
				t.Fatalf("handle publish: %v", err)
			}
			if tc.allowed {
				if ch.pendingMethod == nil || buf.Len() != 0 {
					t.Fatal("expected publish to enter content state without denial")
				}
				return
			}
			if got := decodeSingleChannelClose(t, buf); got.ReplyCode != codec.AccessRefused {
				t.Fatalf("reply code = %d, want %d", got.ReplyCode, codec.AccessRefused)
			}
			if count := conn.broker.stats.GetLocalPublishDenials(); count != 1 {
				t.Fatalf("publish denials = %d, want 1", count)
			}
		})
	}
}

// A local principal's identity is fixed by configuration, so its publications
// are stamped with the authenticated principal even though the listener is
// trusted. Relaying another origin would make an audit record disagree with the
// peer that actually authenticated.
func TestLocalPrincipalStampsOwnIdentityOverRelayedOrigin(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm

	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		BodySize: 2,
		Properties: codec.BasicProperties{
			Headers: map[string]any{
				corebroker.ExternalIDProperty: "pub-123",
				corebroker.ProtocolProperty:   corebroker.ProtocolHTTP,
			},
		},
	}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if qm.exactPublishCalls != 1 {
		t.Fatalf("exact stream publish calls = %d, want 1", qm.exactPublishCalls)
	}
	if got := qm.exactPublish.Properties[corebroker.ExternalIDProperty]; got != testLocalPrincipal {
		t.Fatalf("external_id = %q, want %q", got, testLocalPrincipal)
	}
	if got := qm.exactPublish.Properties[corebroker.ProtocolProperty]; got != corebroker.ProtocolAMQP091 {
		t.Fatalf("protocol = %q, want %q", got, corebroker.ProtocolAMQP091)
	}
}

// The matched permission, not the listener, selects the delivery path. An exact
// target names a protected stream and is appended durably; a prefix names no
// queue and is published as an ordinary topic. Routing by listener instead would
// make one permissions.publish entry mean two different contracts.
func TestLocalPublishPathFollowsGrantKind(t *testing.T) {
	const prefixedKey = "m.domain.c.channel"

	tests := []struct {
		name             string
		role             LocalPrincipalRole
		routingKey       string
		wantDurableCalls int
	}{
		{
			name:             "publisher exact target appends durably",
			role:             LocalRolePublisher,
			routingKey:       testAuditQueue,
			wantDurableCalls: 1,
		},
		{
			name:             "service exact target appends durably",
			role:             LocalRoleService,
			routingKey:       testAuditQueue,
			wantDurableCalls: 1,
		},
		{
			name:             "service prefix publishes as an ordinary topic",
			role:             LocalRoleService,
			routingKey:       prefixedKey,
			wantDurableCalls: 0,
		},
		{
			name:             "publisher prefix publishes as an ordinary topic",
			role:             LocalRolePublisher,
			routingKey:       prefixedKey,
			wantDurableCalls: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue, allowRoutingKeyPrefix: "m."}
			conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
			bindLocalIdentityAs(conn, tc.role)
			qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
			conn.broker.queueManager = qm

			ch := newChannel(conn, 1)
			ch.pendingMethod = &codec.BasicPublish{RoutingKey: tc.routingKey}
			ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
			ch.pendingBody = []byte("{}")
			ch.completePublish()

			if qm.exactPublishCalls != tc.wantDurableCalls {
				t.Fatalf("durable stream publish calls = %d, want %d", qm.exactPublishCalls, tc.wantDurableCalls)
			}
			if tc.wantDurableCalls > 0 && qm.exactStreamName != tc.routingKey {
				t.Fatalf("durable stream = %q, want %q", qm.exactStreamName, tc.routingKey)
			}
		})
	}
}

func TestLocalPolicyBypassesBrokerGlobalHooks(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	hook := &hookCounter{}
	conn.broker.SetBlockingHooks(corebroker.NewBlockingHookEngine(hook, corebroker.HookFailDeny, nil, nil, nil))
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm

	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if hook.calls != 0 {
		t.Fatalf("global hook calls = %d, want 0", hook.calls)
	}
	if qm.publishCalls != 0 {
		t.Fatalf("general queue publish calls = %d, want 0", qm.publishCalls)
	}
	if qm.exactPublishCalls != 1 || qm.exactStreamName != testAuditQueue || qm.exactPublish.Topic != "$queue/atom-audit" {
		t.Fatalf("unexpected exact stream publish: calls=%d queue=%q request=%+v", qm.exactPublishCalls, qm.exactStreamName, qm.exactPublish)
	}
}

func TestLocalDurableStreamAppendFailureNacksConfirm(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{exactPublishErr: errors.New("disk full")}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	ch.completePublish()

	if qm.publishCalls != 0 || qm.exactPublishCalls != 1 || qm.exactStreamName != testAuditQueue {
		t.Fatalf("unexpected routing: general=%d exact=%d queue=%q", qm.publishCalls, qm.exactPublishCalls, qm.exactStreamName)
	}
	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode publisher response: %v", err)
	}
	if _, ok := decoded.(*codec.BasicNack); !ok {
		t.Fatalf("response = %T, want BasicNack", decoded)
	}
}

func TestLocalDurableStreamPublishIsBounded(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm

	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if qm.exactPublishCtx == nil {
		t.Fatal("durable stream publish did not receive a context")
	}
	deadline, ok := qm.exactPublishCtx.Deadline()
	if !ok {
		t.Fatal("durable stream publish context has no deadline; a stalled disk would pin the connection goroutine")
	}
	if remaining := time.Until(deadline); remaining > localPublishTimeout {
		t.Fatalf("deadline in %v, want at most %v", remaining, localPublishTimeout)
	}
}

// blockingStreamQueueManager stalls inside the durable append the way an
// unresponsive disk does: the barrier cannot be interrupted, so the deadline
// has to be enforced by the caller waiting on it.
type blockingStreamQueueManager struct {
	mockChannelQueueManager
	entered chan struct{}
	release chan struct{}
}

func (m *blockingStreamQueueManager) PublishToDurableStream(_ context.Context, _ string, _ qtypes.PublishRequest) error {
	close(m.entered)
	<-m.release
	return nil
}

// stalledStreamQueueManager never completes an append, the way storage that has
// stopped responding behaves, and records how many appends were running at once.
type stalledStreamQueueManager struct {
	mockChannelQueueManager
	release  chan struct{}
	entered  chan struct{}
	current  atomic.Int64
	peak     atomic.Int64
	attempts atomic.Int64
}

func (m *stalledStreamQueueManager) PublishToDurableStream(_ context.Context, _ string, _ qtypes.PublishRequest) error {
	m.attempts.Add(1)
	running := m.current.Add(1)
	for {
		peak := m.peak.Load()
		if running <= peak || m.peak.CompareAndSwap(peak, running) {
			break
		}
	}
	select {
	case m.entered <- struct{}{}:
	default:
	}
	<-m.release
	m.current.Add(-1)
	return nil
}

// assertAbandonedPublishResponse checks the response to a publication FluxMQ
// stopped waiting for: a NACK, then a channel close so the publisher cannot
// keep retrying into storage that has not recovered.
func assertAbandonedPublishResponse(t *testing.T, buf *bytes.Buffer) {
	t.Helper()
	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2 (nack and channel close)", len(frames))
	}
	nack, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode publisher response: %v", err)
	}
	if _, ok := nack.(*codec.BasicNack); !ok {
		t.Fatalf("first response = %T, want BasicNack", nack)
	}
	closeMethod, err := frames[1].Decode()
	if err != nil {
		t.Fatalf("decode channel close: %v", err)
	}
	if _, ok := closeMethod.(*codec.ChannelClose); !ok {
		t.Fatalf("second response = %T, want ChannelClose", closeMethod)
	}
}

func TestLocalDurableStreamPublishBoundsAbandonedAppends(t *testing.T) {
	previousTimeout := localPublishTimeout
	previousLimit := maxOutstandingDurableAppends
	localPublishTimeout = 20 * time.Millisecond
	maxOutstandingDurableAppends = 2
	t.Cleanup(func() {
		localPublishTimeout = previousTimeout
		maxOutstandingDurableAppends = previousLimit
	})

	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &stalledStreamQueueManager{
		release: make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	releaseOnce := sync.Once{}
	release := func() { releaseOnce.Do(func() { close(qm.release) }) }
	t.Cleanup(release)
	conn.broker.queueManager = qm

	// A publisher that keeps retrying against storage that never recovers must
	// not be able to accumulate barriers, each holding its payload.
	const attempts = 8
	for i := range attempts {
		ch := newChannel(conn, uint16(i+1))
		ch.confirmMode = true
		ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
		ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
		ch.pendingBody = []byte("{}")
		ch.completePublish()
	}

	if peak := qm.peak.Load(); peak > int64(maxOutstandingDurableAppends) {
		t.Fatalf("concurrent durable appends peaked at %d, want at most %d", peak, maxOutstandingDurableAppends)
	}
	if started := qm.attempts.Load(); started != int64(maxOutstandingDurableAppends) {
		t.Fatalf("started appends = %d, want %d; later attempts must be refused before starting work", started, maxOutstandingDurableAppends)
	}
	if outstanding := conn.broker.durableAppends.outstandingFor(testAuditQueue); outstanding != maxOutstandingDurableAppends {
		t.Fatalf("outstanding appends = %d, want %d", outstanding, maxOutstandingDurableAppends)
	}
	if timeouts := conn.broker.stats.GetLocalPublishTimeouts(); timeouts != uint64(maxOutstandingDurableAppends) {
		t.Fatalf("publish timeouts = %d, want %d", timeouts, maxOutstandingDurableAppends)
	}
	if rejections := conn.broker.stats.GetLocalPublishRejections(); rejections != attempts-uint64(maxOutstandingDurableAppends) {
		t.Fatalf("publish rejections = %d, want %d", rejections, attempts-maxOutstandingDurableAppends)
	}

	// Every attempt is answered, and each channel is closed so the publisher
	// cannot keep retrying into storage that has not recovered.
	nacks, closes := 0, 0
	for _, frame := range readFramesFrom(t, buf, 0) {
		decoded, err := frame.Decode()
		if err != nil {
			t.Fatalf("decode publisher response: %v", err)
		}
		switch decoded.(type) {
		case *codec.BasicNack:
			nacks++
		case *codec.ChannelClose:
			closes++
		default:
			t.Fatalf("unexpected response %T", decoded)
		}
	}
	if nacks != attempts || closes != attempts {
		t.Fatalf("nacks = %d, channel closes = %d, want %d of each", nacks, closes, attempts)
	}

	// Once storage recovers, the slots are returned and publishing resumes.
	release()
	deadline := time.After(5 * time.Second)
	for conn.broker.durableAppends.outstandingFor(testAuditQueue) != 0 {
		select {
		case <-deadline:
			t.Fatal("durable append slots were not released after storage recovered")
		default:
		}
	}

	localPublishTimeout = previousTimeout
	healthy := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = healthy
	ch := newChannel(conn, attempts+1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()
	if healthy.exactPublishCalls != 1 {
		t.Fatalf("publish calls after recovery = %d, want 1", healthy.exactPublishCalls)
	}
}

func TestLocalDurableStreamPublishNacksWhenBarrierStalls(t *testing.T) {
	previousTimeout := localPublishTimeout
	localPublishTimeout = 50 * time.Millisecond
	t.Cleanup(func() { localPublishTimeout = previousTimeout })

	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &blockingStreamQueueManager{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(qm.release) })
	conn.broker.queueManager = qm

	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	completed := make(chan struct{})
	go func() {
		ch.completePublish()
		close(completed)
	}()

	select {
	case <-qm.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("durable append was never entered")
	}
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("publish did not return while the durable barrier was stalled")
	}

	assertAbandonedPublishResponse(t, buf)
	if count := conn.broker.stats.GetLocalPublishTimeouts(); count != 1 {
		t.Fatalf("publish timeouts = %d, want 1", count)
	}
}

func TestLocalDurableStreamPublishNacksOnConnectionShutdown(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	ctx, cancel := context.WithCancel(context.Background())
	conn.ctx = ctx
	cancel()
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm

	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	assertAbandonedPublishResponse(t, buf)
}

func TestLocalDurableStreamOversizeFailureNacksConfirm(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{exactPublishErr: queue.ErrQueueMessageTooLarge}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	ch.completePublish()

	if qm.publishCalls != 0 || qm.exactPublishCalls != 1 || qm.exactStreamName != testAuditQueue {
		t.Fatalf("unexpected routing: general=%d exact=%d queue=%q", qm.publishCalls, qm.exactPublishCalls, qm.exactStreamName)
	}
	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode publisher response: %v", err)
	}
	if _, ok := decoded.(*codec.BasicNack); !ok {
		t.Fatalf("response = %T, want BasicNack", decoded)
	}
}

func TestLocalDurableStreamSuccessAcksExactQueueOnly(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	ch.completePublish()

	if qm.publishCalls != 0 || qm.exactPublishCalls != 1 || qm.exactStreamName != testAuditQueue {
		t.Fatalf("unexpected routing: general=%d exact=%d queue=%q", qm.publishCalls, qm.exactPublishCalls, qm.exactStreamName)
	}
	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode publisher response: %v", err)
	}
	if _, ok := decoded.(*codec.BasicAck); !ok {
		t.Fatalf("response = %T, want BasicAck", decoded)
	}
}

func TestLocalPublishRechecksAuthorizationAfterContent(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	if err := ch.handleMethod(&codec.BasicPublish{RoutingKey: testAuditQueue}); err != nil {
		t.Fatalf("start publish: %v", err)
	}
	authz.allowRoutingKey = "revoked"
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if qm.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0 after ACL revocation", qm.publishCalls)
	}
	if got := decodeSingleChannelClose(t, buf); got.ReplyCode != codec.AccessRefused {
		t.Fatalf("reply code = %d, want %d", got.ReplyCode, codec.AccessRefused)
	}
}

func TestLocalCredentialRetiredBeforeRegistrationIsUnregistered(t *testing.T) {
	authn := &localAuthenticatorStub{
		principalID:    testLocalPrincipal,
		credentialFP:   testCredentialFP,
		permissionsFP:  testPermissionsFP,
		certificateURI: testCertificateURI,
		authenticated:  true,
	}
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, _ := newPolicyTestConnection(t, NewLocalConnectionPolicy(authn, authz, authz, 0))
	transport := &memoryConn{}
	conn.conn = transport
	conn.writer = bufio.NewWriter(transport)
	conn.closeCh = make(chan struct{})
	start := &codec.ConnectionStartOk{Mechanism: saslMechanismPlain, Response: "\x00atom-audit-publisher\x00old-secret"}
	if err := conn.authenticate(start); err != nil {
		t.Fatalf("authenticate before reload: %v", err)
	}

	// Model a reload after authentication but before run registers the connection.
	// The registry scan cannot see this handshake.
	authz.retired = true
	if got := conn.broker.DisconnectInvalidLocalSessions(func(LocalSessionIdentity) bool { return false }); got != 0 {
		t.Fatalf("pre-registration disconnect count = %d, want 0", got)
	}
	if err := conn.registerAndValidate(); err == nil {
		t.Fatal("registerAndValidate() accepted a retired credential")
	}
	if !conn.broker.HasConnection(conn.connID) {
		t.Fatal("test did not exercise the register-before-revalidate ordering")
	}
	conn.cleanup()
	if conn.broker.HasConnection(conn.connID) {
		t.Fatal("retired session remained registered after connection cleanup")
	}
	if got := conn.broker.stats.GetCurrentConnections(); got != 0 {
		t.Fatalf("active connections = %d, want 0", got)
	}
	if got := conn.broker.stats.GetLocalConnections(); got != 0 {
		t.Fatalf("active local connections = %d, want 0", got)
	}
	if got := conn.broker.stats.GetLocalForcedDisconnects(); got != 1 {
		t.Fatalf("forced disconnects = %d, want 1", got)
	}
	if !transport.closed {
		t.Fatal("retired session transport was not closed")
	}
	if authz.lastIdentity.CredentialFingerprint != testCredentialFP || authz.lastIdentity.CertificateURI != testCertificateURI {
		t.Fatalf("session validator did not receive the full identity: %+v", authz.lastIdentity)
	}
}

func TestLocalPublishRejectsCredentialRetiredDuringContent(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: testAuditQueue}
	conn, buf := newPolicyTestConnection(t, NewLocalConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	if err := ch.handleMethod(&codec.BasicPublish{RoutingKey: testAuditQueue}); err != nil {
		t.Fatalf("start publish: %v", err)
	}

	authz.retired = true
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if qm.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0 after credential retirement", qm.publishCalls)
	}
	if got := decodeSingleChannelClose(t, buf); got.ReplyCode != codec.AccessRefused {
		t.Fatalf("reply code = %d, want %d", got.ReplyCode, codec.AccessRefused)
	}
}

// Reserved properties are gated on who authenticated the peer, so only the
// mTLS internal listener may exchange them. A missing policy is the embedded
// caller path and must fail closed.
func TestConnectionPolicyReservedPropertyTrust(t *testing.T) {
	tests := []struct {
		name   string
		policy *ConnectionPolicy
		want   bool
	}{
		{
			name:   "local publish only policy is trusted",
			policy: NewLocalConnectionPolicy(nil, nil, nil, 0),
			want:   true,
		},
		{
			name:   "external policy is not trusted",
			policy: NewExternalConnectionPolicy(nil, nil, 0),
			want:   false,
		},
		{
			name:   "nil policy is not trusted",
			policy: nil,
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.carriesReservedProperties(); got != tc.want {
				t.Fatalf("carriesReservedProperties() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A connection with no policy resolves to the untrusted external default, so an
// embedded caller never silently becomes a trusted service.
func TestAbsentPolicyResolvesUntrusted(t *testing.T) {
	conn, _ := newPolicyTestConnection(t, nil)
	if conn.connectionPolicy().carriesReservedProperties() {
		t.Fatal("connection with no policy must not carry reserved properties")
	}
}

func TestExplicitExternalPolicyBypassesBrokerGlobalHooks(t *testing.T) {
	conn, _ := newPolicyTestConnection(t, NewExternalConnectionPolicy(nil, nil, 0))
	hook := &hookCounter{}
	conn.broker.SetBlockingHooks(corebroker.NewBlockingHookEngine(hook, corebroker.HookFailDeny, nil, nil, nil))
	var delivered int
	conn.broker.SetCrossDeliver(func(context.Context, string, string, []byte, byte, map[string]string) {
		delivered++
	})
	if err := conn.broker.router.Subscribe("mqtt-client", testAuditQueue, 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe test route: %v", err)
	}

	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if hook.calls != 0 {
		t.Fatalf("broker-global hook calls = %d, want 0", hook.calls)
	}
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1", delivered)
	}
}

func TestMessageSizeRejectedBeforeBodyAllocation(t *testing.T) {
	policy := NewExternalConnectionPolicy(nil, nil, 1024)
	conn, buf := newPolicyTestConnection(t, policy)
	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: testAuditQueue}
	header := &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 1025}
	var payload bytes.Buffer
	if err := header.WriteContentHeader(&payload); err != nil {
		t.Fatalf("write content header: %v", err)
	}
	ch.handleHeaderFrame(&codec.Frame{Type: codec.FrameHeader, Channel: 1, Payload: payload.Bytes()})
	if ch.pendingBody != nil || ch.pendingMethod != nil {
		t.Fatal("oversized message must be reset before body allocation")
	}
	if got := decodeSingleChannelClose(t, buf); got.ReplyCode != codec.ContentTooLarge {
		t.Fatalf("reply code = %d, want %d", got.ReplyCode, codec.ContentTooLarge)
	}
}

func TestFrameLimitRejectedBeforePayloadRead(t *testing.T) {
	var wire bytes.Buffer
	if err := codec.WriteOctet(&wire, codec.FrameBody); err != nil {
		t.Fatalf("write frame type: %v", err)
	}
	if err := codec.WriteShort(&wire, 1); err != nil {
		t.Fatalf("write channel: %v", err)
	}
	if err := codec.WriteLong(&wire, defaultFrameMax); err != nil {
		t.Fatalf("write payload size: %v", err)
	}
	conn := &Connection{reader: bufio.NewReader(&wire), frameMax: 4096}
	if _, err := conn.readFrame(); err == nil {
		t.Fatal("expected oversized frame rejection")
	}
}

func TestChannelIDCannotExceedNegotiatedMaximum(t *testing.T) {
	conn, buf := newPolicyTestConnection(t, NewExternalConnectionPolicy(nil, nil, 0))
	conn.channelMax = 1
	if err := conn.handleChannelOpen(2); err != nil {
		t.Fatalf("handle channel open: %v", err)
	}
	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("connection-close frame count = %d, want 1", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("decode connection close: %v", err)
	}
	closeMethod, ok := decoded.(*codec.ConnectionClose)
	if !ok || closeMethod.ReplyCode != codec.ChannelError {
		t.Fatalf("expected connection ChannelError, got %T %+v", decoded, decoded)
	}
	if len(conn.channels) != 0 {
		t.Fatal("out-of-range channel was registered")
	}
}

func TestPreconfiguredStreamDetectedThroughQueueManager(t *testing.T) {
	conn, _ := newPolicyTestConnection(t, NewExternalConnectionPolicy(nil, nil, 0))
	conn.broker.queueManager = &mockChannelQueueManager{
		queueCfg: &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream},
	}
	if !newChannel(conn, 1).isStreamQueue(testAuditQueue) {
		t.Fatal("expected globally preconfigured stream to be detected")
	}
}

func TestDisconnectInvalidLocalSessions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	newConn := func(id string, identity *LocalSessionIdentity) (*Connection, *memoryConn) {
		transport := &memoryConn{}
		conn := &Connection{
			broker:        b,
			conn:          transport,
			writer:        bufio.NewWriter(transport),
			logger:        logger,
			connID:        id,
			localIdentity: identity,
			closeCh:       make(chan struct{}),
			channels:      make(map[uint16]*Channel),
		}
		b.connections.Store(id, conn)
		return conn, transport
	}
	valid, validTransport := newConn("valid", &LocalSessionIdentity{PrincipalID: "valid", CredentialFingerprint: "new"})
	invalid, invalidTransport := newConn("invalid", &LocalSessionIdentity{PrincipalID: "invalid", CredentialFingerprint: "old"})
	remote, remoteTransport := newConn("remote", nil)

	got := b.DisconnectInvalidLocalSessions(func(identity LocalSessionIdentity) bool {
		return identity.CredentialFingerprint == "new"
	})
	if got != 1 {
		t.Fatalf("disconnected = %d, want 1", got)
	}
	if count := b.stats.GetLocalForcedDisconnects(); count != 1 {
		t.Fatalf("forced disconnects = %d, want 1", count)
	}
	if !invalid.closed.Load() || !invalidTransport.closed {
		t.Fatal("invalid local session was not disconnected")
	}
	if valid.closed.Load() || validTransport.closed || remote.closed.Load() || remoteTransport.closed {
		t.Fatal("valid local and external sessions must remain connected")
	}
}

func TestRecordLocalPrincipalReload(t *testing.T) {
	b := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.RecordLocalPrincipalReload(true)
	b.RecordLocalPrincipalReload(true)
	b.RecordLocalPrincipalReload(false)
	if got := b.stats.GetLocalReloadSuccess(); got != 2 {
		t.Fatalf("reload successes = %d, want 2", got)
	}
	if got := b.stats.GetLocalReloadFailures(); got != 1 {
		t.Fatalf("reload failures = %d, want 1", got)
	}
}
