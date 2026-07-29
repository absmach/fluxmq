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
	"sync"
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
	testCertificateURI = "spiffe://absmach/atom/audit-publisher"
)

type localAuthenticatorStub struct {
	mu             sync.Mutex
	calls          int
	clientID       string
	username       string
	secret         string
	peer           VerifiedPeerIdentity
	principalID    string
	credentialFP   string
	certificateURI string
	authenticated  bool
	err            error
}

func (s *localAuthenticatorStub) AuthenticateLocal(_ context.Context, clientID, username, secret string, peer VerifiedPeerIdentity) (string, string, string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.clientID = clientID
	s.username = username
	s.secret = secret
	s.peer = peer
	return s.principalID, s.credentialFP, s.certificateURI, s.authenticated, s.err
}

type localAuthorizerStub struct {
	allowExchange   string
	allowRoutingKey string
	retired         bool
	lastIdentity    LocalSessionIdentity
	calls           int
}

func (s *localAuthorizerStub) CanPublishLocal(identity LocalSessionIdentity, exchange, routingKey string) bool {
	s.calls++
	s.lastIdentity = identity
	return !s.retired && exchange == s.allowExchange && routingKey == s.allowRoutingKey
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
		connID:   "test-conn",
		channels: make(map[uint16]*Channel),
		peer: VerifiedPeerIdentity{
			URISANs:                []string{testCertificateURI},
			CertificateFingerprint: "certificate-fingerprint",
		},
	}, buf
}

func bindLocalIdentity(conn *Connection) {
	conn.localIdentity = &LocalSessionIdentity{
		PrincipalID:            testLocalPrincipal,
		CredentialFingerprint:  testCredentialFP,
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
		certificateURI: testCertificateURI,
		authenticated:  true,
	}
	authz := &localAuthorizerStub{}
	conn, _ := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(authn, authz, authz, 0))
	globalExternal := &externalAuthenticatorStub{}
	conn.broker.SetAuthEngine(corebroker.NewAuthEngine(globalExternal, nil))
	start := &codec.ConnectionStartOk{Mechanism: "PLAIN", Response: "\x00atom-audit-publisher\x00secret"}
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
	if identity.PrincipalID != testLocalPrincipal || identity.CredentialFingerprint != testCredentialFP || identity.CertificateURI != testCertificateURI {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestLocalAuthenticationRejectsUnverifiedSelectedURI(t *testing.T) {
	authn := &localAuthenticatorStub{
		principalID:    testLocalPrincipal,
		credentialFP:   testCredentialFP,
		certificateURI: "spiffe://attacker.invalid/publisher",
		authenticated:  true,
	}
	authz := &localAuthorizerStub{}
	conn, _ := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(authn, authz, authz, 0))
	start := &codec.ConnectionStartOk{Mechanism: "PLAIN", Response: "\x00atom-audit-publisher\x00secret"}
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
	start := &codec.ConnectionStartOk{Mechanism: "PLAIN", Response: "\x00remote-user\x00remote-secret"}
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
		{"queue declare", &codec.QueueDeclare{Queue: "atom-audit"}, codec.ClassQueue, codec.MethodQueueDeclare},
		{"consume", &codec.BasicConsume{Queue: "atom-audit"}, codec.ClassBasic, codec.MethodBasicConsume},
		{"get", &codec.BasicGet{Queue: "atom-audit"}, codec.ClassBasic, codec.MethodBasicGet},
		{"transaction", &codec.TxSelect{}, codec.ClassTx, codec.MethodTxSelect},
	}
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	for _, tc := range denied {
		t.Run(tc.name, func(t *testing.T) {
			conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
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
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	ch := newChannel(conn, 1)

	if err := ch.handleMethod(&codec.QueueDeclare{Queue: "atom-audit"}); err != nil {
		t.Fatalf("deny queue declare: %v", err)
	}
	if !ch.serverClosing.Load() {
		t.Fatal("channel did not enter server-closing state after denial")
	}
	if err := ch.handleMethod(&codec.BasicPublish{RoutingKey: "atom-audit"}); err != nil {
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
	authz := &localAuthorizerStub{allowExchange: "", allowRoutingKey: "atom-audit"}
	tests := []struct {
		name       string
		exchange   string
		routingKey string
		allowed    bool
	}{
		{"exact target", "", "atom-audit", true},
		{"explicit default alias is not exact", "amq.default", "atom-audit", false},
		{"wrong routing key", "", "other", false},
		{"wrong exchange", "events", "atom-audit", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
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

func TestLocalPolicyBypassesBrokerGlobalHooks(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, _ := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	hook := &hookCounter{}
	conn.broker.SetBlockingHooks(corebroker.NewBlockingHookEngine(hook, corebroker.HookFailDeny, nil, nil, nil))
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: "atom-audit", Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm

	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "atom-audit"}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")
	ch.completePublish()

	if hook.calls != 0 {
		t.Fatalf("global hook calls = %d, want 0", hook.calls)
	}
	if qm.publishCalls != 0 {
		t.Fatalf("general queue publish calls = %d, want 0", qm.publishCalls)
	}
	if qm.exactPublishCalls != 1 || qm.exactStreamName != "atom-audit" || qm.exactPublish.Topic != "$queue/atom-audit" {
		t.Fatalf("unexpected exact stream publish: calls=%d queue=%q request=%+v", qm.exactPublishCalls, qm.exactStreamName, qm.exactPublish)
	}
}

func TestLocalDurableStreamAppendFailureNacksConfirm(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{exactPublishErr: errors.New("disk full")}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "atom-audit"}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	ch.completePublish()

	if qm.publishCalls != 0 || qm.exactPublishCalls != 1 || qm.exactStreamName != "atom-audit" {
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

func TestLocalDurableStreamOversizeFailureNacksConfirm(t *testing.T) {
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{exactPublishErr: queue.ErrQueueMessageTooLarge}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "atom-audit"}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	ch.completePublish()

	if qm.publishCalls != 0 || qm.exactPublishCalls != 1 || qm.exactStreamName != "atom-audit" {
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
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	ch.confirmMode = true
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "atom-audit"}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: 2}
	ch.pendingBody = []byte("{}")

	ch.completePublish()

	if qm.publishCalls != 0 || qm.exactPublishCalls != 1 || qm.exactStreamName != "atom-audit" {
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
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: "atom-audit", Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	if err := ch.handleMethod(&codec.BasicPublish{RoutingKey: "atom-audit"}); err != nil {
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
		certificateURI: testCertificateURI,
		authenticated:  true,
	}
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, _ := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(authn, authz, authz, 0))
	transport := &memoryConn{}
	conn.conn = transport
	conn.writer = bufio.NewWriter(transport)
	conn.closeCh = make(chan struct{})
	start := &codec.ConnectionStartOk{Mechanism: "PLAIN", Response: "\x00atom-audit-publisher\x00old-secret"}
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
	authz := &localAuthorizerStub{allowRoutingKey: "atom-audit"}
	conn, buf := newPolicyTestConnection(t, NewLocalPublishOnlyConnectionPolicy(nil, authz, authz, 0))
	bindLocalIdentity(conn)
	qm := &mockChannelQueueManager{queueCfg: &qtypes.QueueConfig{Name: "atom-audit", Type: qtypes.QueueTypeStream}}
	conn.broker.queueManager = qm
	ch := newChannel(conn, 1)
	if err := ch.handleMethod(&codec.BasicPublish{RoutingKey: "atom-audit"}); err != nil {
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

func TestExplicitExternalPolicyBypassesBrokerGlobalHooks(t *testing.T) {
	conn, _ := newPolicyTestConnection(t, NewExternalConnectionPolicy(nil, nil, 0))
	hook := &hookCounter{}
	conn.broker.SetBlockingHooks(corebroker.NewBlockingHookEngine(hook, corebroker.HookFailDeny, nil, nil, nil))
	var delivered int
	conn.broker.SetCrossDeliver(func(context.Context, string, string, []byte, byte, map[string]string) {
		delivered++
	})
	if err := conn.broker.router.Subscribe("mqtt-client", "atom-audit", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe test route: %v", err)
	}

	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "atom-audit"}
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
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "atom-audit"}
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
		queueCfg: &qtypes.QueueConfig{Name: "atom-audit", Type: qtypes.QueueTypeStream},
	}
	if !newChannel(conn, 1).isStreamQueue("atom-audit") {
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
