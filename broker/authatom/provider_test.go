// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authatom

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	atomv1 "github.com/absmach/fluxmq/pkg/proto/atom/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	testEntityID     = "33333333-3333-4333-8333-333333333333"
	testServiceToken = "atom_service_token"
)

type fakeAtomServer struct {
	atomv1.UnimplementedAuthServiceServer
	atomv1.UnimplementedAuthzServiceServer
	atomv1.UnimplementedAliasServiceServer

	mu sync.Mutex

	authnToken string
	authnErr   error
	authnCalls int

	aliasErr    error
	aliasObject string
	aliasCalls  int
	lastAlias   *atomv1.ResolveAliasRequest
	aliasAuthz  []string

	checkErr   error
	checkAllow bool
	checkCalls int
	lastCheck  *atomv1.CheckRequest
	checkAuthz []string
}

func (s *fakeAtomServer) Authenticate(_ context.Context, req *atomv1.AuthenticateRequest) (*atomv1.AuthenticateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.authnCalls++
	if s.authnErr != nil {
		return nil, s.authnErr
	}
	if req.GetToken() != s.authnToken {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	return &atomv1.AuthenticateResponse{EntityId: testEntityID}, nil
}

func (s *fakeAtomServer) ResolveAlias(ctx context.Context, req *atomv1.ResolveAliasRequest) (*atomv1.ResolveAliasResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.aliasCalls++
	s.lastAlias = req
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.aliasAuthz = md.Get("authorization")
	}
	if s.aliasErr != nil {
		return nil, s.aliasErr
	}
	return &atomv1.ResolveAliasResponse{ObjectId: s.aliasObject}, nil
}

func (s *fakeAtomServer) Check(ctx context.Context, req *atomv1.CheckRequest) (*atomv1.CheckResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checkCalls++
	s.lastCheck = req
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.checkAuthz = md.Get("authorization")
	}
	if s.checkErr != nil {
		return nil, s.checkErr
	}
	return &atomv1.CheckResponse{Allowed: s.checkAllow, Reason: "test"}, nil
}

func newTestProvider(t *testing.T, fake *fakeAtomServer, opts Options) (*Provider, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	atomv1.RegisterAuthServiceServer(srv, fake)
	atomv1.RegisterAuthzServiceServer(srv, fake)
	atomv1.RegisterAliasServiceServer(srv, fake)

	go func() {
		_ = srv.Serve(lis)
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial fake atom: %v", err)
	}

	if opts.ServiceToken == "" {
		opts.ServiceToken = testServiceToken
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Second
	}

	return New(conn, opts), func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
}

func TestProviderAuthenticateSuccessCachesToken(t *testing.T) {
	fake := &fakeAtomServer{authnToken: "client-token"}
	provider, cleanup := newTestProvider(t, fake, Options{AuthnCacheTTL: time.Hour})
	defer cleanup()

	for i := 0; i < 2; i++ {
		res, err := provider.Authenticate("client", "ignored", "Bearer client-token")
		if err != nil {
			t.Fatalf("Authenticate() error = %v", err)
		}
		if !res.Authenticated || res.ID != testEntityID {
			t.Fatalf("Authenticate() = %#v", res)
		}
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.authnCalls != 1 {
		t.Fatalf("authn calls = %d, want 1", fake.authnCalls)
	}
}

func TestProviderAuthenticateInvalidToken(t *testing.T) {
	fake := &fakeAtomServer{authnToken: "good-token"}
	provider, cleanup := newTestProvider(t, fake, Options{})
	defer cleanup()

	res, err := provider.Authenticate("client", "ignored", "bad-token")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if res.Authenticated {
		t.Fatalf("Authenticate() authenticated invalid token")
	}
}

func TestProviderAuthenticateUnavailableReturnsError(t *testing.T) {
	fake := &fakeAtomServer{authnErr: status.Error(codes.Unavailable, "down")}
	provider, cleanup := newTestProvider(t, fake, Options{})
	defer cleanup()

	res, err := provider.Authenticate("client", "ignored", "client-token")
	if err == nil {
		t.Fatalf("Authenticate() expected error")
	}
	if res.Authenticated {
		t.Fatalf("Authenticate() authenticated unavailable Atom")
	}
}

func TestProviderCanPublishMapsAliasRouteToAtomCheck(t *testing.T) {
	fake := &fakeAtomServer{
		authnToken:  "client-token",
		aliasObject: testResourceID,
		checkAllow:  true,
	}
	provider, cleanup := newTestProvider(t, fake, Options{Protocol: ProtocolMQTT, AliasCacheTTL: time.Hour})
	defer cleanup()

	if !provider.CanPublish(testEntityID, "m/factory-a/c/telemetry/temp") {
		t.Fatalf("CanPublish() = false, want true")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()

	if fake.aliasCalls != 1 {
		t.Fatalf("alias calls = %d, want 1", fake.aliasCalls)
	}
	if fake.lastAlias.GetTenantAlias() != "factory-a" || fake.lastAlias.GetObjectKind() != objectKindResource || fake.lastAlias.GetObjectAlias() != "telemetry" {
		t.Fatalf("alias request = %#v", fake.lastAlias)
	}
	if got := fake.aliasAuthz; len(got) != 1 || got[0] != "Bearer "+testServiceToken {
		t.Fatalf("alias authorization metadata = %v", got)
	}

	check := fake.lastCheck
	if check.GetSubjectId() != testEntityID ||
		check.GetAction() != string(actionPublish) ||
		check.GetObjectKind() != objectKindResource ||
		check.GetObjectId() != testResourceID {
		t.Fatalf("check request = %#v", check)
	}
	if check.GetContext()["protocol"] != ProtocolMQTT ||
		check.GetContext()["raw_topic"] != "m/factory-a/c/telemetry/temp" ||
		check.GetContext()["topic"] != "m/factory-a/c/telemetry/temp" ||
		check.GetContext()["route_tenant"] != "factory-a" ||
		check.GetContext()["route_channel"] != "telemetry" ||
		check.GetContext()["subtopic"] != "temp" {
		t.Fatalf("check context = %#v", check.GetContext())
	}
	if got := fake.checkAuthz; len(got) != 1 || got[0] != "Bearer "+testServiceToken {
		t.Fatalf("check authorization metadata = %v", got)
	}
}

func TestProviderCanSubscribeAllowsWildcardAfterChannel(t *testing.T) {
	fake := &fakeAtomServer{aliasObject: testResourceID, checkAllow: true}
	provider, cleanup := newTestProvider(t, fake, Options{Protocol: ProtocolMQTT})
	defer cleanup()

	if !provider.CanSubscribe(testEntityID, "m/factory-a/c/telemetry/line-1/#") {
		t.Fatalf("CanSubscribe() = false, want true")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.lastCheck.GetContext()["filter"] != "line-1/#" {
		t.Fatalf("filter context = %#v", fake.lastCheck.GetContext())
	}
}

func TestProviderAMQPNormalizesAddressBeforeCheck(t *testing.T) {
	fake := &fakeAtomServer{aliasObject: testResourceID, checkAllow: true}
	provider, cleanup := newTestProvider(t, fake, Options{Protocol: ProtocolAMQP})
	defer cleanup()

	if !provider.CanSubscribe(testEntityID, "m.factory-a.c.telemetry.*") {
		t.Fatalf("CanSubscribe() = false, want true")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.lastCheck.GetContext()["topic"] != "m/factory-a/c/telemetry/+" {
		t.Fatalf("normalized topic = %q", fake.lastCheck.GetContext()["topic"])
	}
	if fake.lastCheck.GetContext()["filter"] != "+" {
		t.Fatalf("filter context = %#v", fake.lastCheck.GetContext())
	}
}

func TestProviderFailsClosedOnDeniedOrUnsupported(t *testing.T) {
	fake := &fakeAtomServer{aliasObject: testResourceID, checkAllow: false}
	provider, cleanup := newTestProvider(t, fake, Options{Protocol: ProtocolMQTT})
	defer cleanup()

	if provider.CanPublish(testEntityID, "m/factory-a/c/telemetry/temp") {
		t.Fatalf("CanPublish() allowed denied check")
	}
	if provider.CanSubscribe(testEntityID, "#") {
		t.Fatalf("CanSubscribe() allowed unsupported broad filter")
	}
}
