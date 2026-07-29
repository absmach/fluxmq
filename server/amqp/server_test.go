// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	"github.com/absmach/fluxmq/amqp/codec"
)

const (
	testLocalCertificateURI = "spiffe://absmach/atom/audit-publisher"
	testServerName          = "localhost"
)

type reloadRaceLocalPolicy struct {
	authenticated chan struct{}
	retired       atomic.Bool
}

func (p *reloadRaceLocalPolicy) AuthenticateLocal(_ context.Context, _, _, _ string, _ amqpbroker.VerifiedPeerIdentity) (string, string, string, string, bool, error) {
	close(p.authenticated)
	return "atom-audit-publisher", "old-credential-fingerprint", "old-permissions-fingerprint", testLocalCertificateURI, true, nil
}

func (p *reloadRaceLocalPolicy) CanPublishLocal(amqpbroker.LocalSessionIdentity, string, string) bool {
	return !p.retired.Load()
}

func (p *reloadRaceLocalPolicy) IsSessionActive(amqpbroker.LocalSessionIdentity) bool {
	return !p.retired.Load()
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestServerSignalsReadyAfterBinding(t *testing.T) {
	b := amqpbroker.New(nil, testLogger())
	s := New(Config{Address: "127.0.0.1:0", Logger: testLogger()}, b)
	if s.Addr() != nil {
		t.Fatal("address must be nil before binding")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Listen(ctx) }()

	select {
	case <-s.Ready():
	case <-time.After(time.Second):
		t.Fatal("server did not become ready")
	}
	if s.Addr() == nil {
		t.Fatal("expected bound listener address")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Listen returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop after cancellation")
	}
}

func TestServerEnforcesConnectionLimit(t *testing.T) {
	s := New(Config{MaxConnections: 1, Logger: testLogger()}, amqpbroker.New(nil, testLogger()))
	firstClient, firstServer := net.Pipe()
	defer firstClient.Close()
	defer firstServer.Close()
	if !s.tryAcquireConnectionSlot(context.Background(), firstServer) {
		t.Fatal("first connection should acquire slot")
	}

	secondClient, secondServer := net.Pipe()
	defer secondClient.Close()
	if s.tryAcquireConnectionSlot(context.Background(), secondServer) {
		t.Fatal("second connection should be rejected")
	}
	if _, err := secondClient.Write([]byte("closed")); err == nil {
		t.Fatal("rejected connection should be closed")
	}
	s.releaseConnectionSlot()
}

func TestServerBoundsTLSHandshake(t *testing.T) {
	handshakeTimeout := 20 * time.Millisecond
	s := New(Config{
		TLSHandshakeTimeout: handshakeTimeout,
		Logger:              testLogger(),
	}, amqpbroker.New(nil, testLogger()))
	client, rawServer := net.Pipe()
	defer client.Close()
	tlsServer := tls.Server(rawServer, &tls.Config{MinVersion: tls.VersionTLS12})

	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.handleConnection(context.Background(), tlsServer)
		close(done)
	}()

	select {
	case <-done:
		if elapsed := time.Since(started); elapsed < handshakeTimeout {
			t.Fatalf("TLS handshake returned before configured deadline: %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("TLS handshake was not bounded")
	}
}

func TestServerBoundsAMQPHandshakeAfterTLS(t *testing.T) {
	handshakeTimeout := 100 * time.Millisecond
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := New(Config{
		HandshakeTimeout: handshakeTimeout,
		Logger:           logger,
	}, amqpbroker.New(nil, logger))
	clientTransport, serverTransport := net.Pipe()
	defer clientTransport.Close()
	tlsServer := tls.Server(serverTransport, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{testTLSCertificate(t)},
	})
	tlsClient := tls.Client(clientTransport, &tls.Config{
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // self-signed certificate in a loopback unit test
	})

	serverTLSReady := make(chan error, 1)
	go func() {
		serverTLSReady <- tlsServer.Handshake()
	}()
	if err := tlsClient.Handshake(); err != nil {
		t.Fatalf("complete TLS handshake: %v", err)
	}
	if err := <-serverTLSReady; err != nil {
		t.Fatalf("complete server TLS handshake: %v", err)
	}

	done := make(chan struct{})
	go func() {
		s.handleConnection(context.Background(), tlsServer)
		close(done)
	}()
	go func() {
		_, _ = io.Copy(io.Discard, tlsClient)
	}()

	// Stall before sending the AMQP protocol header. The TLS handshake succeeded,
	// so only a deadline retained through the AMQP handshake can release the slot.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("post-TLS AMQP handshake was not bounded; logs:\n%s", logs.String())
	}
}

func TestRetiredLocalCredentialDuringHandshakeReleasesConnectionSlot(t *testing.T) {
	serverTLS, clientTLS := testMutualTLSConfigs(t)
	logger := testLogger()
	b := amqpbroker.New(nil, logger)
	localPolicy := &reloadRaceLocalPolicy{authenticated: make(chan struct{})}
	s := New(Config{
		HandshakeTimeout: 2 * time.Second,
		MaxConnections:   1,
		ConnectionPolicy: amqpbroker.NewLocalPublishOnlyConnectionPolicy(
			localPolicy,
			localPolicy,
			localPolicy,
			0,
		),
		Logger: logger,
	}, b)

	clientTransport, serverTransport := net.Pipe()
	tlsServer := tls.Server(serverTransport, serverTLS)
	tlsClient := tls.Client(clientTransport, clientTLS)
	defer tlsClient.Close()
	if !s.tryAcquireConnectionSlot(context.Background(), tlsServer) {
		t.Fatal("stale connection did not acquire the only listener slot")
	}
	done := make(chan struct{})
	go func() {
		s.handleConnection(context.Background(), tlsServer)
		close(done)
	}()

	if err := tlsClient.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	if err := tlsClient.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}
	if _, err := tlsClient.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}); err != nil {
		t.Fatalf("write AMQP protocol header: %v", err)
	}
	readAMQPMethod[*codec.ConnectionStart](t, tlsClient)
	writeAMQPMethod(t, tlsClient, &codec.ConnectionStartOk{
		Mechanism: "PLAIN",
		Response:  "\x00atom-audit-publisher\x00old-secret",
		Locale:    "en_US",
	})

	select {
	case <-localPolicy.authenticated:
	case <-time.After(time.Second):
		t.Fatal("local authentication did not complete")
	}
	localPolicy.retired.Store(true)
	if disconnected := b.DisconnectInvalidLocalSessions(localPolicy.IsSessionActive); disconnected != 0 {
		t.Fatalf("pre-registration reload scan disconnected %d sessions, want 0", disconnected)
	}

	tune := readAMQPMethod[*codec.ConnectionTune](t, tlsClient)
	writeAMQPMethod(t, tlsClient, &codec.ConnectionTuneOk{
		ChannelMax: tune.ChannelMax,
		FrameMax:   tune.FrameMax,
		Heartbeat:  tune.Heartbeat,
	})
	writeAMQPMethod(t, tlsClient, &codec.ConnectionOpen{VirtualHost: "/"})
	readAMQPMethod[*codec.ConnectionOpenOk](t, tlsClient)
	closeMethod := readAMQPMethod[*codec.ConnectionClose](t, tlsClient)
	if closeMethod.ReplyCode != codec.AccessRefused {
		t.Fatalf("connection close code = %d, want %d", closeMethod.ReplyCode, codec.AccessRefused)
	}
	// The server has already sent the protocol error. Close the raw peer so the
	// TLS close-notify path cannot keep the connection slot during test cleanup.
	_ = clientTransport.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retired local connection did not terminate")
	}
	if ids := b.ConnectionIDs(); len(ids) != 0 {
		t.Fatalf("retired local connection remained registered: %v", ids)
	}
	if current := b.GetStats().GetCurrentConnections(); current != 0 {
		t.Fatalf("active connection stats = %d, want 0", current)
	}
	if local := b.GetStats().GetLocalConnections(); local != 0 {
		t.Fatalf("active local connection stats = %d, want 0", local)
	}

	nextClient, nextServer := net.Pipe()
	defer nextClient.Close()
	defer nextServer.Close()
	if !s.tryAcquireConnectionSlot(context.Background(), nextServer) {
		t.Fatal("retired local connection continued occupying the only listener slot")
	}
	s.releaseConnectionSlot()
}

func writeAMQPMethod(t *testing.T, conn net.Conn, method interface{ Write(io.Writer) error }) {
	t.Helper()
	var payload bytes.Buffer
	if err := method.Write(&payload); err != nil {
		t.Fatalf("encode AMQP method: %v", err)
	}
	if err := (&codec.Frame{Type: codec.FrameMethod, Channel: 0, Payload: payload.Bytes()}).WriteFrame(conn); err != nil {
		t.Fatalf("write AMQP method: %v", err)
	}
}

func readAMQPMethod[T any](t *testing.T, conn net.Conn) T {
	t.Helper()
	var zero T
	frame, err := codec.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read AMQP frame: %v", err)
		return zero
	}
	decoded, err := frame.Decode()
	if err != nil {
		t.Fatalf("decode AMQP method: %v", err)
		return zero
	}
	method, ok := decoded.(T)
	if !ok {
		t.Fatalf("AMQP method = %T, want %T", decoded, zero)
		return zero
	}
	return method
}

func testMutualTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "FluxMQ test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	serverCertificate := issueTestCertificate(t, caCertificate, caKey, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: testServerName},
		DNSNames:     []string{testServerName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	certificateURI, err := url.Parse(testLocalCertificateURI)
	if err != nil {
		t.Fatalf("parse client certificate URI: %v", err)
	}
	clientCertificate := issueTestCertificate(t, caCertificate, caKey, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "atom-audit-publisher"},
		URIs:         []*url.URL{certificateURI},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})

	caPool := x509.NewCertPool()
	caPool.AddCert(caCertificate)
	return &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{serverCertificate},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caPool,
		}, &tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   testServerName,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{clientCertificate},
		}
}

func issueTestCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, template *x509.Certificate) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate certificate key: %v", err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal certificate key: %v", err)
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatalf("parse certificate key pair: %v", err)
	}
	return certificate
}

func testTLSCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test TLS key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: testServerName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{testServerName},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create test TLS certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal test TLS key: %v", err)
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatalf("load test TLS key pair: %v", err)
	}
	return certificate
}
