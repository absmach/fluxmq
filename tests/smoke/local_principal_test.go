//go:build smoke

// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package smoke_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

const (
	localUsername = "atom-audit-publisher"
	localURISAN   = "spiffe://absmach/atom/audit-publisher"
	auditQueue    = "atom-audit"
)

func TestLocalPrincipalRealProcess(t *testing.T) {
	binary := os.Getenv("FLUXMQ_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set FLUXMQ_SMOKE_BINARY to the built FluxMQ binary")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatalf("resolve FluxMQ binary: %v", err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("FluxMQ binary %q is unavailable: %v", binary, err)
	}

	workDir := t.TempDir()
	pki := newTestPKI(t, workDir)
	currentSecret := randomSecret(t)
	previousSecret := randomSecret(t)
	currentSecretFile := filepath.Join(workDir, "secret-current")
	previousSecretFile := filepath.Join(workDir, "secret-previous")
	writeSecret(t, currentSecretFile, currentSecret)
	writeSecret(t, previousSecretFile, previousSecret)

	callout := newMockCallout(t)
	defer callout.Close()

	ports := testPorts{
		remote:   freePort(t),
		internal: freePort(t),
		health:   freePort(t),
	}
	configPath := filepath.Join(workDir, "fluxmq.yaml")
	dataDir := filepath.Join(workDir, "data")
	writeSmokeConfig(t, configPath, smokeConfig{
		ports:              ports,
		dataDir:            dataDir,
		calloutURL:         callout.URL,
		serverCertFile:     pki.server.certFile,
		serverKeyFile:      pki.server.keyFile,
		clientCAFile:       pki.caFile,
		currentSecretFile:  currentSecretFile,
		previousSecretFile: previousSecretFile,
	})

	broker := startBroker(t, binary, configPath, ports)
	// Capture the variable, not the first process method value: the test replaces
	// broker after the persistence restart and that process must also be stopped
	// when a later assertion fails.
	t.Cleanup(func() { broker.stop() })

	validTLS := pki.clientTLS(t, pki.validClient)
	wrongSANTLS := pki.clientTLS(t, pki.wrongSANClient)
	untrustedTLS := pki.clientTLS(t, pki.untrustedClient)

	oldConn, oldChannel := dialPublisher(t, ports.internal, validTLS, currentSecret)
	t.Cleanup(func() {
		_ = oldChannel.Close()
		_ = oldConn.Close()
	})
	publishConfirmed(t, oldChannel, []byte(`{"id":"audit-1","action":"entity.create"}`))

	assertDialRejected(t, ports.internal, validTLS, localUsername, "wrong-secret")
	assertDialRejected(t, ports.internal, validTLS, "unknown-local-principal", currentSecret)
	assertDialRejected(t, ports.internal, wrongSANTLS, localUsername, currentSecret)
	assertDialRejected(t, ports.internal, untrustedTLS, localUsername, currentSecret)
	assertSubscribeDenied(t, ports.internal, validTLS, currentSecret)
	assertTopologyDenied(t, ports.internal, validTLS, currentSecret)
	assertPublishDenied(t, ports.internal, validTLS, currentSecret, "", "not-atom-audit")
	assertPublishDenied(t, ports.internal, validTLS, currentSecret, "events", auditQueue)

	if got := callout.total(); got != 0 {
		t.Fatalf("internal traffic reached external callouts: calls=%d", got)
	}

	callout.available.Store(false)
	outageConn, outageChannel := dialPublisher(t, ports.internal, validTLS, currentSecret)
	publishConfirmed(t, outageChannel, []byte(`{"id":"audit-2","action":"entity.update"}`))
	_ = outageChannel.Close()
	_ = outageConn.Close()
	if got := callout.total(); got != 0 {
		t.Fatalf("internal publication during external outage made callouts: calls=%d", got)
	}
	callout.available.Store(true)

	// Rotate from currentSecret to nextSecret while retaining currentSecret as
	// the overlap credential. Both old and new connections must remain valid.
	nextSecret := randomSecret(t)
	writeSecret(t, currentSecretFile, nextSecret)
	writeSecret(t, previousSecretFile, currentSecret)
	signalReload(t, broker)

	nextConn, nextChannel := dialPublisherEventually(t, ports.internal, validTLS, nextSecret)
	t.Cleanup(func() {
		_ = nextChannel.Close()
		_ = nextConn.Close()
	})
	publishConfirmed(t, nextChannel, []byte(`{"id":"audit-3","action":"entity.delete"}`))
	publishConfirmed(t, oldChannel, []byte(`{"id":"audit-4","action":"entity.restore"}`))

	// End the overlap by removing previous_secret_file from the config. The old
	// session must be disconnected while the new credential remains usable.
	writeSmokeConfig(t, configPath, smokeConfig{
		ports:             ports,
		dataDir:           dataDir,
		calloutURL:        callout.URL,
		serverCertFile:    pki.server.certFile,
		serverKeyFile:     pki.server.keyFile,
		clientCAFile:      pki.caFile,
		currentSecretFile: currentSecretFile,
	})
	signalReload(t, broker)
	waitForConnectionClose(t, oldConn)
	publishConfirmed(t, nextChannel, []byte(`{"id":"audit-5","action":"entity.purge"}`))
	assertDialRejected(t, ports.internal, validTLS, localUsername, currentSecret)

	broker.stop()
	broker = startBroker(t, binary, configPath, ports)

	callout.resetCounts()
	remoteConn := dialAMQPEventually(t, remoteURL(ports.remote, "remote-reader", "remote-secret"), nil)
	defer remoteConn.Close()
	remoteChannel, err := remoteConn.Channel()
	if err != nil {
		t.Fatalf("open remote channel: %v", err)
	}
	defer remoteChannel.Close()

	deliveries, err := remoteChannel.Consume(
		auditQueue,
		"smoke-replay",
		true,
		false,
		false,
		false,
		amqp091.Table{
			"x-stream-offset":  "first",
			"x-consumer-group": "smoke-replay",
		},
	)
	if err != nil {
		t.Fatalf("consume persisted audit stream: %v", err)
	}

	select {
	case delivery := <-deliveries:
		if !strings.Contains(string(delivery.Body), `"id":"audit-1"`) {
			t.Fatalf("unexpected first replayed event: %s", delivery.Body)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out replaying persisted audit event")
	}

	if callout.authn.Load() == 0 || callout.authz.Load() == 0 || callout.hooks.Load() == 0 {
		t.Fatalf(
			"remote path did not use all external services: authn=%d authz=%d hooks=%d",
			callout.authn.Load(),
			callout.authz.Load(),
			callout.hooks.Load(),
		)
	}
}

type smokeConfig struct {
	ports              testPorts
	dataDir            string
	calloutURL         string
	serverCertFile     string
	serverKeyFile      string
	clientCAFile       string
	currentSecretFile  string
	previousSecretFile string
}

func writeSmokeConfig(t *testing.T, path string, cfg smokeConfig) {
	t.Helper()
	previous := ""
	if cfg.previousSecretFile != "" {
		previous = fmt.Sprintf("\n      previous_secret_file: %q", cfg.previousSecretFile)
	}

	contents := fmt.Sprintf(`server:
  tcp:
    v3: {addr: ""}
    v5: {addr: ""}
  websocket:
    v3: {addr: ""}
    v5: {addr: ""}
  http:
    plain: {addr: ""}
  coap:
    plain: {addr: ""}
  amqp:
    plain: {addr: ""}
  amqp091:
    plain:
      addr: "127.0.0.1:%d"
      max_connections: 32
    internal:
      addr: "127.0.0.1:%d"
      max_connections: 8
      cert_file: %q
      key_file: %q
      ca_file: %q
      client_auth: "require"
      min_version: "TLS1.2"
  health_enabled: true
  health_addr: "127.0.0.1:%d"
  admin_api_addr: ""
  shutdown_timeout: "5s"

broker:
  max_message_size: 1048576

storage:
  type: "badger"
  badger_dir: %q
  sync_writes: true

cluster:
  enabled: false

auth:
  external:
    url: %q
    transport: "http"
    timeout: "500ms"
    protocols:
      amqp091: true
  local_principals:
    - name: %q
      certificate_uri_san: %q
      current_secret_file: %q%s
      permissions:
        publish:
          - exchange: ""
            routing_key: %q
        subscribe: []

hooks:
  url: %q
  transport: "http"
  timeout: "500ms"
  fail_mode: "deny"
  protocols:
    amqp091: true

queues:
  - name: %q
    topics:
      - "$queue/atom-audit/#"
    reserved: true
    type: "stream"
    retention:
      max_age: "720h"
      max_length_bytes: 10737418240
    limits:
      max_message_size: 1048576
      message_ttl: "720h"

log:
  level: "debug"
  format: "text"
`,
		cfg.ports.remote,
		cfg.ports.internal,
		cfg.serverCertFile,
		cfg.serverKeyFile,
		cfg.clientCAFile,
		cfg.ports.health,
		cfg.dataDir,
		cfg.calloutURL,
		localUsername,
		localURISAN,
		cfg.currentSecretFile,
		previous,
		auditQueue,
		cfg.calloutURL,
		auditQueue,
	)

	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write smoke config: %v", err)
	}
}

type testPorts struct {
	remote   int
	internal int
	health   int
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

type brokerProcess struct {
	cmd      *exec.Cmd
	done     chan struct{}
	logs     *lockedBuffer
	stopOnce sync.Once
	waitMu   sync.Mutex
	waitErr  error
}

func startBroker(t *testing.T, binary, configPath string, ports testPorts) *brokerProcess {
	t.Helper()
	logs := &lockedBuffer{}
	cmd := exec.Command(binary, "-config", configPath)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start FluxMQ: %v", err)
	}

	process := &brokerProcess{
		cmd:  cmd,
		done: make(chan struct{}),
		logs: logs,
	}
	go func() {
		err := cmd.Wait()
		process.waitMu.Lock()
		process.waitErr = err
		process.waitMu.Unlock()
		close(process.done)
	}()

	for _, port := range []int{ports.remote, ports.internal, ports.health} {
		if err := waitForTCP(process, port, 15*time.Second); err != nil {
			process.stop()
			t.Fatalf("FluxMQ did not become ready on port %d: %v\n%s", port, err, logs.String())
		}
	}
	return process
}

func (p *brokerProcess) stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
}

func (p *brokerProcess) err() error {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

func waitForTCP(process *brokerProcess, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			return fmt.Errorf("process exited early: %w", process.err())
		default:
		}

		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout after %s", timeout)
}

func signalReload(t *testing.T, broker *brokerProcess) {
	t.Helper()
	if err := broker.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("send SIGHUP: %v", err)
	}
	// Reload is asynchronous. Authentication below is retried until the new
	// immutable snapshot is visible.
	time.Sleep(100 * time.Millisecond)
}

func internalURL(port int, username, secret string) string {
	return (&url.URL{
		Scheme: "amqps",
		User:   url.UserPassword(username, secret),
		Host:   fmt.Sprintf("127.0.0.1:%d", port),
		Path:   "/",
	}).String()
}

func remoteURL(port int, username, secret string) string {
	return (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(username, secret),
		Host:   fmt.Sprintf("127.0.0.1:%d", port),
		Path:   "/",
	}).String()
}

func dialPublisher(t *testing.T, port int, tlsConfig *tls.Config, secret string) (*amqp091.Connection, *amqp091.Channel) {
	t.Helper()
	conn, err := amqp091.DialConfig(internalURL(port, localUsername, secret), amqp091.Config{TLSClientConfig: tlsConfig})
	if err != nil {
		t.Fatalf("dial internal AMQP: %v", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("open internal channel: %v", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		t.Fatalf("enable publisher confirms: %v", err)
	}
	return conn, channel
}

func dialPublisherEventually(t *testing.T, port int, tlsConfig *tls.Config, secret string) (*amqp091.Connection, *amqp091.Channel) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := amqp091.DialConfig(internalURL(port, localUsername, secret), amqp091.Config{TLSClientConfig: tlsConfig})
		if err == nil {
			channel, channelErr := conn.Channel()
			if channelErr == nil {
				if confirmErr := channel.Confirm(false); confirmErr == nil {
					return conn, channel
				} else {
					lastErr = confirmErr
				}
			} else {
				lastErr = channelErr
			}
			_ = conn.Close()
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("dial internal AMQP after reload: %v", lastErr)
	return nil, nil
}

func dialAMQPEventually(t *testing.T, rawURL string, tlsConfig *tls.Config) *amqp091.Connection {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := amqp091.DialConfig(rawURL, amqp091.Config{TLSClientConfig: tlsConfig})
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("dial AMQP: %v", lastErr)
	return nil
}

func publishConfirmed(t *testing.T, channel *amqp091.Channel, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", auditQueue, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("publish audit event: %v", err)
	}
	if confirmation == nil {
		t.Fatal("publisher confirm mode did not return a deferred confirmation")
	}
	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		t.Fatalf("wait for publisher confirmation: %v", err)
	}
	if !acknowledged {
		t.Fatal("broker negatively acknowledged audit publication")
	}
}

func assertDialRejected(t *testing.T, port int, tlsConfig *tls.Config, username, secret string) {
	t.Helper()
	conn, err := amqp091.DialConfig(internalURL(port, username, secret), amqp091.Config{
		TLSClientConfig: tlsConfig,
		Dial:            amqp091.DefaultDial(2 * time.Second),
	})
	if err == nil {
		_ = conn.Close()
		t.Fatalf("expected internal authentication for username %q to be rejected", username)
	}
}

func assertSubscribeDenied(t *testing.T, port int, tlsConfig *tls.Config, secret string) {
	t.Helper()
	conn, channel := dialPublisher(t, port, tlsConfig, secret)
	defer conn.Close()
	defer channel.Close()
	_, err := channel.Consume(auditQueue, "forbidden", true, false, false, false, nil)
	assertAMQPAccessRefused(t, err)
}

func assertTopologyDenied(t *testing.T, port int, tlsConfig *tls.Config, secret string) {
	t.Helper()
	conn, channel := dialPublisher(t, port, tlsConfig, secret)
	defer conn.Close()
	defer channel.Close()
	_, err := channel.QueueDeclare("forbidden", false, false, false, false, nil)
	assertAMQPAccessRefused(t, err)
}

func assertPublishDenied(t *testing.T, port int, tlsConfig *tls.Config, secret, exchange, routingKey string) {
	t.Helper()
	conn, channel := dialPublisher(t, port, tlsConfig, secret)
	defer conn.Close()
	defer channel.Close()
	closed := channel.NotifyClose(make(chan *amqp091.Error, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = channel.PublishWithContext(ctx, exchange, routingKey, false, false, amqp091.Publishing{Body: []byte("denied")})
	select {
	case amqpErr := <-closed:
		if amqpErr == nil || amqpErr.Code != 403 {
			t.Fatalf("expected AMQP 403, got %#v", amqpErr)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for unauthorized publish rejection")
	}
}

func assertAMQPAccessRefused(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected AMQP 403 Access Refused")
	}
	amqpErr, ok := err.(*amqp091.Error)
	if !ok || amqpErr.Code != 403 {
		t.Fatalf("expected AMQP 403 Access Refused, got %T: %v", err, err)
	}
}

func waitForConnectionClose(t *testing.T, conn *amqp091.Connection) {
	t.Helper()
	closed := conn.NotifyClose(make(chan *amqp091.Error, 1))
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("connection authenticated with removed secret was not disconnected")
	}
}

type mockCallout struct {
	*httptest.Server
	available atomic.Bool
	authn     atomic.Int64
	authz     atomic.Int64
	hooks     atomic.Int64
}

func newMockCallout(t *testing.T) *mockCallout {
	t.Helper()
	mock := &mockCallout{}
	mock.available.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/authenticate", func(writer http.ResponseWriter, request *http.Request) {
		mock.authn.Add(1)
		if !mock.available.Load() {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(writer, map[string]any{"authenticated": true, "id": "remote-reader"})
	})
	mux.HandleFunc("/auth/authorize", func(writer http.ResponseWriter, request *http.Request) {
		mock.authz.Add(1)
		if !mock.available.Load() {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(writer, map[string]any{"authorized": true})
	})
	mux.HandleFunc("/hooks", func(writer http.ResponseWriter, request *http.Request) {
		mock.hooks.Add(1)
		if !mock.available.Load() {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(writer, map[string]any{"result": "ok"})
	})
	mock.Server = httptest.NewServer(mux)
	return mock
}

func (m *mockCallout) total() int64 {
	return m.authn.Load() + m.authz.Load() + m.hooks.Load()
}

func (m *mockCallout) resetCounts() {
	m.authn.Store(0)
	m.authz.Store(0)
	m.hooks.Store(0)
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}

type testPKI struct {
	ca              certificateFiles
	caFile          string
	server          certificateFiles
	validClient     certificateFiles
	wrongSANClient  certificateFiles
	untrustedClient certificateFiles
}

type certificateFiles struct {
	certFile string
	keyFile  string
}

type certificateAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestPKI(t *testing.T, dir string) testPKI {
	t.Helper()
	ca := createCA(t, "FluxMQ smoke CA")
	untrustedCA := createCA(t, "Untrusted smoke CA")
	return testPKI{
		ca:              writeCertificate(t, dir, "ca", ca.pem, nil),
		caFile:          writePEM(t, filepath.Join(dir, "ca.crt"), ca.pem, 0o644),
		server:          issueCertificate(t, dir, "server", ca, true, ""),
		validClient:     issueCertificate(t, dir, "atom", ca, false, localURISAN),
		wrongSANClient:  issueCertificate(t, dir, "wrong-san", ca, false, "spiffe://absmach/atom/not-audit-publisher"),
		untrustedClient: issueCertificate(t, dir, "untrusted", untrustedCA, false, localURISAN),
	}
}

func createCA(t *testing.T, commonName string) certificateAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	return certificateAuthority{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func issueCertificate(t *testing.T, dir, name string, ca certificateAuthority, server bool, uriSAN string) certificateFiles {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", name, err)
	}
	template := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		parsedURI, err := url.Parse(uriSAN)
		if err != nil {
			t.Fatalf("parse client URI SAN: %v", err)
		}
		template.URIs = []*url.URL{parsedURI}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("issue %s certificate: %v", name, err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", name, err)
	}
	return writeCertificate(
		t,
		dir,
		name,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
}

func writeCertificate(t *testing.T, dir, name string, certPEM, keyPEM []byte) certificateFiles {
	t.Helper()
	files := certificateFiles{certFile: writePEM(t, filepath.Join(dir, name+".crt"), certPEM, 0o644)}
	if len(keyPEM) != 0 {
		files.keyFile = writePEM(t, filepath.Join(dir, name+".key"), keyPEM, 0o600)
	}
	return files
}

func writePEM(t *testing.T, path string, value []byte, mode os.FileMode) string {
	t.Helper()
	if err := os.WriteFile(path, value, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func (p testPKI) clientTLS(t *testing.T, client certificateFiles) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(client.certFile, client.keyFile)
	if err != nil {
		t.Fatalf("load client key pair: %v", err)
	}
	caPEM, err := os.ReadFile(p.caFile)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append server CA")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		ServerName:   "localhost",
		RootCAs:      roots,
		Certificates: []tls.Certificate{cert},
	}
}

func randomSerial(t *testing.T) *big.Int {
	t.Helper()
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	return serial
}

func randomSecret(t *testing.T) string {
	t.Helper()
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate secret: %v", err)
	}
	return hex.EncodeToString(value)
}

func writeSecret(t *testing.T, path, secret string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
}

type lockedBuffer struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.String()
}
