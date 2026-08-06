// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeKeyPair issues a self-signed certificate and returns the cert and key
// paths, plus the PEM bytes so a test can also use it as a CA file.
func writeKeyPair(t *testing.T, dir, name string) (certFile, keyFile string, certPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certFile = filepath.Join(dir, name+".crt")
	keyFile = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile, certPEM
}

func TestLoadClientTLSConfigNilAndUnsetYieldNoOverride(t *testing.T) {
	got, err := LoadClientTLSConfig(nil)
	if err != nil || got != nil {
		t.Fatalf("nil config = (%v, %v), want (nil, nil)", got, err)
	}

	got, err = LoadClientTLSConfig(&ClientConfig{})
	if err != nil || got != nil {
		t.Fatalf("empty config = (%v, %v), want (nil, nil)", got, err)
	}
}

func TestLoadClientTLSConfigLoadsCertificateAndCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, certPEM := writeKeyPair(t, dir, "client")

	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	cfg, err := LoadClientTLSConfig(&ClientConfig{
		CertFile:   certFile,
		KeyFile:    keyFile,
		CAFile:     caFile,
		ServerName: "atom.internal",
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates = %d, want 1 (no client certificate means no mTLS)", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs is nil, want the configured CA pool")
	}
	if cfg.ServerName != "atom.internal" {
		t.Fatalf("ServerName = %q, want atom.internal", cfg.ServerName)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %x, want TLS1.2 by default", cfg.MinVersion)
	}
}

func TestLoadClientTLSConfigCAOnlyNeedsNoKeyPair(t *testing.T) {
	dir := t.TempDir()
	_, _, certPEM := writeKeyPair(t, dir, "server")
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	cfg, err := LoadClientTLSConfig(&ClientConfig{CAFile: caFile})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Certificates) != 0 {
		t.Fatalf("Certificates = %d, want 0", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs is nil")
	}
}

func TestClientConfigRejectsHalfAKeyPair(t *testing.T) {
	for _, cfg := range []ClientConfig{
		{CertFile: "cert.pem"},
		{KeyFile: "key.pem"},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("%+v validated, want an error: a certificate without its key cannot be presented", cfg)
		}
		if _, err := LoadClientTLSConfig(&cfg); err == nil {
			t.Fatalf("%+v loaded, want an error", cfg)
		}
	}
}

func TestClientConfigRejectsBadMinVersion(t *testing.T) {
	cfg := ClientConfig{MinVersion: "TLS9.9"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("validated an unknown min_version")
	}
}

func TestClientConfigHonoursExplicitMinVersion(t *testing.T) {
	cfg, err := LoadClientTLSConfig(&ClientConfig{MinVersion: "TLS1.3"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %x, want TLS1.3", cfg.MinVersion)
	}
}

func TestLoadClientTLSConfigRejectsUnreadableAndEmptyMaterial(t *testing.T) {
	dir := t.TempDir()

	if _, err := LoadClientTLSConfig(&ClientConfig{
		CertFile: filepath.Join(dir, "missing.crt"),
		KeyFile:  filepath.Join(dir, "missing.key"),
	}); err == nil {
		t.Fatal("loaded a missing key pair")
	}

	// A CA file that parses as no certificates would silently leave RootCAs
	// empty, which fails every handshake for a non-obvious reason.
	emptyCA := filepath.Join(dir, "empty.crt")
	if err := os.WriteFile(emptyCA, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if _, err := LoadClientTLSConfig(&ClientConfig{CAFile: emptyCA}); err == nil {
		t.Fatal("loaded a CA file containing no certificates")
	}
}

func TestClientConfigConfigured(t *testing.T) {
	if (ClientConfig{}).Configured() {
		t.Fatal("empty config reports configured")
	}
	for _, cfg := range []ClientConfig{
		{CertFile: "c"}, {KeyFile: "k"}, {CAFile: "ca"},
		{ServerName: "s"}, {MinVersion: "TLS1.2"},
	} {
		if !cfg.Configured() {
			t.Fatalf("%+v reports unconfigured", cfg)
		}
	}
}
