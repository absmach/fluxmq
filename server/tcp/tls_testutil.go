// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TLSTestCerts holds paths to generated test certificates.
type TLSTestCerts struct {
	CAFile         string
	ServerCertFile string
	ServerKeyFile  string
	ClientCertFile string
	ClientKeyFile  string
}

// GenerateTestCerts generates a CA, server cert, and client cert for testing.
// All certificates are written to a temporary directory that's cleaned up when the test ends.
func GenerateTestCerts(t *testing.T) *TLSTestCerts {
	t.Helper()

	tempDir := t.TempDir()

	// Generate CA
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create CA certificate: %v", err)
	}

	// Write CA cert
	caFile := filepath.Join(tempDir, "ca.crt")
	caCertFile, err := os.Create(caFile)
	if err != nil {
		t.Fatalf("Failed to create CA cert file: %v", err)
	}
	if err := pem.Encode(caCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}); err != nil {
		t.Fatalf("Failed to write CA cert: %v", err)
	}
	caCertFile.Close()

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("Failed to parse CA certificate: %v", err)
	}

	// Generate server certificate
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate server key: %v", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create server certificate: %v", err)
	}

	// Write server cert
	serverCertFile := filepath.Join(tempDir, "server.crt")
	serverCert, err := os.Create(serverCertFile)
	if err != nil {
		t.Fatalf("Failed to create server cert file: %v", err)
	}
	if err := pem.Encode(serverCert, &pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER}); err != nil {
		t.Fatalf("Failed to write server cert: %v", err)
	}
	serverCert.Close()

	// Write server key
	serverKeyFile := filepath.Join(tempDir, "server.key")
	serverKeyPEMFile, err := os.Create(serverKeyFile)
	if err != nil {
		t.Fatalf("Failed to create server key file: %v", err)
	}
	serverKeyBytes := x509.MarshalPKCS1PrivateKey(serverKey)
	if err := pem.Encode(serverKeyPEMFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: serverKeyBytes}); err != nil {
		t.Fatalf("Failed to write server key: %v", err)
	}
	serverKeyPEMFile.Close()

	// Generate client certificate
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate client key: %v", err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
			CommonName:   "test-client",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create client certificate: %v", err)
	}

	// Write client cert
	clientCertFile := filepath.Join(tempDir, "client.crt")
	clientCert, err := os.Create(clientCertFile)
	if err != nil {
		t.Fatalf("Failed to create client cert file: %v", err)
	}
	if err := pem.Encode(clientCert, &pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER}); err != nil {
		t.Fatalf("Failed to write client cert: %v", err)
	}
	clientCert.Close()

	// Write client key
	clientKeyFile := filepath.Join(tempDir, "client.key")
	clientKeyPEMFile, err := os.Create(clientKeyFile)
	if err != nil {
		t.Fatalf("Failed to create client key file: %v", err)
	}
	clientKeyBytes := x509.MarshalPKCS1PrivateKey(clientKey)
	if err := pem.Encode(clientKeyPEMFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: clientKeyBytes}); err != nil {
		t.Fatalf("Failed to write client key: %v", err)
	}
	clientKeyPEMFile.Close()

	return &TLSTestCerts{
		CAFile:         caFile,
		ServerCertFile: serverCertFile,
		ServerKeyFile:  serverKeyFile,
		ClientCertFile: clientCertFile,
		ClientKeyFile:  clientKeyFile,
	}
}

// LoadServerTLSConfig loads a TLS config for the server from test certificates.
func LoadServerTLSConfig(t *testing.T, certs *TLSTestCerts, clientAuth tls.ClientAuthType) *tls.Config {
	t.Helper()

	cert, err := tls.LoadX509KeyPair(certs.ServerCertFile, certs.ServerKeyFile)
	if err != nil {
		t.Fatalf("Failed to load server certificate: %v", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if clientAuth != tls.NoClientCert {
		caCert, err := os.ReadFile(certs.CAFile)
		if err != nil {
			t.Fatalf("Failed to read CA cert: %v", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			t.Fatal("Failed to parse CA certificate")
		}

		config.ClientCAs = caCertPool
		config.ClientAuth = clientAuth
	}

	return config
}

// LoadClientTLSConfig loads a TLS config for the client from test certificates.
func LoadClientTLSConfig(t *testing.T, certs *TLSTestCerts, useClientCert bool) *tls.Config {
	t.Helper()

	caCert, err := os.ReadFile(certs.CAFile)
	if err != nil {
		t.Fatalf("Failed to read CA cert: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		t.Fatal("Failed to parse CA certificate")
	}

	config := &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12,
	}

	if useClientCert {
		cert, err := tls.LoadX509KeyPair(certs.ClientCertFile, certs.ClientKeyFile)
		if err != nil {
			t.Fatalf("Failed to load client certificate: %v", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	return config
}
