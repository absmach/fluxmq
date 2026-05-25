// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTestCertKey(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fluxmq-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	caPath = certPath

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
	return caPath, certPath, keyPath
}

func TestBuildTLSCredentials_DefaultUsesSystemTrust(t *testing.T) {
	cfg, err := buildTLSCredentials(config.ServerConfig{})
	require.NoError(t, err)
	assert.Nil(t, cfg.RootCAs, "no CA file means default system trust")
	assert.Empty(t, cfg.Certificates)
	assert.Equal(t, uint16(0x0303), cfg.MinVersion) // TLS 1.2
}

func TestBuildTLSCredentials_WithCAFile(t *testing.T) {
	dir := t.TempDir()
	caPath, _, _ := writeTestCertKey(t, dir)

	cfg, err := buildTLSCredentials(config.ServerConfig{OtelCAFile: caPath})
	require.NoError(t, err)
	require.NotNil(t, cfg.RootCAs)
}

func TestBuildTLSCredentials_WithMTLS(t *testing.T) {
	dir := t.TempDir()
	caPath, certPath, keyPath := writeTestCertKey(t, dir)

	cfg, err := buildTLSCredentials(config.ServerConfig{
		OtelCAFile:   caPath,
		OtelCertFile: certPath,
		OtelKeyFile:  keyPath,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.RootCAs)
	require.Len(t, cfg.Certificates, 1)
}

func TestBuildTLSCredentials_MTLSRequiresBoth(t *testing.T) {
	_, err := buildTLSCredentials(config.ServerConfig{OtelCertFile: "/x"})
	require.Error(t, err)
	_, err = buildTLSCredentials(config.ServerConfig{OtelKeyFile: "/x"})
	require.Error(t, err)
}

func TestBuildTLSCredentials_BadCAFile(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.pem")
	require.NoError(t, os.WriteFile(bad, []byte("not a cert"), 0o600))

	_, err := buildTLSCredentials(config.ServerConfig{OtelCAFile: bad})
	require.Error(t, err)
}
