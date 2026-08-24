// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package testcert creates short-lived certificate fixtures for in-process
// transport tests.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Files contains a CA and one node identity valid for localhost and 127.0.0.1.
type Files struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// Generate writes a CA and a dual client/server node certificate into dir.
func Generate(dir string) (Files, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Files{}, err
	}
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "FluxMQ test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return Files{}, err
	}

	nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Files{}, err
	}
	node := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "fluxmq-node"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	nodeDER, err := x509.CreateCertificate(rand.Reader, node, ca, &nodeKey.PublicKey, caKey)
	if err != nil {
		return Files{}, err
	}
	nodeKeyDER, err := x509.MarshalECPrivateKey(nodeKey)
	if err != nil {
		return Files{}, err
	}

	files := Files{
		CertFile: filepath.Join(dir, "node.crt"),
		KeyFile:  filepath.Join(dir, "node.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}
	if err := os.WriteFile(files.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return Files{}, err
	}
	if err := os.WriteFile(files.CertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDER}), 0o600); err != nil {
		return Files{}, err
	}
	if err := os.WriteFile(files.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: nodeKeyDER}), 0o600); err != nil {
		return Files{}, err
	}
	return files, nil
}
