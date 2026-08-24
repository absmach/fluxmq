// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// LoadMutualTLSConfigs loads the shared cluster identity into server and
// client configurations. All inter-node protocols use this same trust root so
// a node cannot join etcd, broker routing, or queue Raft anonymously.
func LoadMutualTLSConfigs(cfg *TransportTLSConfig) (*tls.Config, *tls.Config, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("cluster TLS configuration is required")
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load cluster certificate: %w", err)
	}
	caCert, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load cluster CA certificate: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, nil, fmt.Errorf("parse cluster CA certificate")
	}

	server := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}
	client := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}
	return server, client, nil
}
