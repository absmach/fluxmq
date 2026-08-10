// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

var (
	errClientKeyPair   = errors.New("cert_file and key_file must be set together")
	errLoadClientCerts = errors.New("failed to load client certificate")
	errLoadCalloutCA   = errors.New("failed to load callout CA")
)

// ClientConfig configures TLS for a connection FluxMQ opens, rather than one it
// accepts. It is deliberately not [Config]: a client has no use for
// client_auth, cipher-suite pinning, or OCSP/CRL verifiers, and offering those
// knobs on an outbound connection only invites misconfiguration.
//
// Setting cert_file/key_file is what makes the connection mutual: the callout
// server can then authenticate FluxMQ from the certificate, which is the only
// authentication some callout endpoints have.
type ClientConfig struct {
	// Client certificate presented to the server (mTLS). Both or neither.
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// CA used to verify the server's certificate. Empty uses the system pool.
	CAFile string `yaml:"ca_file"`
	// Overrides the name verified against the server certificate. Needed when
	// the URL host differs from the name the certificate was issued for.
	ServerName string `yaml:"server_name"`
	// Defaults to TLS 1.2.
	MinVersion string `yaml:"min_version"`
}

// Configured reports whether any field is set. An unset block means "use the
// default transport", not "use an empty TLS config".
func (c ClientConfig) Configured() bool {
	return c.CertFile != "" || c.KeyFile != "" || c.CAFile != "" ||
		c.ServerName != "" || c.MinVersion != ""
}

// Validate reports configuration errors without touching the filesystem, so a
// config file can be rejected before anything tries to dial with it.
func (c ClientConfig) Validate() error {
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errClientKeyPair
	}
	if _, err := parseTLSMinVersion(c.MinVersion); err != nil {
		return err
	}
	return nil
}

// LoadClientTLSConfig builds the outbound TLS configuration. A nil or unset
// config returns nil, which callers treat as "no TLS override".
func LoadClientTLSConfig(c *ClientConfig) (*tls.Config, error) {
	if c == nil || !c.Configured() {
		return nil, nil
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}

	minVersion, err := parseTLSMinVersion(c.MinVersion)
	if err != nil {
		return nil, err
	}
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}

	config := &tls.Config{
		MinVersion: minVersion,
		ServerName: c.ServerName,
	}

	if c.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, errors.Join(errLoadClientCerts, err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	if c.CAFile != "" {
		ca, err := loadCertFile(c.CAFile)
		if err != nil {
			return nil, errors.Join(errLoadCalloutCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("%w: no certificates found in %s", errAppendCA, c.CAFile)
		}
		config.RootCAs = pool
	}

	return config, nil
}
