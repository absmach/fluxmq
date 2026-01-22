// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"

	"github.com/absmach/fluxmq/pkg/tls/verifier"
	"github.com/absmach/fluxmq/pkg/tls/verifier/crl"
	"github.com/absmach/fluxmq/pkg/tls/verifier/ocsp"
	"github.com/pion/dtls/v3"
)

var (
	errTLSdetails     = errors.New("failed to get TLS details of connection")
	errLoadCerts      = errors.New("failed to load certificates")
	errLoadServerCA   = errors.New("failed to load Server CA")
	errLoadClientCA   = errors.New("failed to load Client CA")
	errAppendCA       = errors.New("failed to append root ca tls.Config")
	errUnsupportedTLS = errors.New("unsupported tls configuration")
)

type Config struct {
	CertFile     string      `yaml:"cert_file"`
	KeyFile      string      `yaml:"key_file"`
	ServerCAFile string      `yaml:"server_ca_file"`
	ClientCAFile string      `yaml:"ca_file"`
	OCSP         ocsp.Config `yaml:"ocsp"`
	CRL          crl.Config  `yaml:"crl"`
}

type TLSConfig interface {
	*tls.Config | *dtls.Config
}

// LoadTLSConfig returns a TLS or DTLS configuration that can be used for TLS or DTLS servers.
func LoadTLSConfig[sc TLSConfig](c *Config) (sc, error) {

	var zero sc

	if c.CertFile == "" || c.KeyFile == "" {
		return zero, nil
	}

	certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return zero, errors.Join(errLoadCerts, err)
	}

	// Loading Server CA file
	rootCA, err := loadCertFile(c.ServerCAFile)
	if err != nil {
		return zero, errors.Join(errLoadServerCA, err)
	}

	// Loading Client CA File
	clientCA, err := loadCertFile(c.ClientCAFile)
	if err != nil {
		return zero, errors.Join(errLoadClientCA, err)
	}

	verifiers, err := BuildVerifiers(*c)
	if err != nil {
		return zero, err
	}

	switch any(zero).(type) {
	case *tls.Config:

		config := &tls.Config{
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			},
			PreferServerCipherSuites: true,
		}
		config.Certificates = []tls.Certificate{certificate}

		if len(rootCA) > 0 {
			if config.RootCAs == nil {
				config.RootCAs = x509.NewCertPool()
			}
			if !config.RootCAs.AppendCertsFromPEM(rootCA) {
				return zero, errAppendCA
			}
		}

		if len(clientCA) > 0 {
			if config.ClientCAs == nil {
				config.ClientCAs = x509.NewCertPool()
			}
			if !config.ClientCAs.AppendCertsFromPEM(clientCA) {
				return zero, errAppendCA
			}
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}

		if len(verifiers) > 0 {
			config.VerifyPeerCertificate = verifier.NewValidator(verifiers)
		}
		return any(config).(sc), nil
	case *dtls.Config:
		config := &dtls.Config{
			CipherSuites: []dtls.CipherSuiteID{
				dtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				dtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				dtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			},
		}
		config.Certificates = []tls.Certificate{certificate}

		if len(rootCA) > 0 {
			if config.RootCAs == nil {
				config.RootCAs = x509.NewCertPool()
			}
			if !config.RootCAs.AppendCertsFromPEM(rootCA) {
				return zero, errAppendCA
			}
		}

		if len(clientCA) > 0 {
			if config.ClientCAs == nil {
				config.ClientCAs = x509.NewCertPool()
			}
			if !config.ClientCAs.AppendCertsFromPEM(clientCA) {
				return zero, errAppendCA
			}
			config.ClientAuth = dtls.RequireAndVerifyClientCert
		}

		if len(verifiers) > 0 {
			config.VerifyPeerCertificate = verifier.NewValidator(verifiers)
		}
		return any(config).(sc), nil
	default:
		return zero, errUnsupportedTLS
	}
}

// ClientCert returns client certificate.
func ClientCert(conn net.Conn) (x509.Certificate, error) {
	switch connVal := conn.(type) {
	case *tls.Conn:
		if err := connVal.Handshake(); err != nil {
			return x509.Certificate{}, err
		}
		state := connVal.ConnectionState()
		if state.Version == 0 {
			return x509.Certificate{}, errTLSdetails
		}
		if len(state.PeerCertificates) == 0 {
			return x509.Certificate{}, nil
		}
		cert := *state.PeerCertificates[0]
		return cert, nil
	default:
		return x509.Certificate{}, nil
	}
}

// SecurityStatus returns log message from TLS config.
func SecurityStatus[sc TLSConfig](s sc) string {
	if s == nil {
		return "no TLS"
	}
	switch c := any(s).(type) {
	case *tls.Config:
		ret := "TLS"
		// It is possible to establish TLS with client certificates only.
		if len(c.Certificates) == 0 {
			ret = "no server certificates"
		}
		if c.ClientCAs != nil {
			ret += " and " + c.ClientAuth.String()
		}
		return ret
	case *dtls.Config:
		return "DTLS"
	default:
		return "no TLS"
	}
}

func loadCertFile(certFile string) ([]byte, error) {
	if certFile != "" {
		return os.ReadFile(certFile)
	}
	return []byte{}, nil
}
