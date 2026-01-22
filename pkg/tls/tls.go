// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

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
	ClientAuth   string      `yaml:"client_auth"`
	OCSP         ocsp.Config `yaml:"ocsp"`
	CRL          crl.Config  `yaml:"crl"`
}

type clientAuthMode int

const (
	clientAuthUnset clientAuthMode = iota
	clientAuthNone
	clientAuthRequest
	clientAuthRequireAny
	clientAuthVerifyIfGiven
	clientAuthRequireAndVerify
)

func parseClientAuth(value string) (clientAuthMode, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "":
		return clientAuthUnset, nil
	case "none", "no":
		return clientAuthNone, nil
	case "request":
		return clientAuthRequest, nil
	case "require_any", "require-any", "requireany":
		return clientAuthRequireAny, nil
	case "verify_if_given", "verify-if-given", "verifyifgiven":
		return clientAuthVerifyIfGiven, nil
	case "require", "require_and_verify", "require-and-verify", "requireandverify":
		return clientAuthRequireAndVerify, nil
	default:
		return clientAuthUnset, fmt.Errorf("invalid client_auth %q", value)
	}
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

	clientAuthMode, err := parseClientAuth(c.ClientAuth)
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
		}

		applyClientAuthTLS(config, clientAuthMode, len(clientCA) > 0)

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
		}

		applyClientAuthDTLS(config, clientAuthMode, len(clientCA) > 0)

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

func applyClientAuthTLS(config *tls.Config, mode clientAuthMode, hasClientCA bool) {
	switch mode {
	case clientAuthUnset:
		if hasClientCA {
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}
	case clientAuthNone:
		config.ClientAuth = tls.NoClientCert
	case clientAuthRequest:
		config.ClientAuth = tls.RequestClientCert
	case clientAuthRequireAny:
		config.ClientAuth = tls.RequireAnyClientCert
	case clientAuthVerifyIfGiven:
		config.ClientAuth = tls.VerifyClientCertIfGiven
	case clientAuthRequireAndVerify:
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
}

func applyClientAuthDTLS(config *dtls.Config, mode clientAuthMode, hasClientCA bool) {
	switch mode {
	case clientAuthUnset:
		if hasClientCA {
			config.ClientAuth = dtls.RequireAndVerifyClientCert
		}
	case clientAuthNone:
		config.ClientAuth = dtls.NoClientCert
	case clientAuthRequest:
		config.ClientAuth = dtls.RequestClientCert
	case clientAuthRequireAny:
		config.ClientAuth = dtls.RequireAnyClientCert
	case clientAuthVerifyIfGiven:
		config.ClientAuth = dtls.VerifyClientCertIfGiven
	case clientAuthRequireAndVerify:
		config.ClientAuth = dtls.RequireAndVerifyClientCert
	}
}
