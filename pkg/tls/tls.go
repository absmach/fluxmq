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
	CertFile                 string      `yaml:"cert_file"`
	KeyFile                  string      `yaml:"key_file"`
	ServerCAFile             string      `yaml:"server_ca_file"`
	ClientCAFile             string      `yaml:"ca_file"`
	ClientAuth               string      `yaml:"client_auth"`
	MinVersion               string      `yaml:"min_version"`
	CipherSuites             []string    `yaml:"cipher_suites"`
	PreferServerCipherSuites *bool       `yaml:"prefer_server_cipher_suites"`
	OCSP                     ocsp.Config `yaml:"ocsp"`
	CRL                      crl.Config  `yaml:"crl"`
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

func parseTLSMinVersion(value string) (uint16, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "":
		return 0, nil
	case "tls1.0", "1.0":
		return tls.VersionTLS10, nil
	case "tls1.1", "1.1":
		return tls.VersionTLS11, nil
	case "tls1.2", "1.2":
		return tls.VersionTLS12, nil
	case "tls1.3", "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("invalid min_version %q", value)
	}
}

func parseCipherSuites(names []string) ([]uint16, []dtls.CipherSuiteID, []string, []string, error) {
	if len(names) == 0 {
		return nil, nil, nil, nil, nil
	}

	tlsMap := tlsCipherSuiteMap()
	dtlsMap := dtlsCipherSuiteMap()
	tlsSuites := make([]uint16, 0, len(names))
	dtlsSuites := make([]dtls.CipherSuiteID, 0, len(names))
	missingTLS := make([]string, 0)
	missingDTLS := make([]string, 0)

	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if isTLS13CipherSuite(trimmed) {
			return nil, nil, nil, nil, fmt.Errorf("cipher_suites %q is TLS 1.3 and not configurable", trimmed)
		}
		tlsID, tlsOK := tlsMap[trimmed]
		dtlsID, dtlsOK := dtlsMap[trimmed]
		if !tlsOK && !dtlsOK {
			return nil, nil, nil, nil, fmt.Errorf("unsupported cipher suite %q", trimmed)
		}
		if tlsOK {
			tlsSuites = append(tlsSuites, tlsID)
		} else {
			missingTLS = append(missingTLS, trimmed)
		}
		if dtlsOK {
			dtlsSuites = append(dtlsSuites, dtlsID)
		} else {
			missingDTLS = append(missingDTLS, trimmed)
		}
	}

	return tlsSuites, dtlsSuites, missingTLS, missingDTLS, nil
}

func tlsCipherSuiteMap() map[string]uint16 {
	suites := make(map[string]uint16)
	for _, suite := range tls.CipherSuites() {
		suites[suite.Name] = suite.ID
	}
	for _, suite := range tls.InsecureCipherSuites() {
		suites[suite.Name] = suite.ID
	}
	return suites
}

func dtlsCipherSuiteMap() map[string]dtls.CipherSuiteID {
	suites := make(map[string]dtls.CipherSuiteID)
	for _, suite := range dtls.CipherSuites() {
		suites[suite.Name] = dtls.CipherSuiteID(suite.ID)
	}
	for _, suite := range dtls.InsecureCipherSuites() {
		suites[suite.Name] = dtls.CipherSuiteID(suite.ID)
	}
	return suites
}

func isTLS13CipherSuite(name string) bool {
	switch name {
	case "TLS_AES_128_GCM_SHA256",
		"TLS_AES_256_GCM_SHA384",
		"TLS_CHACHA20_POLY1305_SHA256":
		return true
	default:
		return false
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

	minVersion, err := parseTLSMinVersion(c.MinVersion)
	if err != nil {
		return zero, err
	}

	tlsSuites, dtlsSuites, missingTLS, missingDTLS, err := parseCipherSuites(c.CipherSuites)
	if err != nil {
		return zero, err
	}

	switch any(zero).(type) {
	case *tls.Config:

		config := &tls.Config{}
		if minVersion != 0 {
			config.MinVersion = minVersion
		}
		if len(c.CipherSuites) > 0 {
			if len(missingTLS) > 0 {
				return zero, fmt.Errorf("cipher_suites not supported for TLS: %s", strings.Join(missingTLS, ", "))
			}
			config.CipherSuites = tlsSuites
		}
		if c.PreferServerCipherSuites != nil {
			config.PreferServerCipherSuites = *c.PreferServerCipherSuites
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
		config := &dtls.Config{}
		if len(c.CipherSuites) > 0 {
			if len(missingDTLS) > 0 {
				return zero, fmt.Errorf("cipher_suites not supported for DTLS: %s", strings.Join(missingDTLS, ", "))
			}
			config.CipherSuites = dtlsSuites
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
