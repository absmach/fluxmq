// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package crl

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/absmach/mqtt/pkg/tls/verifier"
)

var (
	errRetrieveCRL         = errors.New("failed to retrieve CRL")
	errReadCRL             = errors.New("failed to read CRL")
	errParseCRL            = errors.New("failed to parse CRL")
	errExpiredCRL          = errors.New("crl expired")
	errCRLSign             = errors.New("failed to verify CRL signature")
	errOfflineCRLLoad      = errors.New("failed to load offline CRL file")
	errOfflineCRLIssuer    = errors.New("failed to load offline CRL issuer cert file")
	errOfflineCRLIssuerPEM = errors.New("failed to decode PEM block in offline CRL issuer cert file")
	errCRLDistIssuer       = errors.New("failed to load CRL distribution points issuer cert file")
	errCRLDistIssuerPEM    = errors.New("failed to decode PEM block in CRL distribution points issuer cert file")
	errNoCRL               = errors.New("neither offline CRL file nor CRL distribution points configured")
	errCertRevoked         = errors.New("certificate revoked")
)

var (
	errParseCert = errors.New("failed to parse Certificate")
	errClientCrt = errors.New("client certificate not received")
)

type Config struct {
	Depth                               uint   `yaml:"depth"`
	OfflineCRLFile                      string `yaml:"offline_crl_file"`
	OfflineCRLIssuerCertFile            string `yaml:"offline_crl_issuer_cert_file"`
	CRLDistributionPoints               string `yaml:"distribution_points"`
	CRLDistributionPointsIssuerCertFile string `yaml:"distribution_points_issuer_cert_file"`
}

type crlVerifier struct {
	Config
}

var _ verifier.Verifier = (*crlVerifier)(nil)

func New(cfg Config) (verifier.Verifier, error) {
	return &crlVerifier{Config: cfg}, nil
}

func (c *crlVerifier) VerifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	switch {
	case len(verifiedChains) > 0:
		return c.VerifyVerifiedPeerCertificates(verifiedChains)
	case len(rawCerts) > 0:
		var peerCertificates []*x509.Certificate
		peerCertificates, err := parseCertificates(rawCerts)
		if err != nil {
			return err
		}
		return c.VerifyRawPeerCertificates(peerCertificates)
	default:
		return errClientCrt
	}
}

func (c *crlVerifier) VerifyVerifiedPeerCertificates(verifiedPeerCertificateChains [][]*x509.Certificate) error {
	offlineCRL, err := c.loadOfflineCRL()
	if err != nil {
		return err
	}
	for _, verifiedChain := range verifiedPeerCertificateChains {
		for i := range verifiedChain {
			cert := verifiedChain[i]
			issuer := cert
			if i+1 < len(verifiedChain) {
				issuer = verifiedChain[i+1]
			}

			crl, err := c.getCRLFromDistributionPoint(cert, issuer)
			if err != nil {
				return err
			}
			switch {
			case crl == nil && offlineCRL != nil:
				crl = offlineCRL
			case crl == nil && offlineCRL == nil:
				return errNoCRL
			}

			if err := c.crlVerify(cert, crl); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *crlVerifier) VerifyRawPeerCertificates(peerCertificates []*x509.Certificate) error {
	offlineCRL, err := c.loadOfflineCRL()
	if err != nil {
		return err
	}
	for i, peerCertificate := range peerCertificates {
		issuerCert := retrieveIssuerCert(peerCertificate.Issuer, peerCertificates)
		crl, err := c.getCRLFromDistributionPoint(peerCertificate, issuerCert)
		if err != nil {
			return err
		}
		switch {
		case crl == nil && offlineCRL != nil:
			crl = offlineCRL
		case crl == nil && offlineCRL == nil:
			return errNoCRL
		}

		if err := c.crlVerify(peerCertificate, crl); err != nil {
			return err
		}
		if i+1 == int(c.Depth) {
			return nil
		}
	}
	return nil
}

func (c *crlVerifier) crlVerify(peerCertificate *x509.Certificate, crl *x509.RevocationList) error {
	for _, revokedCertificate := range crl.RevokedCertificateEntries {
		if revokedCertificate.SerialNumber.Cmp(peerCertificate.SerialNumber) == 0 {
			return errCertRevoked
		}
	}
	return nil
}

func (c *crlVerifier) loadOfflineCRL() (*x509.RevocationList, error) {
	offlineCRLBytes, err := loadCertFile(c.OfflineCRLFile)
	if err != nil {
		return nil, errors.Join(errOfflineCRLLoad, err)
	}
	if len(offlineCRLBytes) == 0 {
		return nil, nil
	}
	issuer, err := c.loadOfflineCRLIssuerCert()
	if err != nil {
		return nil, err
	}
	_ = issuer
	offlineCRL, err := parseVerifyCRL(offlineCRLBytes, nil, false)
	if err != nil {
		return nil, err
	}
	return offlineCRL, nil
}

func (c *crlVerifier) getCRLFromDistributionPoint(cert, issuer *x509.Certificate) (*x509.RevocationList, error) {
	switch {
	case len(cert.CRLDistributionPoints) > 0:
		return retrieveCRL(cert.CRLDistributionPoints[0], issuer, true)
	case c.CRLDistributionPoints != "" && c.CRLDistributionPointsIssuerCertFile != "":
		var crlIssuerCrt *x509.Certificate
		var err error
		if crlIssuerCrt, err = c.loadDistPointCRLIssuerCert(); err != nil {
			return nil, err
		}
		return retrieveCRL(c.CRLDistributionPoints, crlIssuerCrt, true)
	default:
		return nil, nil
	}
}

func (c *crlVerifier) loadDistPointCRLIssuerCert() (*x509.Certificate, error) {
	crlIssuerCertBytes, err := loadCertFile(c.CRLDistributionPointsIssuerCertFile)
	if err != nil {
		return nil, errors.Join(errCRLDistIssuer, err)
	}
	if len(crlIssuerCertBytes) == 0 {
		return nil, nil
	}
	crlIssuerCertPEM, _ := pem.Decode(crlIssuerCertBytes)
	if crlIssuerCertPEM == nil {
		return nil, errCRLDistIssuerPEM
	}
	crlIssuerCert, err := x509.ParseCertificate(crlIssuerCertPEM.Bytes)
	if err != nil {
		return nil, errors.Join(errCRLDistIssuer, err)
	}
	return crlIssuerCert, nil
}

func (c *crlVerifier) loadOfflineCRLIssuerCert() (*x509.Certificate, error) {
	offlineCrlIssuerCertBytes, err := loadCertFile(c.OfflineCRLIssuerCertFile)
	if err != nil {
		return nil, errors.Join(errOfflineCRLIssuer, err)
	}
	if len(offlineCrlIssuerCertBytes) == 0 {
		return nil, nil
	}
	offlineCrlIssuerCertPEM, _ := pem.Decode(offlineCrlIssuerCertBytes)
	if offlineCrlIssuerCertPEM == nil {
		return nil, errOfflineCRLIssuerPEM
	}
	crlIssuerCert, err := x509.ParseCertificate(offlineCrlIssuerCertPEM.Bytes)
	if err != nil {
		return nil, errors.Join(errOfflineCRLIssuer, err)
	}
	return crlIssuerCert, nil
}

func retrieveCRL(crlDistributionPoints string, issuerCert *x509.Certificate, checkSign bool) (*x509.RevocationList, error) {
	resp, err := http.Get(crlDistributionPoints)
	if err != nil {
		return nil, errors.Join(errRetrieveCRL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Join(errReadCRL, err)
	}
	return parseVerifyCRL(body, issuerCert, checkSign)
}

func parseVerifyCRL(clrB []byte, issuerCert *x509.Certificate, checkSign bool) (*x509.RevocationList, error) {
	block, _ := pem.Decode(clrB)
	if block == nil {
		return nil, errParseCRL
	}

	crl, err := x509.ParseRevocationList(block.Bytes)
	if err != nil {
		return nil, errors.Join(errParseCRL, err)
	}

	if checkSign {
		if err := crl.CheckSignatureFrom(issuerCert); err != nil {
			return nil, errors.Join(errCRLSign, err)
		}
	}

	if crl.NextUpdate.Before(time.Now()) {
		return nil, errExpiredCRL
	}
	return crl, nil
}

func loadCertFile(certFile string) ([]byte, error) {
	if certFile != "" {
		return os.ReadFile(certFile)
	}
	return []byte{}, nil
}

func retrieveIssuerCert(issuerSubject pkix.Name, certs []*x509.Certificate) *x509.Certificate {
	for _, cert := range certs {
		if cert.Subject.SerialNumber != "" && issuerSubject.SerialNumber != "" && cert.Subject.SerialNumber == issuerSubject.SerialNumber {
			return cert
		}
		if (cert.Subject.SerialNumber == "" || issuerSubject.SerialNumber == "") && cert.Subject.String() == issuerSubject.String() {
			return cert
		}
	}
	return nil
}

func parseCertificates(rawCerts [][]byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for _, rawCert := range rawCerts {
		cert, err := x509.ParseCertificate(rawCert)
		if err != nil {
			return nil, errors.Join(errParseCert, err)
		}
		certs = append(certs, cert)
	}
	return certs, nil
}
