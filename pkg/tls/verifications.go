// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"github.com/absmach/fluxmq/pkg/tls/verifier"
	"github.com/absmach/fluxmq/pkg/tls/verifier/crl"
	"github.com/absmach/fluxmq/pkg/tls/verifier/ocsp"
)

func BuildVerifiers(cfg Config) ([]verifier.Verifier, error) {
	var vms []verifier.Verifier

	if ocspEnabled(cfg.OCSP) {
		vm, err := ocsp.New(cfg.OCSP)
		if err != nil {
			return nil, err
		}
		vms = append(vms, vm)
	}

	if crlEnabled(cfg.CRL) {
		vm, err := crl.New(cfg.CRL)
		if err != nil {
			return nil, err
		}
		vms = append(vms, vm)
	}

	if len(vms) == 0 {
		return nil, nil
	}

	return vms, nil
}

func ocspEnabled(cfg ocsp.Config) bool {
	return cfg.Depth > 0 || cfg.ResponderURL != ""
}

func crlEnabled(cfg crl.Config) bool {
	return cfg.Depth > 0 ||
		cfg.OfflineCRLFile != "" ||
		cfg.OfflineCRLIssuerCertFile != "" ||
		cfg.CRLDistributionPoints != "" ||
		cfg.CRLDistributionPointsIssuerCertFile != ""
}
