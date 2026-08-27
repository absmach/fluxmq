// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"errors"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
)

// ErrSessionIdentityMismatch reports that a session takeover was requested by
// a principal that is not allowed to inherit the existing session.
var ErrSessionIdentityMismatch = errors.New("session external identity mismatch")

// SessionIdentityGuard carries the authenticated identity policy that the
// current session owner must validate before performing a destructive handoff.
type SessionIdentityGuard struct {
	ExternalID   string
	RequireBound bool
}

func sessionIdentityGuardFromProto(guard *clusterv1.SessionIdentityGuard) *SessionIdentityGuard {
	if guard == nil {
		return nil
	}
	return &SessionIdentityGuard{
		ExternalID:   guard.GetExpectedExternalId(),
		RequireBound: guard.GetRequireBound(),
	}
}

func sessionIdentityGuardToProto(guard *SessionIdentityGuard) *clusterv1.SessionIdentityGuard {
	if guard == nil {
		return nil
	}
	return &clusterv1.SessionIdentityGuard{
		ExpectedExternalId: guard.ExternalID,
		RequireBound:       guard.RequireBound,
	}
}
