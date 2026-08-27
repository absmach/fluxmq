// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/stretchr/testify/require"
)

type identityRejectingHandler struct {
	MessageHandler
	identity *SessionIdentityGuard
}

func (h *identityRejectingHandler) GetSessionStateAndClose(_ context.Context, _ string, identity *SessionIdentityGuard) (*clusterv1.SessionState, error) {
	h.identity = identity
	return nil, ErrSessionIdentityMismatch
}

func TestTakeoverSessionReturnsTypedIdentityMismatch(t *testing.T) {
	handler := &identityRejectingHandler{}
	transport := &Transport{handler: handler}
	req := connect.NewRequest(&clusterv1.TakeoverRequest{
		ClientId: "bound-client",
		IdentityGuard: &clusterv1.SessionIdentityGuard{
			ExpectedExternalId: "entity-a",
			RequireBound:       true,
		},
	})

	resp, err := transport.TakeoverSession(context.Background(), req)
	require.NoError(t, err)
	require.False(t, resp.Msg.Success)
	require.Equal(t, clusterv1.TakeoverFailureReason_TAKEOVER_FAILURE_REASON_IDENTITY_MISMATCH, resp.Msg.FailureReason)
	require.Equal(t, &SessionIdentityGuard{ExternalID: "entity-a", RequireBound: true}, handler.identity)
}
