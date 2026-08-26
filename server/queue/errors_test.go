// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"testing"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/stretchr/testify/require"
)

func TestConnectQueueErrorContract(t *testing.T) {
	failure := queuepkg.Failure{
		Code:       queuepkg.ErrorCodeUnavailable,
		Retryable:  true,
		Ownership:  queuepkg.OwnershipLost,
		Leader:     queuepkg.LeaderNotLocal,
		Durability: queuepkg.DurabilityUnconfirmed,
	}
	err := queuepkg.WithFailure(errors.New("implementation detail"), failure)

	connectErr := newConnectError(queuepkg.ErrorCodeInternal, err)
	require.Equal(t, connect.CodeUnavailable, connectErr.Code())
	require.Len(t, connectErr.Details(), 1)

	value, detailErr := connectErr.Details()[0].Value()
	require.NoError(t, detailErr)
	detail, ok := value.(*queuev1.QueueErrorDetail)
	require.True(t, ok)
	require.Equal(t, queuev1.QueueErrorCode_QUEUE_ERROR_CODE_UNAVAILABLE, detail.Code)
	require.True(t, detail.Retryable)
	require.Equal(t, queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_LOST, detail.Ownership)
	require.Equal(t, queuev1.QueueLeaderState_QUEUE_LEADER_STATE_NOT_LOCAL, detail.Leader)
	require.Equal(t, queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNCONFIRMED, detail.Durability)
}

func TestDurabilityUnconfirmedProjectsToRetryableUnavailable(t *testing.T) {
	connectErr := newConnectError(queuepkg.ErrorCodeInternal, storage.ErrDurabilityUnconfirmed)
	require.Equal(t, connect.CodeUnavailable, connectErr.Code())
	require.Len(t, connectErr.Details(), 1)

	value, err := connectErr.Details()[0].Value()
	require.NoError(t, err)
	detail := value.(*queuev1.QueueErrorDetail)
	require.Equal(t, queuev1.QueueErrorCode_QUEUE_ERROR_CODE_UNAVAILABLE, detail.Code)
	require.True(t, detail.Retryable)
	require.Equal(t, queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNCONFIRMED, detail.Durability)
}

func TestConnectQueueErrorFallbackIsAlsoTyped(t *testing.T) {
	connectErr := newConnectError(queuepkg.ErrorCodeInvalidArgument, errors.New("request detail"))
	require.Equal(t, connect.CodeInvalidArgument, connectErr.Code())
	require.Len(t, connectErr.Details(), 1)

	value, err := connectErr.Details()[0].Value()
	require.NoError(t, err)
	detail := value.(*queuev1.QueueErrorDetail)
	require.Equal(t, queuev1.QueueErrorCode_QUEUE_ERROR_CODE_INVALID_ARGUMENT, detail.Code)
}
