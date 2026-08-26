// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"testing"

	"github.com/absmach/fluxmq/amqp1/performatives"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/stretchr/testify/require"
)

func TestAMQP1QueueErrorContract(t *testing.T) {
	failure := queuepkg.Failure{
		Code:       queuepkg.ErrorCodeUnavailable,
		Retryable:  true,
		Ownership:  queuepkg.OwnershipLost,
		Leader:     queuepkg.LeaderNotLocal,
		Durability: queuepkg.DurabilityUnconfirmed,
	}
	err := queuepkg.WithFailure(errors.New("detail"), failure)

	rejected, ok := amqp1QueueOutcome(err).(*performatives.Rejected)
	require.True(t, ok)
	require.NotNil(t, rejected.Error)
	require.Equal(t, performatives.ErrInternalError, rejected.Error.Condition)
	require.Equal(t, string(failure.Code), rejected.Error.Info[amqp1QueueErrorCodeKey])
	require.Equal(t, failure.Retryable, rejected.Error.Info[amqp1RetryableKey])
	require.Equal(t, string(failure.Ownership), rejected.Error.Info[amqp1OwnershipStateKey])
	require.Equal(t, string(failure.Leader), rejected.Error.Info[amqp1LeaderStateKey])
	require.Equal(t, string(failure.Durability), rejected.Error.Info[amqp1DurabilityStateKey])
}

func TestAMQP1DurabilityUnconfirmedProjection(t *testing.T) {
	rejected, ok := amqp1QueueOutcome(storage.ErrDurabilityUnconfirmed).(*performatives.Rejected)
	require.True(t, ok)
	require.Equal(t, string(queuepkg.ErrorCodeUnavailable), rejected.Error.Info[amqp1QueueErrorCodeKey])
	require.Equal(t, true, rejected.Error.Info[amqp1RetryableKey])
	require.Equal(t, string(queuepkg.DurabilityUnconfirmed), rejected.Error.Info[amqp1DurabilityStateKey])
}

func TestAMQP1QueueConditions(t *testing.T) {
	tests := []struct {
		code queuepkg.ErrorCode
		want string
	}{
		{code: queuepkg.ErrorCodeNotFound, want: string(performatives.ErrNotFound)},
		{code: queuepkg.ErrorCodeConflict, want: string(performatives.ErrResourceLocked)},
		{code: queuepkg.ErrorCodeInvalidArgument, want: string(performatives.ErrInvalidField)},
		{code: queuepkg.ErrorCodeFailedPrecondition, want: string(performatives.ErrPreconditionFailed)},
		{code: queuepkg.ErrorCodeResourceExhausted, want: string(performatives.ErrResourceLimitExceeded)},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, string(amqp1QueueCondition(tt.code)))
	}
}
