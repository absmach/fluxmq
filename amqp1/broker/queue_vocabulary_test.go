// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"testing"

	"github.com/absmach/fluxmq/amqp1/performatives"
	amqptypes "github.com/absmach/fluxmq/amqp1/types"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fluxmq:* info keys and their value vocabulary are an external contract:
// AMQP 1.0 clients branch on them instead of parsing the error description.
// They are not covered by the protobuf compatibility baseline, so this test is
// what pins them. A change here is a change to a published contract, not a
// refactor — update the table in API-COMPATIBILITY.md in the same commit.
func TestAMQP1QueueVocabularyIsStable(t *testing.T) {
	t.Run("info keys", func(t *testing.T) {
		assert.Equal(t, amqptypes.Symbol("fluxmq:queue-error-code"), amqp1QueueErrorCodeKey)
		assert.Equal(t, amqptypes.Symbol("fluxmq:retryable"), amqp1RetryableKey)
		assert.Equal(t, amqptypes.Symbol("fluxmq:ownership"), amqp1OwnershipStateKey)
		assert.Equal(t, amqptypes.Symbol("fluxmq:leader"), amqp1LeaderStateKey)
		assert.Equal(t, amqptypes.Symbol("fluxmq:durability"), amqp1DurabilityStateKey)
	})

	t.Run("management property names", func(t *testing.T) {
		assert.Equal(t, "errorCode", amqp1ManagementErrorCodeKey)
		assert.Equal(t, "retryable", amqp1ManagementRetryableKey)
		assert.Equal(t, "ownership", amqp1ManagementOwnershipKey)
		assert.Equal(t, "leader", amqp1ManagementLeaderKey)
		assert.Equal(t, "durability", amqp1ManagementDurabilityKey)
	})

	t.Run("error code values", func(t *testing.T) {
		want := map[queuepkg.ErrorCode]string{
			queuepkg.ErrorCodeCanceled:           "canceled",
			queuepkg.ErrorCodeInvalidArgument:    "invalid_argument",
			queuepkg.ErrorCodeNotFound:           "not_found",
			queuepkg.ErrorCodeAlreadyExists:      "already_exists",
			queuepkg.ErrorCodeConflict:           "conflict",
			queuepkg.ErrorCodeFailedPrecondition: "failed_precondition",
			queuepkg.ErrorCodeResourceExhausted:  "resource_exhausted",
			queuepkg.ErrorCodeOutOfRange:         "out_of_range",
			queuepkg.ErrorCodeUnavailable:        "unavailable",
			queuepkg.ErrorCodeDeadlineExceeded:   "deadline_exceeded",
			queuepkg.ErrorCodeInternal:           "internal",
		}
		for code, spelling := range want {
			assert.Equal(t, spelling, code.String())
		}
	})

	t.Run("state values", func(t *testing.T) {
		assert.Equal(t, "unspecified", queuepkg.OwnershipUnspecified.String())
		assert.Equal(t, "caller", queuepkg.OwnershipCaller.String())
		assert.Equal(t, "other", queuepkg.OwnershipOther.String())
		assert.Equal(t, "lost", queuepkg.OwnershipLost.String())

		assert.Equal(t, "unspecified", queuepkg.LeaderUnspecified.String())
		assert.Equal(t, "required", queuepkg.LeaderRequired.String())
		assert.Equal(t, "unavailable", queuepkg.LeaderUnavailable.String())
		assert.Equal(t, "not_local", queuepkg.LeaderNotLocal.String())

		assert.Equal(t, "unspecified", queuepkg.DurabilityUnspecified.String())
		assert.Equal(t, "not_attempted", queuepkg.DurabilityNotAttempted.String())
		assert.Equal(t, "unconfirmed", queuepkg.DurabilityUnconfirmed.String())
		assert.Equal(t, "unsupported", queuepkg.DurabilityUnsupported.String())
	})
}

// The rejected outcome must carry all five fields with the pinned spellings, so
// a client can read them without parsing the description.
func TestAMQP1QueueOutcomeCarriesFullVocabulary(t *testing.T) {
	failure := queuepkg.Failure{
		Code:       queuepkg.ErrorCodeUnavailable,
		Retryable:  true,
		Ownership:  queuepkg.OwnershipLost,
		Leader:     queuepkg.LeaderNotLocal,
		Durability: queuepkg.DurabilityUnconfirmed,
	}
	outcome := amqp1QueueOutcome(queuepkg.WithFailure(errors.New("replication unavailable"), failure))

	rejected, ok := outcome.(*performatives.Rejected)
	require.True(t, ok, "queue failures must produce a rejected outcome")
	require.NotNil(t, rejected.Error)
	require.Equal(t, performatives.ErrInternalError, rejected.Error.Condition)

	assert.Equal(t, "unavailable", rejected.Error.Info[amqp1QueueErrorCodeKey])
	assert.Equal(t, true, rejected.Error.Info[amqp1RetryableKey])
	assert.Equal(t, "lost", rejected.Error.Info[amqp1OwnershipStateKey])
	assert.Equal(t, "not_local", rejected.Error.Info[amqp1LeaderStateKey])
	assert.Equal(t, "unconfirmed", rejected.Error.Info[amqp1DurabilityStateKey])
}
