// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"github.com/absmach/fluxmq/amqp1/performatives"
	amqptypes "github.com/absmach/fluxmq/amqp1/types"
	queuepkg "github.com/absmach/fluxmq/queue"
)

// These five keys and the value vocabulary they carry are the AMQP 1.0
// projection of the queue failure taxonomy, and they are an external contract:
// clients branch on them instead of parsing the error description. The values
// come from the domain type's String method rather than a direct conversion, so
// the wire spelling stays decoupled from the Go identifier.
// TestAMQP1QueueVocabularyIsStable pins every key and value.
const (
	amqp1QueueErrorCodeKey  amqptypes.Symbol = "fluxmq:queue-error-code"
	amqp1RetryableKey       amqptypes.Symbol = "fluxmq:retryable"
	amqp1OwnershipStateKey  amqptypes.Symbol = "fluxmq:ownership"
	amqp1LeaderStateKey     amqptypes.Symbol = "fluxmq:leader"
	amqp1DurabilityStateKey amqptypes.Symbol = "fluxmq:durability"
)

// The management endpoint carries the same vocabulary as plain
// application-property names, because management responses are ordinary AMQP
// messages rather than error performatives.
const (
	amqp1ManagementErrorCodeKey  = "errorCode"
	amqp1ManagementRetryableKey  = "retryable"
	amqp1ManagementOwnershipKey  = "ownership"
	amqp1ManagementLeaderKey     = "leader"
	amqp1ManagementDurabilityKey = "durability"
)

func amqp1QueueOutcome(err error) any {
	failure := queuepkg.ClassifyError(err)
	info := map[amqptypes.Symbol]any{
		amqp1QueueErrorCodeKey:  failure.Code.String(),
		amqp1RetryableKey:       failure.Retryable,
		amqp1OwnershipStateKey:  failure.Ownership.String(),
		amqp1LeaderStateKey:     failure.Leader.String(),
		amqp1DurabilityStateKey: failure.Durability.String(),
	}
	return &performatives.Rejected{Error: &performatives.Error{
		Condition:   amqp1QueueCondition(failure.Code),
		Description: "queue operation failed",
		Info:        info,
	}}
}

func amqp1QueueCondition(code queuepkg.ErrorCode) amqptypes.Symbol {
	switch code {
	case queuepkg.ErrorCodeNotFound:
		return performatives.ErrNotFound
	case queuepkg.ErrorCodeConflict:
		return performatives.ErrResourceLocked
	case queuepkg.ErrorCodeInvalidArgument,
		queuepkg.ErrorCodeOutOfRange:
		return performatives.ErrInvalidField
	case queuepkg.ErrorCodeAlreadyExists,
		queuepkg.ErrorCodeFailedPrecondition:
		return performatives.ErrPreconditionFailed
	case queuepkg.ErrorCodeResourceExhausted:
		return performatives.ErrResourceLimitExceeded
	default:
		return performatives.ErrInternalError
	}
}

func amqp1ManagementStatus(code queuepkg.ErrorCode) int32 {
	switch code {
	case queuepkg.ErrorCodeInvalidArgument,
		queuepkg.ErrorCodeOutOfRange:
		return 400
	case queuepkg.ErrorCodeNotFound:
		return 404
	case queuepkg.ErrorCodeAlreadyExists,
		queuepkg.ErrorCodeConflict:
		return 409
	case queuepkg.ErrorCodeFailedPrecondition:
		return 412
	case queuepkg.ErrorCodeResourceExhausted:
		return 429
	case queuepkg.ErrorCodeUnavailable,
		queuepkg.ErrorCodeDeadlineExceeded:
		return 503
	default:
		return 500
	}
}
