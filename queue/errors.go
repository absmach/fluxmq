// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// ErrorCode is the stable, protocol-independent class of a queue operation
// failure. Protocol adapters map these values to their native wire errors;
// callers must not infer behavior from an error string.
//
// These values are deliberately independent of any transport numbering. The
// protobuf enum, the Connect code, and the AMQP 1.0 symbol are all projections
// of this type, declared in their own adapters.
type ErrorCode string

// String returns the stable wire spelling of the code. Adapters that put the
// code on a wire must use this rather than converting the type directly, so the
// external vocabulary stays decoupled from the Go identifier.
func (c ErrorCode) String() string { return string(c) }

const (
	ErrorCodeCanceled           ErrorCode = "canceled"
	ErrorCodeInvalidArgument    ErrorCode = "invalid_argument"
	ErrorCodeNotFound           ErrorCode = "not_found"
	ErrorCodeAlreadyExists      ErrorCode = "already_exists"
	ErrorCodeConflict           ErrorCode = "conflict"
	ErrorCodeFailedPrecondition ErrorCode = "failed_precondition"
	ErrorCodeResourceExhausted  ErrorCode = "resource_exhausted"
	ErrorCodeOutOfRange         ErrorCode = "out_of_range"
	ErrorCodeUnavailable        ErrorCode = "unavailable"
	ErrorCodeDeadlineExceeded   ErrorCode = "deadline_exceeded"
	ErrorCodeInternal           ErrorCode = "internal"
)

// OwnershipState describes whether ownership contributed to a failure.
type OwnershipState string

// String returns the stable wire spelling of the state.
func (s OwnershipState) String() string { return string(s) }

const (
	OwnershipUnspecified OwnershipState = "unspecified"
	OwnershipCaller      OwnershipState = "caller"
	OwnershipOther       OwnershipState = "other"
	OwnershipLost        OwnershipState = "lost"
)

// LeaderState describes the replication-leader condition observed by an
// operation.
type LeaderState string

// String returns the stable wire spelling of the state.
func (s LeaderState) String() string { return string(s) }

const (
	LeaderUnspecified LeaderState = "unspecified"
	LeaderRequired    LeaderState = "required"
	LeaderUnavailable LeaderState = "unavailable"
	LeaderNotLocal    LeaderState = "not_local"
)

// DurabilityState describes what can be asserted about an unsuccessful write.
type DurabilityState string

// String returns the stable wire spelling of the state.
func (s DurabilityState) String() string { return string(s) }

const (
	DurabilityUnspecified  DurabilityState = "unspecified"
	DurabilityNotAttempted DurabilityState = "not_attempted"
	DurabilityUnconfirmed  DurabilityState = "unconfirmed"
	DurabilityUnsupported  DurabilityState = "unsupported"
)

// Failure is the stable semantic description of a queue error. It deliberately
// contains no implementation name (storage backend, coordinator, or protocol).
type Failure struct {
	Code       ErrorCode
	Retryable  bool
	Ownership  OwnershipState
	Leader     LeaderState
	Durability DurabilityState
}

// FailureError attaches an explicit semantic failure to an implementation
// error while preserving errors.Is/errors.As through Unwrap.
type FailureError struct {
	failure Failure
	cause   error
}

// WithFailure annotates err with an explicit queue failure. A nil error remains
// nil so callers can use it directly on return paths.
func WithFailure(err error, failure Failure) error {
	if err == nil {
		return nil
	}
	return &FailureError{failure: normalizeFailure(failure), cause: err}
}

func (e *FailureError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return string(e.failure.Code)
}

// Unwrap preserves the implementation error for errors.Is/errors.As callers.
func (e *FailureError) Unwrap() error { return e.cause }

// QueueFailure returns the stable failure carried by this error.
func (e *FailureError) QueueFailure() Failure { return e.failure }

type failureCarrier interface {
	QueueFailure() Failure
}

// ClassifyError converts implementation errors into the stable queue failure
// contract. New implementation errors default to Internal until explicitly
// classified; their strings never become protocol behavior.
//
// Passing a nil error is invalid input: the taxonomy describes failures and has
// no success value. It fails closed as Internal rather than inventing one, so a
// caller that reaches this with nil reports "cannot classify", not "success".
func ClassifyError(err error) Failure {
	if err == nil {
		return normalizeFailure(Failure{Code: ErrorCodeInternal})
	}

	var carrier failureCarrier
	if errors.As(err, &carrier) {
		return normalizeFailure(carrier.QueueFailure())
	}

	switch {
	case errors.Is(err, context.Canceled):
		return normalizeFailure(Failure{Code: ErrorCodeCanceled})
	case errors.Is(err, context.DeadlineExceeded):
		return normalizeFailure(Failure{Code: ErrorCodeDeadlineExceeded, Retryable: true, Durability: DurabilityUnconfirmed})
	case errors.Is(err, cluster.ErrSessionOwned):
		return normalizeFailure(Failure{Code: ErrorCodeConflict, Retryable: true, Ownership: OwnershipOther})
	case errors.Is(err, cluster.ErrSessionOwnershipLost):
		return normalizeFailure(Failure{Code: ErrorCodeUnavailable, Retryable: true, Ownership: OwnershipLost})
	case errors.Is(err, cluster.ErrTakeoverInProgress):
		return normalizeFailure(Failure{Code: ErrorCodeConflict, Retryable: true, Ownership: OwnershipOther})
	case errors.Is(err, types.ErrInvalidConfig),
		errors.Is(err, ErrInvalidCommand),
		errors.Is(err, storage.ErrInvalidOffset),
		errors.Is(err, consumer.ErrInvalidOffset):
		return normalizeFailure(Failure{Code: ErrorCodeInvalidArgument})
	case errors.Is(err, storage.ErrQueueNotFound),
		errors.Is(err, storage.ErrMessageNotFound),
		errors.Is(err, storage.ErrConsumerNotFound),
		errors.Is(err, storage.ErrPendingEntryNotFound),
		errors.Is(err, consumer.ErrGroupNotFound),
		errors.Is(err, consumer.ErrConsumerNotFound),
		errors.Is(err, consumer.ErrMessageNotPending),
		errors.Is(err, consumer.ErrNoMessages):
		return normalizeFailure(Failure{Code: ErrorCodeNotFound})
	case errors.Is(err, storage.ErrQueueAlreadyExists),
		errors.Is(err, storage.ErrConsumerGroupExists):
		return normalizeFailure(Failure{Code: ErrorCodeAlreadyExists})
	case errors.Is(err, storage.ErrOffsetOutOfRange):
		return normalizeFailure(Failure{Code: ErrorCodeOutOfRange})
	case errors.Is(err, ErrQueueMessageTooLarge):
		return normalizeFailure(Failure{Code: ErrorCodeResourceExhausted, Durability: DurabilityNotAttempted})
	case errors.Is(err, storage.ErrLogFull),
		errors.Is(err, consumer.ErrPELFull):
		return normalizeFailure(Failure{Code: ErrorCodeResourceExhausted, Retryable: true, Durability: DurabilityNotAttempted})
	case errors.Is(err, ErrDurableSyncUnsupported),
		errors.Is(err, ErrDurableReplicatedStreamUnsupported),
		errors.Is(err, ErrFsyncReplicatedQueueUnsupported),
		errors.Is(err, ErrAtomicBatchDurabilityUnsupported):
		return normalizeFailure(Failure{Code: ErrorCodeFailedPrecondition, Durability: DurabilityUnsupported})
	case errors.Is(err, ErrReplicationUnavailable),
		errors.Is(err, raft.ErrRaftDisabled):
		return normalizeFailure(Failure{
			Code:       ErrorCodeUnavailable,
			Retryable:  true,
			Leader:     LeaderUnavailable,
			Durability: DurabilityUnconfirmed,
		})
	case errors.Is(err, ErrCaptureStillRunning):
		return normalizeFailure(Failure{Code: ErrorCodeUnavailable, Retryable: true})
	case errors.Is(err, ErrQueueNotStream),
		errors.Is(err, ErrQueueNotDurable),
		errors.Is(err, ErrQueueNotReserved),
		errors.Is(err, ErrQueueNotProtected),
		errors.Is(err, ErrProtectedQueueMutation),
		errors.Is(err, ErrProtectedQueueContractDrift),
		errors.Is(err, ErrDLQDisabled),
		errors.Is(err, ErrReplicationWritePolicy),
		errors.Is(err, ErrAtomicBatchReplicationUnsupported),
		errors.Is(err, consumer.ErrGroupModeMismatch),
		errors.Is(err, consumer.ErrCommitOffsetOnlyForStreamMode),
		errors.Is(err, consumer.ErrCommitOffsetNotMonotonic),
		errors.Is(err, consumer.ErrNackNotSupportedForStream),
		errors.Is(err, ErrAckOnlyForAutoCommitStream),
		errors.Is(err, consumer.ErrDLQHandlerUnavailable),
		errors.Is(err, consumer.ErrDelayedNackUnsupported):
		return normalizeFailure(Failure{Code: ErrorCodeFailedPrecondition})
	default:
		return normalizeFailure(Failure{Code: ErrorCodeInternal})
	}
}

func normalizeFailure(failure Failure) Failure {
	if failure.Code == "" {
		failure.Code = ErrorCodeInternal
	}
	if failure.Ownership == "" {
		failure.Ownership = OwnershipUnspecified
	}
	if failure.Leader == "" {
		failure.Leader = LeaderUnspecified
	}
	if failure.Durability == "" {
		failure.Durability = DurabilityUnspecified
	}
	return failure
}
