// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
)

// Request-shape rejections. An append that writes nothing cannot be reported as
// a success, because offset 0 is a valid offset and would be indistinguishable
// from a real append.
var (
	errEmptyBatch         = errors.New("append batch requires at least one message")
	errEmptyStream        = errors.New("append stream requires at least one message")
	errStreamQueueChanged = errors.New("append stream cannot change queue mid-stream")
)

// The queue failure taxonomy is protocol-independent; these tables are its
// projection onto Connect and onto the QueueService protobuf contract. They are
// declared together, and TestErrorProjectionTablesAreExhaustive fails when a
// newly added domain value is missing from any of them — a switch with a
// default arm would instead degrade the new value to Internal in silence.

var connectCodes = map[queuepkg.ErrorCode]connect.Code{
	queuepkg.ErrorCodeCanceled:           connect.CodeCanceled,
	queuepkg.ErrorCodeInvalidArgument:    connect.CodeInvalidArgument,
	queuepkg.ErrorCodeNotFound:           connect.CodeNotFound,
	queuepkg.ErrorCodeAlreadyExists:      connect.CodeAlreadyExists,
	queuepkg.ErrorCodeConflict:           connect.CodeAborted,
	queuepkg.ErrorCodeFailedPrecondition: connect.CodeFailedPrecondition,
	queuepkg.ErrorCodeResourceExhausted:  connect.CodeResourceExhausted,
	queuepkg.ErrorCodeOutOfRange:         connect.CodeOutOfRange,
	queuepkg.ErrorCodeUnavailable:        connect.CodeUnavailable,
	queuepkg.ErrorCodeDeadlineExceeded:   connect.CodeDeadlineExceeded,
	queuepkg.ErrorCodeInternal:           connect.CodeInternal,
}

var protoErrorCodes = map[queuepkg.ErrorCode]queuev1.QueueErrorCode{
	queuepkg.ErrorCodeCanceled:           queuev1.QueueErrorCode_QUEUE_ERROR_CODE_CANCELED,
	queuepkg.ErrorCodeInvalidArgument:    queuev1.QueueErrorCode_QUEUE_ERROR_CODE_INVALID_ARGUMENT,
	queuepkg.ErrorCodeNotFound:           queuev1.QueueErrorCode_QUEUE_ERROR_CODE_NOT_FOUND,
	queuepkg.ErrorCodeAlreadyExists:      queuev1.QueueErrorCode_QUEUE_ERROR_CODE_ALREADY_EXISTS,
	queuepkg.ErrorCodeConflict:           queuev1.QueueErrorCode_QUEUE_ERROR_CODE_CONFLICT,
	queuepkg.ErrorCodeFailedPrecondition: queuev1.QueueErrorCode_QUEUE_ERROR_CODE_FAILED_PRECONDITION,
	queuepkg.ErrorCodeResourceExhausted:  queuev1.QueueErrorCode_QUEUE_ERROR_CODE_RESOURCE_EXHAUSTED,
	queuepkg.ErrorCodeOutOfRange:         queuev1.QueueErrorCode_QUEUE_ERROR_CODE_OUT_OF_RANGE,
	queuepkg.ErrorCodeUnavailable:        queuev1.QueueErrorCode_QUEUE_ERROR_CODE_UNAVAILABLE,
	queuepkg.ErrorCodeDeadlineExceeded:   queuev1.QueueErrorCode_QUEUE_ERROR_CODE_DEADLINE_EXCEEDED,
	queuepkg.ErrorCodeInternal:           queuev1.QueueErrorCode_QUEUE_ERROR_CODE_INTERNAL,
}

var protoOwnershipStates = map[queuepkg.OwnershipState]queuev1.QueueOwnershipState{
	queuepkg.OwnershipUnspecified: queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_UNSPECIFIED,
	queuepkg.OwnershipCaller:      queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_CALLER,
	queuepkg.OwnershipOther:       queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_OTHER,
	queuepkg.OwnershipLost:        queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_LOST,
}

var protoLeaderStates = map[queuepkg.LeaderState]queuev1.QueueLeaderState{
	queuepkg.LeaderUnspecified: queuev1.QueueLeaderState_QUEUE_LEADER_STATE_UNSPECIFIED,
	queuepkg.LeaderRequired:    queuev1.QueueLeaderState_QUEUE_LEADER_STATE_REQUIRED,
	queuepkg.LeaderUnavailable: queuev1.QueueLeaderState_QUEUE_LEADER_STATE_UNAVAILABLE,
	queuepkg.LeaderNotLocal:    queuev1.QueueLeaderState_QUEUE_LEADER_STATE_NOT_LOCAL,
}

var protoDurabilityStates = map[queuepkg.DurabilityState]queuev1.QueueDurabilityState{
	queuepkg.DurabilityUnspecified:  queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNSPECIFIED,
	queuepkg.DurabilityNotAttempted: queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_NOT_ATTEMPTED,
	queuepkg.DurabilityUnconfirmed:  queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNCONFIRMED,
	queuepkg.DurabilityUnsupported:  queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNSUPPORTED,
}

// newConnectError projects a queue failure onto a Connect error. fallback is the
// method's own classification for errors the shared taxonomy cannot recognise;
// it is a domain code rather than a Connect code so this function never has to
// translate backwards.
//
// Every QueueService error carries the same typed QueueErrorDetail so clients
// never need to parse its message.
func newConnectError(fallback queuepkg.ErrorCode, err error) *connect.Error {
	return newConnectErrorWithProgress(fallback, err, nil)
}

// newConnectErrorWithProgress is newConnectError for an operation that applied
// part of a multi-entry request before failing. progress travels in the same
// typed detail, so a client learns the committed prefix from the error itself
// rather than having to re-read the queue.
func newConnectErrorWithProgress(fallback queuepkg.ErrorCode, err error, progress *queuev1.QueueProgressDetail) *connect.Error {
	failure := queuepkg.ClassifyError(err)
	if failure.Code == queuepkg.ErrorCodeInternal {
		failure.Code = fallback
	}

	connectErr := connect.NewError(connectCode(failure.Code), err)
	detail, detailErr := connect.NewErrorDetail(failureToProto(failure, progress))
	if detailErr != nil {
		// The contract above promises a typed detail on every error. Losing it
		// pushes the client back to parsing the message, so it is worth saying
		// loudly rather than degrading in silence.
		slog.Error("queue error detail could not be attached",
			slog.String("code", failure.Code.String()),
			slog.String("error", detailErr.Error()))
		return connectErr
	}
	connectErr.AddDetail(detail)
	return connectErr
}

func connectCode(code queuepkg.ErrorCode) connect.Code {
	if mapped, ok := connectCodes[code]; ok {
		return mapped
	}
	return connect.CodeInternal
}

func failureToProto(failure queuepkg.Failure, progress *queuev1.QueueProgressDetail) *queuev1.QueueErrorDetail {
	return &queuev1.QueueErrorDetail{
		Code:       protoErrorCodes[failure.Code],
		Retryable:  failure.Retryable,
		Ownership:  protoOwnershipStates[failure.Ownership],
		Leader:     protoLeaderStates[failure.Leader],
		Durability: protoDurabilityStates[failure.Durability],
		Progress:   progress,
	}
}

// settlementProgress converts a partial settlement outcome into the wire detail.
// failedOffset is the offset the command stopped on. It returns nil when nothing
// was applied, so a total failure carries no progress at all.
func settlementProgress(outcome queuepkg.SettlementOutcome, failedOffset uint64) *queuev1.QueueProgressDetail {
	if len(outcome.Offsets) == 0 {
		return nil
	}
	return &queuev1.QueueProgressDetail{
		ProcessedCount: uint32(len(outcome.Offsets)),
		FailedOffset:   failedOffset,
		Committed:      outcome.Committed,
	}
}

// appendProgress reports the offset range an append committed before failing.
// It returns nil when nothing was committed.
func appendProgress(processed uint32, firstOffset, lastOffset uint64) *queuev1.QueueProgressDetail {
	if processed == 0 {
		return nil
	}
	return &queuev1.QueueProgressDetail{
		ProcessedCount: processed,
		FirstOffset:    firstOffset,
		LastOffset:     lastOffset,
	}
}

// failedOffset returns the offset a settlement command stopped on: settlement
// applies offsets in request order, so the first unsettled one is the failure.
func failedOffset(requested []uint64, settled int) uint64 {
	if settled < 0 || settled >= len(requested) {
		return 0
	}
	return requested[settled]
}
