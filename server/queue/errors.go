// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	"google.golang.org/protobuf/proto"
)

// Request-shape rejections. An append that writes nothing cannot be reported as
// a success, because offset 0 is a valid offset and would be indistinguishable
// from a real append.
var (
	errEmptyBatch         = errors.New("append batch requires at least one message")
	errEmptyStream        = errors.New("append stream requires at least one message")
	errStreamQueueChanged = errors.New("append stream cannot change queue mid-stream")

	// Settlement over the public API names its consumer group. Without one the
	// broker resolves the owner by scanning every group on the queue, so the
	// cursor reported back describes whichever group happened to hold the
	// offset. The in-process adapters may still omit it; the public contract
	// may not.
	errSettlementGroupRequired = errors.New("settlement requires a group id")
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
func newConnectErrorWithProgress(fallback queuepkg.ErrorCode, err error, progress progressSetter) *connect.Error {
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

// progressSetter applies operation-specific progress to an error detail.
//
// The generated oneof wrapper interface is unexported, so the two progress
// shapes cannot be named by a shared type from here. A setter keeps the call
// sites uniform while still making it impossible to attach append progress to a
// settlement error, or the reverse: only the matching constructor can build one.
// A nil setter means the operation applied nothing and so reports no progress.
type progressSetter func(*queuev1.QueueErrorDetail)

func failureToProto(failure queuepkg.Failure, progress progressSetter) *queuev1.QueueErrorDetail {
	detail := &queuev1.QueueErrorDetail{
		Code:       protoErrorCodes[failure.Code],
		Retryable:  failure.Retryable,
		Ownership:  protoOwnershipStates[failure.Ownership],
		Leader:     protoLeaderStates[failure.Leader],
		Durability: protoDurabilityStates[failure.Durability],
	}
	if progress != nil {
		progress(detail)
	}
	return detail
}

// settlementProgress converts a partial settlement outcome into the wire detail.
// failedOffset is the offset the command stopped on, which the caller supplied
// and so can be named exactly. It returns nil when nothing was applied, so a
// total failure carries no progress at all.
func settlementProgress(outcome queuepkg.SettlementOutcome, failedOffset uint64) progressSetter {
	if len(outcome.Offsets) == 0 {
		return nil
	}
	return func(detail *queuev1.QueueErrorDetail) {
		detail.Progress = &queuev1.QueueErrorDetail_SettlementProgress{
			SettlementProgress: &queuev1.SettlementProgress{
				ProcessedCount: proto.Uint32(uint32(len(outcome.Offsets))),
				FailedOffset:   proto.Uint64(failedOffset),
				Committed:      proto.Uint64(outcome.Committed),
				Cursor:         proto.Uint64(outcome.Cursor),
			},
		}
	}
}

// appendProgress reports the prefix an append committed before failing.
//
// The failed record was never appended, so it has no offset to report; its
// position in the request is the only coordinate that identifies it. That
// position is the count of records already committed.
func appendProgress(processed uint32, firstOffset, lastOffset uint64) progressSetter {
	if processed == 0 {
		return nil
	}
	return func(detail *queuev1.QueueErrorDetail) {
		detail.Progress = &queuev1.QueueErrorDetail_AppendProgress{
			AppendProgress: &queuev1.AppendProgress{
				ProcessedCount: proto.Uint32(processed),
				FailedIndex:    proto.Uint32(processed),
				FirstOffset:    proto.Uint64(firstOffset),
				LastOffset:     proto.Uint64(lastOffset),
			},
		}
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
