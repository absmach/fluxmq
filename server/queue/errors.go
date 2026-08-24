// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
)

// newConnectError preserves an intentional method-specific Connect code for
// otherwise-unclassified errors and uses the shared queue taxonomy whenever a
// domain/storage error is known. Every QueueService error carries the same
// typed QueueErrorDetail so clients never need to parse its message.
func newConnectError(fallback connect.Code, err error) *connect.Error {
	failure := queuepkg.ClassifyError(err)
	if failure.Code == queuepkg.ErrorCodeInternal && fallback != connect.CodeInternal {
		failure.Code = errorCodeFromConnect(fallback)
	}

	code := connectCode(failure.Code)
	if failure.Code == queuepkg.ErrorCodeInternal {
		code = fallback
	}

	connectErr := connect.NewError(code, err)
	detail, detailErr := connect.NewErrorDetail(failureToProto(failure))
	if detailErr == nil {
		connectErr.AddDetail(detail)
	}
	return connectErr
}

func connectCode(code queuepkg.ErrorCode) connect.Code {
	switch code {
	case queuepkg.ErrorCodeCanceled:
		return connect.CodeCanceled
	case queuepkg.ErrorCodeInvalidArgument:
		return connect.CodeInvalidArgument
	case queuepkg.ErrorCodeNotFound:
		return connect.CodeNotFound
	case queuepkg.ErrorCodeAlreadyExists:
		return connect.CodeAlreadyExists
	case queuepkg.ErrorCodeConflict:
		return connect.CodeAborted
	case queuepkg.ErrorCodeFailedPrecondition:
		return connect.CodeFailedPrecondition
	case queuepkg.ErrorCodeResourceExhausted:
		return connect.CodeResourceExhausted
	case queuepkg.ErrorCodeOutOfRange:
		return connect.CodeOutOfRange
	case queuepkg.ErrorCodeUnavailable:
		return connect.CodeUnavailable
	case queuepkg.ErrorCodeDeadlineExceeded:
		return connect.CodeDeadlineExceeded
	default:
		return connect.CodeInternal
	}
}

func errorCodeFromConnect(code connect.Code) queuepkg.ErrorCode {
	switch code {
	case connect.CodeCanceled:
		return queuepkg.ErrorCodeCanceled
	case connect.CodeInvalidArgument:
		return queuepkg.ErrorCodeInvalidArgument
	case connect.CodeNotFound:
		return queuepkg.ErrorCodeNotFound
	case connect.CodeAlreadyExists:
		return queuepkg.ErrorCodeAlreadyExists
	case connect.CodeAborted:
		return queuepkg.ErrorCodeConflict
	case connect.CodeFailedPrecondition:
		return queuepkg.ErrorCodeFailedPrecondition
	case connect.CodeResourceExhausted:
		return queuepkg.ErrorCodeResourceExhausted
	case connect.CodeOutOfRange:
		return queuepkg.ErrorCodeOutOfRange
	case connect.CodeUnavailable:
		return queuepkg.ErrorCodeUnavailable
	case connect.CodeDeadlineExceeded:
		return queuepkg.ErrorCodeDeadlineExceeded
	default:
		return queuepkg.ErrorCodeInternal
	}
}

func failureToProto(failure queuepkg.Failure) *queuev1.QueueErrorDetail {
	return &queuev1.QueueErrorDetail{
		Code:       errorCodeToProto(failure.Code),
		Retryable:  failure.Retryable,
		Ownership:  ownershipToProto(failure.Ownership),
		Leader:     leaderToProto(failure.Leader),
		Durability: durabilityToProto(failure.Durability),
	}
}

func errorCodeToProto(code queuepkg.ErrorCode) queuev1.QueueErrorCode {
	switch code {
	case queuepkg.ErrorCodeCanceled:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_CANCELED
	case queuepkg.ErrorCodeInvalidArgument:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_INVALID_ARGUMENT
	case queuepkg.ErrorCodeNotFound:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_NOT_FOUND
	case queuepkg.ErrorCodeAlreadyExists:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_ALREADY_EXISTS
	case queuepkg.ErrorCodeConflict:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_CONFLICT
	case queuepkg.ErrorCodeFailedPrecondition:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_FAILED_PRECONDITION
	case queuepkg.ErrorCodeResourceExhausted:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_RESOURCE_EXHAUSTED
	case queuepkg.ErrorCodeOutOfRange:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_OUT_OF_RANGE
	case queuepkg.ErrorCodeUnavailable:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_UNAVAILABLE
	case queuepkg.ErrorCodeDeadlineExceeded:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_DEADLINE_EXCEEDED
	default:
		return queuev1.QueueErrorCode_QUEUE_ERROR_CODE_INTERNAL
	}
}

func ownershipToProto(state queuepkg.OwnershipState) queuev1.QueueOwnershipState {
	switch state {
	case queuepkg.OwnershipCaller:
		return queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_CALLER
	case queuepkg.OwnershipOther:
		return queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_OTHER
	case queuepkg.OwnershipLost:
		return queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_LOST
	default:
		return queuev1.QueueOwnershipState_QUEUE_OWNERSHIP_STATE_UNSPECIFIED
	}
}

func leaderToProto(state queuepkg.LeaderState) queuev1.QueueLeaderState {
	switch state {
	case queuepkg.LeaderRequired:
		return queuev1.QueueLeaderState_QUEUE_LEADER_STATE_REQUIRED
	case queuepkg.LeaderUnavailable:
		return queuev1.QueueLeaderState_QUEUE_LEADER_STATE_UNAVAILABLE
	case queuepkg.LeaderNotLocal:
		return queuev1.QueueLeaderState_QUEUE_LEADER_STATE_NOT_LOCAL
	default:
		return queuev1.QueueLeaderState_QUEUE_LEADER_STATE_UNSPECIFIED
	}
}

func durabilityToProto(state queuepkg.DurabilityState) queuev1.QueueDurabilityState {
	switch state {
	case queuepkg.DurabilityNotAttempted:
		return queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_NOT_ATTEMPTED
	case queuepkg.DurabilityUnconfirmed:
		return queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNCONFIRMED
	case queuepkg.DurabilityUnsupported:
		return queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNSUPPORTED
	default:
		return queuev1.QueueDurabilityState_QUEUE_DURABILITY_STATE_UNSPECIFIED
	}
}
