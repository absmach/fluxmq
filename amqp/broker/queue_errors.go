// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"github.com/absmach/fluxmq/amqp/codec"
	queuepkg "github.com/absmach/fluxmq/queue"
)

func amqp091QueueError(err error) (uint16, string) {
	failure := queuepkg.ClassifyError(err)
	switch failure.Code {
	case queuepkg.ErrorCodeNotFound:
		return codec.NotFound, "queue resource not found"
	case queuepkg.ErrorCodeConflict:
		return codec.ResourceLocked, "queue resource is owned elsewhere"
	case queuepkg.ErrorCodeInvalidArgument,
		queuepkg.ErrorCodeAlreadyExists,
		queuepkg.ErrorCodeFailedPrecondition,
		queuepkg.ErrorCodeOutOfRange:
		return codec.PreconditionFailed, "queue operation rejected"
	case queuepkg.ErrorCodeResourceExhausted:
		return codec.ResourceError, "queue resource exhausted"
	case queuepkg.ErrorCodeUnavailable,
		queuepkg.ErrorCodeDeadlineExceeded,
		queuepkg.ErrorCodeCanceled:
		return codec.InternalError, "queue operation unavailable"
	default:
		return codec.InternalError, "queue operation failed"
	}
}
