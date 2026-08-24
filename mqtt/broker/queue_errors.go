// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	queuepkg "github.com/absmach/fluxmq/queue"
)

// mqtt5QueuePublishError maps the protocol-independent queue failure onto the
// reason space available to MQTT 5 PUBLISH acknowledgements. MQTT 3.1.1 has no
// negative PUBACK/PUBREC reason code and therefore closes the connection by
// returning the original error.
func mqtt5QueuePublishError(err error) (byte, string) {
	failure := queuepkg.ClassifyError(err)
	switch failure.Code {
	case queuepkg.ErrorCodeResourceExhausted:
		return v5.PubAckQuotaExceeded, "Queue resource exhausted"
	case queuepkg.ErrorCodeInvalidArgument,
		queuepkg.ErrorCodeNotFound,
		queuepkg.ErrorCodeAlreadyExists,
		queuepkg.ErrorCodeConflict,
		queuepkg.ErrorCodeFailedPrecondition,
		queuepkg.ErrorCodeOutOfRange:
		return v5.PubAckImplementationSpecificError, "Queue operation rejected"
	default:
		return v5.PubAckUnspecifiedError, "Queue operation unavailable"
	}
}
