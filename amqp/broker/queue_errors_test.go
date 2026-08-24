// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/stretchr/testify/require"
)

func TestAMQP091QueueErrorContract(t *testing.T) {
	tests := []struct {
		name string
		code queuepkg.ErrorCode
		want uint16
	}{
		{name: "missing", code: queuepkg.ErrorCodeNotFound, want: codec.NotFound},
		{name: "owned", code: queuepkg.ErrorCodeConflict, want: codec.ResourceLocked},
		{name: "invalid", code: queuepkg.ErrorCodeInvalidArgument, want: codec.PreconditionFailed},
		{name: "precondition", code: queuepkg.ErrorCodeFailedPrecondition, want: codec.PreconditionFailed},
		{name: "resource exhausted", code: queuepkg.ErrorCodeResourceExhausted, want: codec.ResourceError},
		{name: "unavailable", code: queuepkg.ErrorCodeUnavailable, want: codec.InternalError},
		{name: "internal", code: queuepkg.ErrorCodeInternal, want: codec.InternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := queuepkg.WithFailure(errors.New("detail"), queuepkg.Failure{Code: tt.code})
			got, _ := amqp091QueueError(err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAMQP091UnconfirmedQueueFailureClosesChannel(t *testing.T) {
	ch, output := newTestChannel(t)
	ch.conn.broker.queueManager = &mockChannelQueueManager{publishErr: queuepkg.WithFailure(
		errors.New("backend detail"),
		queuepkg.Failure{Code: queuepkg.ErrorCodeResourceExhausted, Durability: queuepkg.DurabilityNotAttempted},
	)}

	ch.handleQueuePublish("$queue/orders/process", []byte("payload"), nil, "publisher")

	closeMethod := decodeSingleChannelClose(t, output)
	require.Equal(t, uint16(codec.ResourceError), closeMethod.ReplyCode)
	require.Equal(t, uint16(codec.ClassBasic), closeMethod.ClassID)
	require.Equal(t, uint16(codec.MethodBasicPublish), closeMethod.MethodID)
}
