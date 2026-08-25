// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

func TestClassifyErrorContract(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Failure
	}{
		// A nil error is invalid input to a failure classifier. It fails closed
		// as Internal rather than reporting a success the taxonomy cannot express.
		{name: "nil fails closed", want: Failure{Code: ErrorCodeInternal}},
		{name: "canceled", err: context.Canceled, want: Failure{Code: ErrorCodeCanceled}},
		{name: "deadline", err: context.DeadlineExceeded, want: Failure{Code: ErrorCodeDeadlineExceeded, Retryable: true, Durability: DurabilityUnconfirmed}},
		{name: "invalid config", err: fmt.Errorf("wrapped: %w", types.ErrInvalidConfig), want: Failure{Code: ErrorCodeInvalidArgument}},
		{name: "queue missing", err: storage.ErrQueueNotFound, want: Failure{Code: ErrorCodeNotFound}},
		{name: "queue exists", err: storage.ErrQueueAlreadyExists, want: Failure{Code: ErrorCodeAlreadyExists}},
		{name: "offset range", err: storage.ErrOffsetOutOfRange, want: Failure{Code: ErrorCodeOutOfRange}},
		{name: "message too large", err: ErrQueueMessageTooLarge, want: Failure{Code: ErrorCodeResourceExhausted, Durability: DurabilityNotAttempted}},
		{name: "PEL full", err: consumer.ErrPELFull, want: Failure{Code: ErrorCodeResourceExhausted, Retryable: true, Durability: DurabilityNotAttempted}},
		{name: "protected", err: ErrProtectedQueueMutation, want: Failure{Code: ErrorCodeFailedPrecondition}},
		{name: "durability unsupported", err: ErrFsyncReplicatedQueueUnsupported, want: Failure{Code: ErrorCodeFailedPrecondition, Durability: DurabilityUnsupported}},
		{name: "replication unavailable", err: ErrReplicationUnavailable, want: Failure{Code: ErrorCodeUnavailable, Retryable: true, Leader: LeaderUnavailable, Durability: DurabilityUnconfirmed}},
		{name: "owned elsewhere", err: cluster.ErrSessionOwned, want: Failure{Code: ErrorCodeConflict, Retryable: true, Ownership: OwnershipOther}},
		{name: "ownership lost", err: cluster.ErrSessionOwnershipLost, want: Failure{Code: ErrorCodeUnavailable, Retryable: true, Ownership: OwnershipLost}},
		{name: "unknown", err: errors.New("backend detail"), want: Failure{Code: ErrorCodeInternal}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := normalizeFailure(tt.want)
			if got := ClassifyError(tt.err); got != want {
				t.Fatalf("ClassifyError() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestWithFailureOverridesClassificationAndPreservesCause(t *testing.T) {
	cause := fmt.Errorf("backend: %w", storage.ErrQueueNotFound)
	want := normalizeFailure(Failure{
		Code:       ErrorCodeUnavailable,
		Retryable:  true,
		Leader:     LeaderNotLocal,
		Durability: DurabilityUnconfirmed,
	})

	err := WithFailure(cause, want)
	if !errors.Is(err, storage.ErrQueueNotFound) {
		t.Fatal("WithFailure did not preserve the wrapped cause")
	}
	if got := ClassifyError(err); got != want {
		t.Fatalf("ClassifyError() = %+v, want %+v", got, want)
	}
}
