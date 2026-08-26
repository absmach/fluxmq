// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
)

func TestAppendRejectsInvalidEnvelopesBeforeWriting(t *testing.T) {
	for backendName, factory := range stateMachineBackendFactories() {
		t.Run(backendName, func(t *testing.T) {
			backend := factory(t)
			ctx := context.Background()
			manager := NewManager(backend.queue, backend.group, nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if err := manager.CreateQueue(ctx, types.DefaultQueueConfig(testQueueJobs, "jobs/#")); err != nil {
				t.Fatalf("create queue: %v", err)
			}

			valid := publishEnvelope(t, "jobs/valid", []byte("valid"))
			for name, envelopes := range map[string][]*message.Envelope{
				"single nil":   {nil},
				"invalid tail": {valid, nil},
			} {
				t.Run(name, func(t *testing.T) {
					_, err := manager.StateMachine().Append(ctx, AppendCommand{
						QueueName:   testQueueJobs,
						Envelopes:   envelopes,
						AtomicBatch: len(envelopes) > 1,
					})
					if !errors.Is(err, ErrInvalidCommand) {
						t.Fatalf("append error = %v, want invalid command", err)
					}
					count, countErr := backend.queue.Count(ctx, testQueueJobs)
					if countErr != nil || count != 0 {
						t.Fatalf("queue count = %d, error = %v; want 0", count, countErr)
					}
				})
			}

			for _, version := range []message.Version{0, message.Version1 + 1} {
				invalid := publishEnvelope(t, "jobs/version", []byte("invalid"))
				invalid.Version = version
				_, err := manager.StateMachine().Append(ctx, AppendCommand{QueueName: testQueueJobs, Envelopes: []*message.Envelope{invalid}})
				if !errors.Is(err, ErrInvalidCommand) || !errors.Is(err, message.ErrUnsupportedVersion) {
					t.Fatalf("version %d append error = %v, want invalid command and unsupported version", version, err)
				}
			}
		})
	}
}

func TestPublishEntryPointsRejectInvalidEnvelopeVersions(t *testing.T) {
	ctx := context.Background()
	backend := stateMachineBackendFactories()["memory"](t)
	manager := NewManager(backend.queue, backend.group, nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err := manager.CreateQueue(ctx, types.DefaultQueueConfig(testQueueJobs, "jobs/#")); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	for name, publish := range map[string]func(context.Context, *message.Envelope) error{
		"publish": manager.Publish,
		"capture": manager.PublishToMatchingQueues,
	} {
		t.Run(name, func(t *testing.T) {
			invalid := publishEnvelope(t, "jobs/version", []byte("invalid"))
			invalid.Version = 0
			err := publish(ctx, invalid)
			if !errors.Is(err, ErrInvalidCommand) || !errors.Is(err, message.ErrUnsupportedVersion) {
				t.Fatalf("publish error = %v, want invalid command and unsupported version", err)
			}
			count, countErr := backend.queue.Count(ctx, testQueueJobs)
			if countErr != nil || count != 0 {
				t.Fatalf("queue count = %d, error = %v; want 0", count, countErr)
			}
		})
	}
}
