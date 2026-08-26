// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

func benchmarkQueueDeliveryPath(b *testing.B, queueCount int, fullSweep bool) {
	b.Helper()

	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var lastOffset uint64
	var lastGroupID string
	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		lastOffset = msg.BrokerMeta.Queue.Offset
		lastGroupID = msg.BrokerMeta.Queue.GroupID
		return nil
	})

	cfg := DefaultConfig()
	cfg.DeliveryBatchSize = 1
	mgr := NewManager(logStore, groupStore, deliveryTarget, cfg, logger, nil)

	ctx := context.Background()
	for i := 0; i < queueCount; i++ {
		queueName := fmt.Sprintf("q-%d", i)
		queueCfg := types.DefaultQueueConfig(queueName, "$queue/"+queueName+"/#")
		if err := mgr.CreateQueue(ctx, queueCfg); err != nil {
			b.Fatalf("CreateQueue(%s) failed: %v", queueName, err)
		}
	}

	if err := mgr.Subscribe(ctx, "q-0", "", "worker-1", "workers", ""); err != nil {
		b.Fatalf("Subscribe failed: %v", err)
	}

	b.ResetTimer()
	published := publishEnvelope(b, "$queue/q-0/jobs", []byte("x"))

	for i := 0; i < b.N; i++ {
		lastOffset = 0
		lastGroupID = ""

		if err := mgr.Publish(ctx, published); err != nil {
			b.Fatalf("Publish failed: %v", err)
		}

		if fullSweep {
			mgr.deliverMessages()
		} else if ok := mgr.deliverQueue(ctx, "q-0"); !ok {
			b.Fatalf("deliverQueue returned no delivery")
		}

		if lastGroupID == "" {
			b.Fatalf("expected delivered group-id")
		}

		if err := mgr.Ack(ctx, "q-0", lastGroupID, lastOffset); err != nil {
			if errors.Is(err, storage.ErrConsumerNotFound) {
				b.Fatalf("Ack failed with consumer not found: %v", err)
			}
			b.Fatalf("Ack failed: %v", err)
		}
	}
}

func BenchmarkQueueDeliveryScanVsTargeted(b *testing.B) {
	for _, queueCount := range []int{100, 1000} {
		b.Run(fmt.Sprintf("full_sweep_%dq", queueCount), func(b *testing.B) {
			benchmarkQueueDeliveryPath(b, queueCount, true)
		})

		b.Run(fmt.Sprintf("targeted_queue_%dq", queueCount), func(b *testing.B) {
			benchmarkQueueDeliveryPath(b, queueCount, false)
		})
	}
}
