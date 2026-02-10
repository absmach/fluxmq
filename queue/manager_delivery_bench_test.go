// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

func benchmarkQueueDeliveryPath(b *testing.B, queueCount int, fullSweep bool) {
	b.Helper()

	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var lastMessageID string
	var lastGroupID string
	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		if msg.Properties != nil {
			lastMessageID = msg.Properties[types.PropMessageID]
			lastGroupID = msg.Properties[types.PropGroupID]
		}
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
	for i := 0; i < b.N; i++ {
		lastMessageID = ""
		lastGroupID = ""

		if err := mgr.Publish(ctx, types.PublishRequest{
			Topic:   "$queue/q-0/jobs",
			Payload: []byte("x"),
		}); err != nil {
			b.Fatalf("Publish failed: %v", err)
		}

		if fullSweep {
			mgr.deliverMessages()
		} else if ok := mgr.deliverQueue(ctx, "q-0"); !ok {
			b.Fatalf("deliverQueue returned no delivery")
		}

		if lastMessageID == "" {
			b.Fatalf("expected delivered message-id")
		}
		if lastGroupID == "" {
			b.Fatalf("expected delivered group-id")
		}

		if err := mgr.Ack(ctx, "q-0", lastMessageID, lastGroupID); err != nil {
			if err == storage.ErrConsumerNotFound {
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
