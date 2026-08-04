// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"testing"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// PublishToMatchingQueues runs on the publish path of every protocol, including
// for topics that match no queue at all. That case is the one to watch: a broker
// whose queues are addressed through "$queue/" pays it on every ordinary publish
// and never captures anything, so it must stay allocation-free.
func BenchmarkPublishToMatchingQueues(b *testing.B) {
	newManager := func(b *testing.B, queueCount int, capturePattern string) *Manager {
		b.Helper()
		manager := NewManager(memlog.New(), newMockGroupStore(), nil, DefaultConfig(), nil, nil)
		ctx := context.Background()
		for i := range queueCount {
			name := fmt.Sprintf("queue-%d", i)
			if err := manager.CreateQueue(ctx, types.DefaultQueueConfig(name, "$queue/"+name+"/#")); err != nil {
				b.Fatalf("CreateQueue %s failed: %v", name, err)
			}
		}
		if capturePattern != "" {
			if err := manager.CreateQueue(ctx, types.DefaultQueueConfig("messages", capturePattern)); err != nil {
				b.Fatalf("CreateQueue messages failed: %v", err)
			}
		}
		return manager
	}

	benchmarks := []struct {
		name           string
		queueCount     int
		capturePattern string
	}{
		{name: "no_match/32_queues", queueCount: 32},
		{name: "no_match/512_queues", queueCount: 512},
		{name: "captured/32_queues", queueCount: 32, capturePattern: "m/#"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			manager := newManager(b, bm.queueCount, bm.capturePattern)
			ctx := context.Background()
			payload := []byte("payload")

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := manager.PublishToMatchingQueues(ctx, types.PublishRequest{
					ClientID: "publisher",
					Topic:    "m/acme/c/temp/reading",
					Payload:  payload,
				}); err != nil {
					b.Fatalf("PublishToMatchingQueues failed: %v", err)
				}
			}
		})
	}
}
