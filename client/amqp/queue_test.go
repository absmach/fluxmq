// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestQueueMessageStreamMetadata(t *testing.T) {
	msg := &QueueMessage{
		Delivery: amqp091.Delivery{
			Headers: amqp091.Table{
				"x-stream-offset":         "42",
				"x-stream-timestamp":      "1700000000000",
				"x-work-acked":            "true",
				"x-work-committed-offset": "100",
				"x-work-group":            "workers",
			},
		},
	}

	if off, ok := msg.StreamOffset(); !ok || off != 42 {
		t.Fatalf("expected stream offset 42, got %d (ok=%v)", off, ok)
	}
	if ts, ok := msg.StreamTimestamp(); !ok || ts != 1700000000000 {
		t.Fatalf("expected stream timestamp, got %d (ok=%v)", ts, ok)
	}
	if acked, ok := msg.WorkAcked(); !ok || !acked {
		t.Fatalf("expected work acked true, got %v (ok=%v)", acked, ok)
	}
	if committed, ok := msg.WorkCommittedOffset(); !ok || committed != 100 {
		t.Fatalf("expected committed offset 100, got %d (ok=%v)", committed, ok)
	}
	if group, ok := msg.WorkGroup(); !ok || group != "workers" {
		t.Fatalf("expected work group workers, got %q (ok=%v)", group, ok)
	}
}

func TestNormalizeStreamTopic(t *testing.T) {
	tests := []struct {
		name string
		opts *StreamConsumeOptions
		want string
	}{
		{
			name: "queue root",
			opts: &StreamConsumeOptions{
				QueueName: "events",
			},
			want: "$queue/events",
		},
		{
			name: "queue root with filter",
			opts: &StreamConsumeOptions{
				QueueName: "events",
				Filter:    "supermq/domain/#",
			},
			want: "$queue/events/supermq/domain/#",
		},
		{
			name: "full queue topic passthrough",
			opts: &StreamConsumeOptions{
				QueueName: "$queue/events",
				Filter:    "supermq/group/#",
			},
			want: "$queue/events/supermq/group/#",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeStreamTopic(tc.opts); got != tc.want {
				t.Fatalf("normalizeStreamTopic() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQueueSubscriptionKey(t *testing.T) {
	streamKey := queueSubscriptionKey("$queue/events/supermq/domain/#", "domains", true)
	queueKey := queueSubscriptionKey("$queue/events", "domains", false)

	if streamKey == queueKey {
		t.Fatalf("expected stream and queue subscription keys to differ")
	}
}
