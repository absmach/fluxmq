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
