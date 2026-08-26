// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/message"
)

func BenchmarkCreateRoutedQueueMessage(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024)

	msg := message.New("$queue/bench", payload)
	msg.User.MessageID = "bench"
	msg.Broker.Queue.Offset = 42
	defer message.Release(msg)

	b.ReportAllocs()
	for b.Loop() {
		routed := createRoutedQueueMessage(msg, "group", "bench", false, 0, false, "")
		message.Release(routed)
	}
}
