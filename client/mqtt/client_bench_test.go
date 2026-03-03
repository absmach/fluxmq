// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"testing"
)

func setupBenchClient(b *testing.B, opts *Options) *Client {
	b.Helper()

	c, err := New(opts)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})
	b.Cleanup(func() { c.cleanup(context.Background(), nil) })

	return c
}

func BenchmarkPublishQoS0(b *testing.B) {
	c := setupBenchClient(b, NewOptions().SetClientID("bench-publish-qos0"))
	payload := []byte("benchmark-payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Publish(nil, "bench/qos0", payload, 0, false); err != nil {
			b.Fatalf("publish qos0: %v", err)
		}
	}
}

func BenchmarkPublishQoS0Parallel(b *testing.B) {
	c := setupBenchClient(b, NewOptions().SetClientID("bench-publish-qos0-par"))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		payload := []byte("parallel-benchmark")
		for pb.Next() {
			if err := c.Publish(nil, "bench/qos0-par", payload, 0, false); err != nil {
				b.Errorf("publish qos0 parallel: %v", err)
				return
			}
		}
	})
}

func BenchmarkPublishAsyncQoS1(b *testing.B) {
	c := setupBenchClient(b, NewOptions().SetClientID("bench-publish-async-qos1"))
	payload := []byte("benchmark-qos1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tok := c.PublishAsync(nil, "bench/qos1", payload, 1, false)
		if tok.MessageID == 0 {
			b.Fatalf("missing packet id for qos1 publish")
		}
		c.pending.complete(tok.MessageID, nil, nil)
		if err := tok.Wait(); err != nil {
			b.Fatalf("publish async qos1 wait: %v", err)
		}
	}
}
