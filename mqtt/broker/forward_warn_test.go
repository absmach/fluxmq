// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncBuffer guards concurrent writes from broker goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestForwardPublishWarnsWhenNoLocalSubscription(t *testing.T) {
	var logBuf syncBuffer
	b := NewBroker(nil, nil, WithLogger(slog.New(slog.NewTextHandler(&logBuf, nil))))
	defer b.Close()

	err := b.ForwardPublish(context.Background(), &cluster.Message{
		Topic:   "no/subscribers/here",
		Payload: []byte("lost?"),
		QoS:     1,
	})
	require.NoError(t, err)

	logged := logBuf.String()
	require.Contains(t, logged, "forwarded publish matched no local subscription")
	require.Contains(t, logged, "no/subscribers/here")

	// Immediately repeated misses are throttled to one warning.
	err = b.ForwardPublish(context.Background(), &cluster.Message{
		Topic:   "still/no/subscribers",
		Payload: []byte("lost?"),
		QoS:     1,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(logBuf.String(), "forwarded publish matched no local subscription"))
}

func TestForwardPublishDoesNotWarnWhenSubscriptionMatches(t *testing.T) {
	var logBuf syncBuffer
	b := NewBroker(nil, nil, WithLogger(slog.New(slog.NewTextHandler(&logBuf, nil))))
	defer b.Close()

	require.NoError(t, b.router.Subscribe("local-sub", "telemetry/#", 1, storage.SubscribeOptions{}))

	err := b.ForwardPublish(context.Background(), &cluster.Message{
		Topic:   testTelemetryRoom,
		Payload: []byte("hello"),
		QoS:     1,
	})
	require.NoError(t, err)
	assert.NotContains(t, logBuf.String(), "forwarded publish matched no local subscription")
}
