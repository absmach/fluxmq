// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A disconnect writes the offline queue to storage. A client that resumes the
// session still in memory must get those messages back, in the order they
// queued and ahead of anything queued since — past ten of them as well, where
// storage's byte-ordered keys would otherwise put 10 before 2.
func TestResumedSessionReclaimsPersistedOfflineQueue(t *testing.T) {
	cfg := config.Default()
	b := NewBroker(memory.New(), nil, WithSessionConfig(cfg.Session))
	t.Cleanup(func() { _ = b.Close() })

	s, _, err := b.CreateSession("reclaim", 4, session.Options{ExpiryInterval: 3600})
	require.NoError(t, err)

	var want []string
	enqueue := func(payload string) {
		msg := message.NewDelivery("reclaim/t", []byte(payload), 1, false)
		require.NoError(t, s.OfflineQueue().Enqueue(msg))
		message.Release(msg)
		want = append(want, payload)
	}
	for i := range 12 {
		enqueue(fmt.Sprintf("at-disconnect-%02d", i))
	}
	b.persistSessionMessages(s)
	require.Zero(t, s.OfflineQueue().Len(), "disconnecting moves the queue to storage")
	enqueue("after-disconnect")

	require.NoError(t, b.reclaimPersistedQueue(s))

	var got []string
	for _, msg := range s.OfflineQueue().Drain() {
		got = append(got, string(msg.PayloadBytes()))
		message.Release(msg)
	}
	assert.Equal(t, want, got)
}
