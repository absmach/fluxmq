// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestPeerUptime covers the clock-skew clamp. StartedAt is the peer's own wall
// clock, so an unsynchronised peer can report a start time in this node's
// future, and uptime_seconds must not go negative for it.
func TestPeerUptime(t *testing.T) {
	cases := []struct {
		name      string
		startedAt time.Time
		assert    func(t *testing.T, uptime time.Duration)
	}{
		{
			name:      "unset start time reports no uptime",
			startedAt: time.Time{},
			assert: func(t *testing.T, uptime time.Duration) {
				t.Helper()
				assert.Zero(t, uptime)
			},
		},
		{
			name:      "past start time reports its age",
			startedAt: time.Now().Add(-time.Hour),
			assert: func(t *testing.T, uptime time.Duration) {
				t.Helper()
				assert.InDelta(t, time.Hour.Seconds(), uptime.Seconds(), 5)
			},
		},
		{
			name:      "peer clock ahead of ours clamps to zero",
			startedAt: time.Now().Add(time.Hour),
			assert: func(t *testing.T, uptime time.Duration) {
				t.Helper()
				assert.Zero(t, uptime)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.assert(t, peerUptime(tc.startedAt))
		})
	}
}

// TestRetryNodeMetadataRegistrationStops covers the retry's shutdown contract:
// it runs under the cluster WaitGroup, so a node that never manages to
// register must not hold Stop() open.
func TestRetryNodeMetadataRegistrationStops(t *testing.T) {
	c := &EtcdCluster{
		stopCh: make(chan struct{}),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	close(c.stopCh)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.retryNodeMetadataRegistration()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retryNodeMetadataRegistration did not return after stopCh closed")
	}
}

// TestDeregisterNodeMetadataWithoutClient covers the shutdown path of a
// cluster whose client never came up: Stop() calls this unconditionally.
func TestDeregisterNodeMetadataWithoutClient(t *testing.T) {
	c := &EtcdCluster{
		nodeID: "node-1",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	assert.NotPanics(t, c.deregisterNodeMetadata)
}
