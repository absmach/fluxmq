// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingWriteConnection accepts nothing: every data write fails as a closed
// socket's would.
type failingWriteConnection struct {
	captureConnection
}

func (c *failingWriteConnection) TryWriteDataPacket(p packets.ControlPacket, _ func()) error {
	p.Release()
	return errors.New("broken pipe")
}

// A pending message whose delivery fails stays in the inflight tracker for a
// retry. Draining it must not release it a second time: the envelope would go
// back to the pool while the tracker still holds it, and the next message to
// take it from the pool would overwrite this one.
func TestDrainedDeliveryFailureKeepsTheMessage(t *testing.T) {
	cfg := config.Default()
	b := NewBroker(memory.New(), nil, WithSessionConfig(cfg.Session))
	t.Cleanup(func() { _ = b.Close() })

	s, _, err := b.CreateSession("drain-fail", 5, session.Options{ExpiryInterval: 3600})
	require.NoError(t, err)
	_, err = s.Connect(&failingWriteConnection{})
	require.NoError(t, err)

	require.True(t, s.TryEnqueuePending(message.NewDelivery("t", []byte("kept"), 1, false), nil))
	b.drainOnePending(readLoopContext, s)

	held := s.Inflight().GetAll()
	require.Len(t, held, 1, "a failed write leaves the message for a retry")
	assert.Equal(t, "kept", string(held[0].Message.PayloadBytes()), "the retained message was released")
	assert.Equal(t, "t", held[0].Message.Topic)
}

// stalledConnection blocks every control write, as a synchronous connection
// does once the client stops reading and its socket buffer fills.
type stalledConnection struct {
	captureConnection
	release chan struct{}
}

func (c *stalledConnection) WritePacket(p packets.ControlPacket) error {
	<-c.release
	p.Release()
	return errors.New("broken pipe")
}

// Shedding a subscriber that stopped reading must not write to it: on a
// synchronous connection with a full socket buffer that write would hold the
// delivering read loop for the whole write timeout.
func TestShedSlowSubscriberDoesNotWriteToIt(t *testing.T) {
	cfg := config.Default()
	cfg.Session.PendingQueueSize = 1
	b := NewBroker(memory.New(), nil, WithSessionConfig(cfg.Session))
	t.Cleanup(func() { _ = b.Close() })

	s, _, err := b.CreateSession("stalled", 5, session.Options{ExpiryInterval: 3600, ReceiveMaximum: 1})
	require.NoError(t, err)
	conn := &stalledConnection{release: make(chan struct{})}
	t.Cleanup(func() { close(conn.release) })
	gen, err := s.Connect(conn)
	require.NoError(t, err)
	require.True(t, s.TryAcquireSendQuota(999, gen), "fill the quota")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, payload := range []string{"pended", "sheds"} {
			_, _ = b.DeliverToSession(readLoopContext, s, message.NewDelivery("t", []byte(payload), 1, false))
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shedding the subscriber blocked on a write to it")
	}

	assert.False(t, s.IsConnected())
	// The disconnect moves the offline queue to storage on its own goroutine,
	// so the messages are in one place or the other.
	require.Eventually(t, func() bool {
		stored, err := b.stores.messages.List(s.ID + queuePrefix)
		return err == nil && s.OfflineQueue().Len()+len(stored) == 2
	}, 2*time.Second, 5*time.Millisecond, "the shed subscriber's messages were not kept")
}
