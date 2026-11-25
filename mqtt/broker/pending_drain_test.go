// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"testing"

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
