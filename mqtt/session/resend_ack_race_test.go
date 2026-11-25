// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publishRecorder records the PUBLISH packets written to it.
type publishRecorder struct {
	testConn
	mu   sync.Mutex
	sent []string
}

func (r *publishRecorder) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	if pub, ok := pkt.(*v5.Publish); ok {
		r.mu.Lock()
		r.sent = append(r.sent, pub.TopicName+"|"+string(pub.Payload))
		r.mu.Unlock()
	}
	pkt.Release()
	if onSent != nil {
		onSent()
	}
	return nil
}

func (r *publishRecorder) published() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

// A resumed session resends its unacknowledged messages beside the read loop
// that processes acknowledgements, waiting for quota between them. A message
// acknowledged during that wait is released; resending must neither encode
// its released envelope nor keep the quota token it took for it.
func TestResendSkipsMessagesAcknowledgedWhileWaitingForQuota(t *testing.T) {
	s := New("resend", packets.V5, Options{ReceiveMaximum: 1},
		messages.NewInflightTracker(16), messages.NewMessageQueue(16, true), config.SessionConfig{})
	conn := &publishRecorder{}
	gen, err := s.Connect(conn)
	require.NoError(t, err)

	for id, payload := range map[uint16]string{1: "first", 2: "second"} {
		require.NoError(t, s.Inflight().Add(id, message.NewDelivery("t/"+payload, []byte(payload), 1, false), messages.Outbound))
		s.Inflight().MarkSent(id)
	}

	done := make(chan struct{})
	go func() {
		s.ResendUnacknowledgedTo(conn, gen)
		close(done)
	}()

	// With a quota of one, the resend sends one message and waits.
	require.Eventually(t, func() bool { return len(conn.published()) == 1 }, time.Second, time.Millisecond)
	first := conn.published()[0]
	waiting := map[string]uint16{"t/first|first": 2, "t/second|second": 1}[first]
	sentID := 3 - waiting

	// The client acknowledges the waiting message, then the sent one, which
	// frees the quota the resend is waiting for.
	acked, err := s.Inflight().Ack(waiting)
	require.NoError(t, err)
	message.Release(acked)
	acked, err = s.Inflight().Ack(sentID)
	require.NoError(t, err)
	message.Release(acked)
	s.ReleaseSendQuota(sentID, gen)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resend did not finish once the quota was free")
	}
	assert.Equal(t, []string{first}, conn.published(), "an acknowledged message was resent")
	assert.True(t, s.HasSendQuota(gen), "the token taken for the acknowledged message was kept")
}
