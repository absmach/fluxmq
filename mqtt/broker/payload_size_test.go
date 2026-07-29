// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPayloadLimitBroker builds a broker whose configured maximum message size is
// maxSize, with one connected session.
func newPayloadLimitBroker(t *testing.T, maxSize int, clientVersion byte) (*Broker, *connCtx) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(memory.New(), cluster.NewNoopCluster("test"), WithLogger(logger),
		WithBrokerConfig(config.BrokerConfig{MaxMessageSize: maxSize}))
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	s, _, err := b.CreateSession("client1", clientVersion, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.NoError(t, b.subscribe(s, testTopic, 0, storage.SubscribeOptions{}))
	s.SetMaxQoS(b.MaxQoS())

	return b, &connCtx{Session: s, conn: &mockConnection{}, epoch: s.Epoch()}
}

// TestPublishEnforcesMaxMessageSize guards the limit broker.max_message_size
// documents. The transports cap the whole packet with an allowance for topic and
// properties, so without a payload check a small configured limit would accept a
// payload many times its size.
func TestPublishEnforcesMaxMessageSize(t *testing.T) {
	const maxSize = 1024

	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "under_limit", size: maxSize - 1},
		{name: "at_limit", size: maxSize},
		{name: "one_byte_over", size: maxSize + 1, wantErr: true},
		{name: "far_over_but_within_packet_headroom", size: maxSize + 32*1024, wantErr: true},
	}

	for _, tt := range tests {
		t.Run("v5/"+tt.name, func(t *testing.T) {
			b, cc := newPayloadLimitBroker(t, maxSize, 5)

			err := newV5Handler(b).HandlePublish(cc, &v5.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
				TopicName:   testTopic,
				Payload:     bytes.Repeat([]byte("x"), tt.size),
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrPacketTooLarge)
				return
			}
			require.NoError(t, err)
		})

		t.Run("v3/"+tt.name, func(t *testing.T) {
			b, cc := newPayloadLimitBroker(t, maxSize, 4)

			err := newV3Handler(b).HandlePublish(cc, &v3.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
				TopicName:   testTopic,
				Payload:     bytes.Repeat([]byte("x"), tt.size),
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrPacketTooLarge)
				return
			}
			require.NoError(t, err)
		})
	}
}

// expandingHook rewrites every publish payload to a fixed size.
type expandingHook struct {
	size int
}

func (h *expandingHook) HandleHook(_ context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookResult, error) {
	return corebroker.BlockingHookResult{
		Allowed:    true,
		Topic:      req.Topic,
		Payload:    bytes.Repeat([]byte("y"), h.size),
		PayloadSet: true,
		QoS:        req.QoS,
		Retain:     req.Retain,
		Properties: req.Properties,
	}, nil
}

// TestPublishRejectsHookExpandedPayload covers the other direction: a hook can
// rewrite the payload after the inbound check, so the limit is re-applied to the
// result. Overshooting is the hook's doing, so the publish is refused without
// tearing the client's connection down.
func TestPublishRejectsHookExpandedPayload(t *testing.T) {
	const maxSize = 1024

	b, cc := newPayloadLimitBroker(t, maxSize, 5)
	b.SetBlockingHooks(corebroker.NewBlockingHookEngine(&expandingHook{size: maxSize * 4}, corebroker.HookFailDeny, nil, nil, nil))

	err := newV5Handler(b).HandlePublish(cc, &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
		TopicName:   testTopic,
		Payload:     []byte("small"),
	})
	assert.ErrorIs(t, err, ErrPacketTooLarge)
}

// TestPublishUnlimitedWhenUnset keeps the opt-out working.
func TestPublishUnlimitedWhenUnset(t *testing.T) {
	b, cc := newPayloadLimitBroker(t, 0, 5)

	require.NoError(t, newV5Handler(b).HandlePublish(cc, &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
		TopicName:   testTopic,
		Payload:     bytes.Repeat([]byte("x"), 1<<20),
	}))
}
