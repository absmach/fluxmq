// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxQoS_DefaultValue(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, WithLogger(logger))

	if got := b.MaxQoS(); got != 2 {
		t.Errorf("Default MaxQoS() = %d, want 2", got)
	}
}

func TestMaxQoS_SetValue(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, WithLogger(logger))

	tests := []struct {
		name    string
		setQoS  byte
		wantQoS byte
	}{
		{"set to 0", 0, 0},
		{"set to 1", 1, 1},
		{"set to 2", 2, 2},
		{"set to 3 (clamped to 2)", 3, 2},
		{"set to 255 (clamped to 2)", 255, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b.SetMaxQoS(tt.setQoS)
			if got := b.MaxQoS(); got != tt.wantQoS {
				t.Errorf("MaxQoS() = %d, want %d", got, tt.wantQoS)
			}
		})
	}
}

// newMaxQoSBroker builds a broker with the given Maximum QoS and a session
// whose writes are captured, so a test can assert which acknowledgement (if
// any) a PUBLISH produced.
func newMaxQoSBroker(t *testing.T, maxQoS byte, clientVersion byte) (*Broker, *connCtx, *mockConnection) {
	t.Helper()

	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, WithLogger(logger))
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	b.SetMaxQoS(maxQoS)

	s, _, err := b.CreateSession("client1", clientVersion, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.NoError(t, b.subscribe(s, testTopic, 2, storage.SubscribeOptions{}))

	// The advertised maximum travels with the connection, so the helper attaches
	// one the way CONNECT does rather than poking at session state.
	conn := &mockConnection{}
	epoch, _ := s.ConnectWithOptions(conn, session.ConnectOptions{
		Version:        clientVersion,
		ReceiveMaximum: 65535,
		MaxQoS:         b.MaxQoS(),
	})
	return b, &connCtx{Session: s, conn: conn, epoch: epoch}, conn
}

// ackTypes reports the acknowledgement packets written to the connection.
// A delivery to the session's own subscription is not an acknowledgement, so
// PUBLISH is filtered out.
func ackTypes(conn *mockConnection) []byte {
	types := make([]byte, 0, len(conn.packets))
	for _, pkt := range conn.packets {
		switch pkt.Type() {
		case packets.PubAckType, packets.PubRecType, packets.PubRelType, packets.PubCompType:
			types = append(types, pkt.Type())
		}
	}
	return types
}

// TestMaxQoS_InboundHandshakeIsNotDowngraded is the regression guard for the
// acknowledgement handshake: the QoS a client published at determines the reply,
// so it must never be rewritten by the server's Maximum QoS. A publish within
// the limit gets its own acknowledgement; one above it is refused outright
// rather than answered with the wrong packet — or with silence, which leaves the
// publisher retransmitting forever.
func TestMaxQoS_InboundHandshakeIsNotDowngraded(t *testing.T) {
	tests := []struct {
		name        string
		maxQoS      byte
		publishQoS  byte
		wantErr     error
		wantAckType byte
	}{
		{name: "qos1/within_limit", maxQoS: 2, publishQoS: 1, wantAckType: packets.PubAckType},
		{name: "qos2/within_limit", maxQoS: 2, publishQoS: 2, wantAckType: packets.PubRecType},
		{name: "qos2/at_limit", maxQoS: 2, publishQoS: 2, wantAckType: packets.PubRecType},
		{name: "qos2/above_limit_1", maxQoS: 1, publishQoS: 2, wantErr: ErrQoSNotSupported},
		{name: "qos2/above_limit_0", maxQoS: 0, publishQoS: 2, wantErr: ErrQoSNotSupported},
		{name: "qos1/above_limit_0", maxQoS: 0, publishQoS: 1, wantErr: ErrQoSNotSupported},
	}

	for _, tt := range tests {
		t.Run("v5/"+tt.name, func(t *testing.T) {
			b, cc, conn := newMaxQoSBroker(t, tt.maxQoS, 5)

			err := newV5Handler(b).HandlePublish(cc, &v5.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: tt.publishQoS},
				TopicName:   testTopic,
				Payload:     []byte("test data"),
				ID:          1,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.NotContains(t, ackTypes(conn), packets.PubAckType,
					"a refused publish must not be acknowledged as if it were accepted")
				return
			}
			require.NoError(t, err)
			assert.Contains(t, ackTypes(conn), tt.wantAckType,
				"acknowledgement must match the QoS the client published at")
		})

		t.Run("v3/"+tt.name, func(t *testing.T) {
			b, cc, conn := newMaxQoSBroker(t, tt.maxQoS, 4)

			err := newV3Handler(b).HandlePublish(cc, &v3.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: tt.publishQoS},
				TopicName:   testTopic,
				Payload:     []byte("test data"),
				ID:          1,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.NotContains(t, ackTypes(conn), packets.PubAckType,
					"a refused publish must not be acknowledged as if it were accepted")
				return
			}
			require.NoError(t, err)
			assert.Contains(t, ackTypes(conn), tt.wantAckType,
				"acknowledgement must match the QoS the client published at")
		})
	}
}

// TestMaxQoS_QoS0IsAlwaysAccepted keeps the unacknowledged path working at every
// Maximum QoS setting.
func TestMaxQoS_QoS0IsAlwaysAccepted(t *testing.T) {
	for _, maxQoS := range []byte{0, 1, 2} {
		t.Run(fmt.Sprintf("max_qos_%d", maxQoS), func(t *testing.T) {
			b, cc, conn := newMaxQoSBroker(t, maxQoS, 5)

			err := newV5Handler(b).HandlePublish(cc, &v5.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
				TopicName:   testTopic,
				Payload:     []byte("test data"),
			})
			require.NoError(t, err)
			assert.Empty(t, ackTypes(conn), "QoS 0 publishes are not acknowledged")
		})
	}
}

// TestMaxQoS_ReloadDoesNotAffectConnectedClients covers hot reload: a client is
// told the maximum QoS once, in its CONNACK. Lowering the limit afterwards must
// not disconnect it for publishing at the QoS it was granted — only connections
// established after the change see the new limit.
func TestMaxQoS_ReloadDoesNotAffectConnectedClients(t *testing.T) {
	b, cc, conn := newMaxQoSBroker(t, 2, 5)

	// The configuration is lowered while the client is connected.
	b.SetMaxQoS(1)

	err := newV5Handler(b).HandlePublish(cc, &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   testTopic,
		Payload:     []byte("test data"),
		ID:          1,
	})
	require.NoError(t, err, "a client must not be refused a QoS it was granted at CONNECT")
	assert.Contains(t, ackTypes(conn), byte(packets.PubRecType))

	// A connection established after the change is held to the new limit.
	s2, _, err := b.CreateSession("client2", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	conn2 := &mockConnection{}
	epoch2, _ := s2.ConnectWithOptions(conn2, session.ConnectOptions{
		Version:        5,
		ReceiveMaximum: 65535,
		MaxQoS:         b.MaxQoS(),
	})
	cc2 := &connCtx{Session: s2, conn: conn2, epoch: epoch2}

	err = newV5Handler(b).HandlePublish(cc2, &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   testTopic,
		Payload:     []byte("test data"),
		ID:          1,
	})
	require.ErrorIs(t, err, ErrQoSNotSupported)
}

// TestMaxQoS_SupersededConnectDoesNotOverwriteSnapshot covers the takeover race:
// the advertised maximum travels with the connection epoch, so a CONNECT that
// loses a takeover cannot leave the replacement connection enforcing a limit
// other than the one its own CONNACK announced.
func TestMaxQoS_SupersededConnectDoesNotOverwriteSnapshot(t *testing.T) {
	b, cc, _ := newMaxQoSBroker(t, 2, 5)
	s := cc.Session

	// A replacement connection is admitted while the maximum is still 2.
	replacement := &mockConnection{}
	epoch, superseded := s.ConnectWithOptions(replacement, session.ConnectOptions{
		Version:        5,
		ReceiveMaximum: 65535,
		MaxQoS:         b.MaxQoS(),
	})
	require.NotNil(t, superseded, "the original connection must be superseded")

	// The configuration is lowered, and a late CONNECT for the superseded
	// generation applies its own (now lower) value.
	b.SetMaxQoS(1)
	require.Equal(t, byte(2), s.MaxQoS(),
		"a reload must not reach a connection that was already admitted")

	// The replacement keeps the QoS it was granted.
	err := newV5Handler(b).HandlePublish(&connCtx{Session: s, conn: replacement, epoch: epoch}, &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   testTopic,
		Payload:     []byte("test data"),
		ID:          1,
	})
	require.NoError(t, err)
	assert.Contains(t, ackTypes(replacement), byte(packets.PubRecType))
}
