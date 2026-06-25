// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
)

// mockAddr implements net.Addr for testing.
type mockAddr struct{}

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return "127.0.0.1:1883" }

// mockConnection implements core.Connection for testing.
type mockConnection struct {
	net.Conn
	packets []packets.ControlPacket
}

func (m *mockConnection) WritePacket(p packets.ControlPacket) error {
	return m.WriteControlPacket(p, nil)
}

func (m *mockConnection) WriteControlPacket(p packets.ControlPacket, onSent func()) error {
	m.packets = append(m.packets, p)
	if onSent != nil {
		onSent()
	}
	return nil
}

func (m *mockConnection) WriteDataPacket(p packets.ControlPacket, onSent func()) error {
	m.packets = append(m.packets, p)
	if onSent != nil {
		onSent()
	}
	return nil
}

func (m *mockConnection) TryWriteDataPacket(p packets.ControlPacket, onSent func()) error {
	return m.WriteDataPacket(p, onSent)
}

func (m *mockConnection) ReadPacket() (packets.ControlPacket, error) {
	return nil, io.EOF
}

func (m *mockConnection) Close() error {
	return nil
}

func (m *mockConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConnection) SetKeepAlive(d time.Duration) error {
	return nil
}

func (m *mockConnection) SetOnDisconnect(fn func(graceful bool)) {
	// no-op
}

func (m *mockConnection) Touch() {
	// no-op
}

func (m *mockConnection) RemoteAddr() net.Addr {
	return &mockAddr{}
}

func (m *mockConnection) LocalAddr() net.Addr {
	return &mockAddr{}
}

func TestBroker_HandleV5Connect(t *testing.T) {
	t.Log("Creating broker")
	b := NewBroker(nil, nil)
	defer b.Close()

	conn := &mockConnection{}

	connect := &v5.Connect{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.ConnectType,
		},
		ProtocolName:    testProtocolMQTT,
		ProtocolVersion: 5,
		ClientID:        testMQTTClientID,
		CleanStart:      true,
		KeepAlive:       60,
	}

	t.Log("Calling HandleConnect via V5Handler")
	handler := NewV5Handler(b)
	err := handler.HandleConnect(conn, connect)
	t.Logf("HandleConnect returned: %v", err)

	// Expect nil or io.EOF (because runSession exits on EOF)
	if err != nil && err != io.EOF {
		t.Fatalf("HandleConnect failed: %v", err)
	}

	// Verify ConnAck was sent
	if len(conn.packets) == 0 {
		t.Fatal("Expected ConnAck, got no packets")
	}

	ack, ok := conn.packets[0].(*v5.ConnAck)
	if !ok {
		t.Fatalf("Expected v5.ConnAck, got %T", conn.packets[0])
	}

	if ack.ReasonCode != 0x00 {
		t.Errorf("Expected success reason code 0x00, got 0x%02x", ack.ReasonCode)
	}
}

func TestBroker_MaxSessions(t *testing.T) {
	b := NewBroker(nil, nil,
		WithSessionConfig(config.SessionConfig{MaxSessions: 2}),
	)
	defer b.Close()

	opts := session.Options{CleanStart: true}

	_, _, err := b.CreateSession("client-1", 5, opts)
	if err != nil {
		t.Fatalf("first session: %v", err)
	}
	_, _, err = b.CreateSession("client-2", 5, opts)
	if err != nil {
		t.Fatalf("second session: %v", err)
	}

	_, _, err = b.CreateSession("client-3", 5, opts)
	if !errors.Is(err, ErrMaxSessionsExceeded) {
		t.Fatalf("expected ErrMaxSessionsExceeded, got %v", err)
	}
}

func TestBroker_MaxSessionsZeroMeansUnlimited(t *testing.T) {
	b := NewBroker(nil, nil,
		WithSessionConfig(config.SessionConfig{MaxSessions: 0}),
	)
	defer b.Close()

	opts := session.Options{CleanStart: true}
	for i := range 5 {
		clientID := "client-" + string(rune('a'+i))
		if _, _, err := b.CreateSession(clientID, 5, opts); err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
	}
}

func TestBroker_MaxSessionsExistingClientNotCounted(t *testing.T) {
	b := NewBroker(nil, nil,
		WithSessionConfig(config.SessionConfig{MaxSessions: 1}),
	)
	defer b.Close()

	opts := session.Options{CleanStart: true}
	if _, _, err := b.CreateSession("client-1", 5, opts); err != nil {
		t.Fatalf("first session: %v", err)
	}

	// Reconnect with the same client ID (CleanStart=true destroys and recreates).
	// Session count stays at 1, so this must succeed.
	if _, _, err := b.CreateSession("client-1", 5, opts); err != nil {
		t.Fatalf("reconnect of existing client: %v", err)
	}
}

func TestBroker_DefaultExpiryInterval(t *testing.T) {
	b := NewBroker(nil, nil,
		WithSessionConfig(config.SessionConfig{DefaultExpiryInterval: 600}),
	)
	defer b.Close()

	// Persistent session with no expiry specified → should get DefaultExpiryInterval.
	opts := session.Options{CleanStart: false, ExpiryInterval: 0}
	s, _, err := b.CreateSession("client-1", 5, opts)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if s.ExpiryInterval != 600 {
		t.Errorf("expected ExpiryInterval=600, got %d", s.ExpiryInterval)
	}
}

func TestBroker_DefaultExpiryIntervalNotAppliedWhenSet(t *testing.T) {
	b := NewBroker(nil, nil,
		WithSessionConfig(config.SessionConfig{DefaultExpiryInterval: 600}),
	)
	defer b.Close()

	// Client explicitly requests 120s — default must not override it.
	opts := session.Options{CleanStart: false, ExpiryInterval: 120}
	s, _, err := b.CreateSession("client-1", 5, opts)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if s.ExpiryInterval != 120 {
		t.Errorf("expected ExpiryInterval=120, got %d", s.ExpiryInterval)
	}
}

func TestBroker_DefaultExpiryIntervalZeroAllowsIndefinite(t *testing.T) {
	b := NewBroker(nil, nil,
		WithSessionConfig(config.SessionConfig{DefaultExpiryInterval: 0}),
	)
	defer b.Close()

	// DefaultExpiryInterval=0 means no forced cap; persistent sessions stay at 0.
	opts := session.Options{CleanStart: false, ExpiryInterval: 0}
	s, _, err := b.CreateSession("client-1", 5, opts)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if s.ExpiryInterval != 0 {
		t.Errorf("expected ExpiryInterval=0, got %d", s.ExpiryInterval)
	}
}
