// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/absmach/mqtt/core/packets"
	v5 "github.com/absmach/mqtt/core/packets/v5"
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
	m.packets = append(m.packets, p)
	return nil
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
	b := NewBroker(nil, nil, nil, nil)
	defer b.Close()

	conn := &mockConnection{}

	connect := &v5.Connect{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.ConnectType,
		},
		ProtocolName:    "MQTT",
		ProtocolVersion: 5,
		ClientID:        "test-client",
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
