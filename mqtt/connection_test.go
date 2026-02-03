// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt_test

import (
	"io"
	"net"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockTCPConnection(t *testing.T) (net.Conn, net.Conn) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close()
		clientConn.Close()
	})
	return serverConn, clientConn
}

func TestNewConnection(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)
	require.NotNil(t, conn)
}

func TestConnectionReadPacketV3(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	go func() {
		connect := &v3.Connect{
			FixedHeader:     v3.FixedHeader{PacketType: v3.ConnectType},
			ProtocolName:    "MQTT",
			ProtocolVersion: 4,
			CleanSession:    true,
			KeepAlive:       60,
			ClientID:        "test-client",
		}
		encoded := connect.Encode()
		clientConn.Write(encoded)
	}()

	pkt, err := conn.ReadPacket()
	require.NoError(t, err)
	require.NotNil(t, pkt)

	connect, ok := pkt.(*v3.Connect)
	require.True(t, ok)
	assert.Equal(t, "test-client", connect.ClientID)
	assert.Equal(t, byte(4), connect.ProtocolVersion)
}

func TestConnectionReadPacketV5(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	go func() {
		connect := &v5.Connect{
			FixedHeader:     v5.FixedHeader{PacketType: v5.ConnectType},
			ProtocolName:    "MQTT",
			ProtocolVersion: v5.V5,
			CleanStart:      true,
			KeepAlive:       60,
			ClientID:        "test-client-v5",
		}
		encoded := connect.Encode()
		clientConn.Write(encoded)
	}()

	pkt, err := conn.ReadPacket()
	require.NoError(t, err)
	require.NotNil(t, pkt)

	connect, ok := pkt.(*v5.Connect)
	require.True(t, ok)
	assert.Equal(t, "test-client-v5", connect.ClientID)
	assert.Equal(t, v5.V5, connect.ProtocolVersion)
}

func TestConnectionWritePacket(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	decodedCh := make(chan *v3.ConnAck, 1)
	errCh := make(chan error, 1)
	go func() {
		pkt, err := v3.ReadPacket(clientConn)
		if err != nil {
			errCh <- err
			return
		}
		ack, ok := pkt.(*v3.ConnAck)
		if !ok {
			errCh <- assert.AnError
			return
		}
		decodedCh <- ack
	}()

	connack := &v3.ConnAck{
		FixedHeader:    v3.FixedHeader{PacketType: v3.ConnAckType},
		SessionPresent: false,
		ReturnCode:     0,
	}

	err := conn.WritePacket(connack)
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case receivedConnack := <-decodedCh:
		assert.Equal(t, byte(0), receivedConnack.ReturnCode)
	}
}

func TestConnectionSetKeepAlive(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	err := conn.SetKeepAlive(30 * time.Second)
	assert.NoError(t, err)
}

func TestConnectionTouch(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	conn.Touch()

	conn.Touch()
}

func TestConnectionOnDisconnect(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	called := false
	conn.SetOnDisconnect(func(g bool) {
		called = true
	})

	assert.False(t, called)
}

func TestConnectionReadWriteDeadline(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	deadline := time.Now().Add(5 * time.Second)

	err := conn.SetReadDeadline(deadline)
	assert.NoError(t, err)

	err = conn.SetWriteDeadline(deadline)
	assert.NoError(t, err)

	err = conn.SetDeadline(deadline)
	assert.NoError(t, err)
}

func TestConnectionAddresses(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)

	localAddr := conn.LocalAddr()
	assert.NotNil(t, localAddr)

	remoteAddr := conn.RemoteAddr()
	assert.NotNil(t, remoteAddr)
}

func TestConnectionMultiplePackets(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn)
	go func() {
		_, _ = io.Copy(io.Discard, clientConn)
	}()

	cases := []struct {
		desc string
		pkt  interface {
			Encode() []byte
			Pack(io.Writer) error
		}
	}{
		{
			desc: "connack packet",
			pkt: &v3.ConnAck{
				FixedHeader:    v3.FixedHeader{PacketType: v3.ConnAckType},
				SessionPresent: false,
				ReturnCode:     0,
			},
		},
		{
			desc: "puback packet",
			pkt: &v3.PubAck{
				FixedHeader: v3.FixedHeader{PacketType: v3.PubAckType},
				ID:          1,
			},
		},
		{
			desc: "pingresp packet",
			pkt: &v3.PingResp{
				FixedHeader: v3.FixedHeader{PacketType: v3.PingRespType},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.pkt.Pack(conn)
			assert.NoError(t, err)
		})
	}
}
