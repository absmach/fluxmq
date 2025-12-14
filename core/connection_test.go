package core_test

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets/v3"
	v5 "github.com/dborovcanin/mqtt/core/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockTCPConnection(t *testing.T) (*net.TCPListener, *net.TCPConn, *net.TCPConn) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)

	done := make(chan struct{})
	var serverConn *net.TCPConn
	go func() {
		defer close(done)
		conn, err := listener.AcceptTCP()
		if err == nil {
			serverConn = conn
		}
	}()

	clientConn, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	require.NoError(t, err)

	<-done
	require.NotNil(t, serverConn)

	return listener, serverConn, clientConn
}

func TestNewConnection(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)
	require.NotNil(t, conn)
}

func TestConnectionReadPacketV3(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

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
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

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
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)

	connack := &v3.ConnAck{
		FixedHeader:    v3.FixedHeader{PacketType: v3.ConnAckType},
		SessionPresent: false,
		ReturnCode:     0,
	}

	err := conn.WritePacket(connack)
	require.NoError(t, err)

	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	require.NoError(t, err)
	require.Greater(t, n, 0)

	reader := bytes.NewReader(buf[:n])
	decoded, err := v3.ReadPacket(reader)
	require.NoError(t, err)

	receivedConnack, ok := decoded.(*v3.ConnAck)
	require.True(t, ok)
	assert.Equal(t, byte(0), receivedConnack.ReturnCode)
}

func TestConnectionSetKeepAlive(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)

	err := conn.SetKeepAlive(30 * time.Second)
	assert.NoError(t, err)
}

func TestConnectionTouch(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)

	conn.Touch()

	conn.Touch()
}

func TestConnectionOnDisconnect(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)

	called := false
	conn.SetOnDisconnect(func(g bool) {
		called = true
	})

	assert.False(t, called)
}

func TestConnectionReadWriteDeadline(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

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
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)

	localAddr := conn.LocalAddr()
	assert.NotNil(t, localAddr)

	remoteAddr := conn.RemoteAddr()
	assert.NotNil(t, remoteAddr)
}

func TestConnectionMultiplePackets(t *testing.T) {
	listener, serverConn, clientConn := createMockTCPConnection(t)
	defer listener.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := core.NewConnection(serverConn)

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
