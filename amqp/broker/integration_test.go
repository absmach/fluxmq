// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	amqpconn "github.com/absmach/fluxmq/amqp"
	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	"github.com/absmach/fluxmq/amqp/frames"
	"github.com/absmach/fluxmq/amqp/message"
	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/amqp/sasl"
	"github.com/absmach/fluxmq/amqp/types"
	"github.com/absmach/fluxmq/broker"
	amqpserver "github.com/absmach/fluxmq/server/amqp"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startServer(t *testing.T) (*amqpbroker.Broker, string) {
	t.Helper()
	b := amqpbroker.New(nil, nil)
	cfg := amqpserver.Config{
		Address:         "127.0.0.1:0",
		ShutdownTimeout: 5 * time.Second,
	}
	srv := amqpserver.New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		srv.Listen(ctx)
	}()

	var addr net.Addr
	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		addr = srv.Addr()
		if addr != nil {
			break
		}
	}
	require.NotNil(t, addr, "server did not start")
	return b, addr.String()
}

func handshake(t *testing.T, c *amqpconn.Connection, containerID string) {
	t.Helper()

	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDSASL))
	_, err := c.ReadProtocolHeader()
	require.NoError(t, err)

	_, _, err = c.ReadSASLFrame()
	require.NoError(t, err)

	init := &sasl.Init{Mechanism: sasl.MechANONYMOUS}
	body, err := init.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WriteSASLFrame(body))

	_, _, err = c.ReadSASLFrame()
	require.NoError(t, err)

	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDAMQP))
	_, err = c.ReadProtocolHeader()
	require.NoError(t, err)

	open := &performatives.Open{
		ContainerID:  containerID,
		MaxFrameSize: 65536,
		ChannelMax:   255,
	}
	body, err = open.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	_, desc, _, _, err := c.ReadPerformative()
	require.NoError(t, err)
	require.Equal(t, performatives.DescriptorOpen, desc)
}

func beginSession(t *testing.T, c *amqpconn.Connection, channel uint16) {
	t.Helper()
	begin := &performatives.Begin{
		NextOutgoingID: 0,
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		HandleMax:      255,
	}
	body, err := begin.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(channel, body))

	_, desc, _, _, err := c.ReadPerformative()
	require.NoError(t, err)
	require.Equal(t, performatives.DescriptorBegin, desc)
}

func dialAndHandshake(t *testing.T, addr, containerID string) *amqpconn.Connection {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	c := amqpconn.NewConnection(conn)
	handshake(t, c, containerID)
	return c
}

func grantCredit(t *testing.T, c *amqpconn.Connection, channel uint16, handle, credit uint32) {
	t.Helper()
	dc := uint32(0)
	flow := &performatives.Flow{
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		Handle:         &handle,
		DeliveryCount:  &dc,
		LinkCredit:     &credit,
	}
	body, err := flow.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(channel, body))
}

func TestIntegrationPubSub(t *testing.T) {
	_, addr := startServer(t)

	// Publisher
	pub := dialAndHandshake(t, addr, "pub-client")
	beginSession(t, pub, 0)

	senderAttach := &performatives.Attach{
		Name:                 "sender",
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: "test/integration"},
		InitialDeliveryCount: 0,
	}
	body, err := senderAttach.Encode()
	require.NoError(t, err)
	require.NoError(t, pub.WritePerformative(0, body))

	_, desc, _, _, err := pub.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Read Flow (credit grant)
	_, desc, _, _, err = pub.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	// Subscriber
	sub := dialAndHandshake(t, addr, "sub-client")
	beginSession(t, sub, 0)

	recvAttach := &performatives.Attach{
		Name:   "receiver",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/integration"},
	}
	body, err = recvAttach.Encode()
	require.NoError(t, err)
	require.NoError(t, sub.WritePerformative(0, body))

	_, desc, _, _, err = sub.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	grantCredit(t, sub, 0, 0, 10)
	time.Sleep(50 * time.Millisecond)

	// Publish
	msg := &message.Message{
		Properties: &message.Properties{To: "test/integration"},
		Data:       [][]byte{[]byte("integration test payload")},
	}
	msgBytes, err := msg.Encode()
	require.NoError(t, err)

	did := uint32(0)
	mf := uint32(0)
	transfer := &performatives.Transfer{
		Handle:        0,
		DeliveryID:    &did,
		DeliveryTag:   []byte{0x01},
		MessageFormat: &mf,
		Settled:       true,
	}
	require.NoError(t, pub.WriteTransfer(0, transfer, msgBytes))

	// Receive
	_, desc, perf, payload, err := sub.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorTransfer, desc)
	assert.Equal(t, uint32(0), perf.(*performatives.Transfer).Handle)

	recvMsg, err := message.Decode(payload)
	require.NoError(t, err)
	require.Len(t, recvMsg.Data, 1)
	assert.Equal(t, []byte("integration test payload"), recvMsg.Data[0])
}

func TestIntegrationMultipleSubscribers(t *testing.T) {
	b, addr := startServer(t)

	const numSubs = 3
	subs := make([]*amqpconn.Connection, numSubs)
	for i := 0; i < numSubs; i++ {
		sub := dialAndHandshake(t, addr, fmt.Sprintf("sub-%d", i))
		beginSession(t, sub, 0)

		attach := &performatives.Attach{
			Name:   "recv",
			Handle: 0,
			Role:   performatives.RoleReceiver,
			Source: &performatives.Source{Address: "fanout/topic"},
		}
		body, err := attach.Encode()
		require.NoError(t, err)
		require.NoError(t, sub.WritePerformative(0, body))

		_, desc, _, _, err := sub.ReadPerformative()
		require.NoError(t, err)
		assert.Equal(t, performatives.DescriptorAttach, desc)

		grantCredit(t, sub, 0, 0, 10)
		subs[i] = sub
	}

	time.Sleep(50 * time.Millisecond)

	go b.Publish("fanout/topic", []byte("fanout msg"), nil)

	var wg sync.WaitGroup
	for i, sub := range subs {
		wg.Add(1)
		go func(idx int, s *amqpconn.Connection) {
			defer wg.Done()
			_, desc, _, payload, err := s.ReadPerformative()
			assert.NoError(t, err)
			assert.Equal(t, performatives.DescriptorTransfer, desc)
			msg, err := message.Decode(payload)
			assert.NoError(t, err)
			if len(msg.Data) > 0 {
				assert.Equal(t, []byte("fanout msg"), msg.Data[0])
			}
		}(i, sub)
	}
	wg.Wait()
}

func TestIntegrationWildcardSubscription(t *testing.T) {
	b, addr := startServer(t)

	sub := dialAndHandshake(t, addr, "wildcard-sub")
	beginSession(t, sub, 0)

	attach := &performatives.Attach{
		Name:   "wildcard-recv",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "sensors/+/temp"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, sub.WritePerformative(0, body))

	_, desc, _, _, err := sub.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	grantCredit(t, sub, 0, 0, 10)
	time.Sleep(50 * time.Millisecond)

	go b.Publish("sensors/room1/temp", []byte("22.5"), nil)

	_, desc, _, payload, err := sub.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorTransfer, desc)

	msg, err := message.Decode(payload)
	require.NoError(t, err)
	require.Len(t, msg.Data, 1)
	assert.Equal(t, []byte("22.5"), msg.Data[0])
}

func TestIntegrationIdleTimeout(t *testing.T) {
	_, addr := startServer(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	c := amqpconn.NewConnection(conn)

	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDSASL))
	_, err = c.ReadProtocolHeader()
	require.NoError(t, err)

	_, _, err = c.ReadSASLFrame()
	require.NoError(t, err)

	init := &sasl.Init{Mechanism: sasl.MechANONYMOUS}
	body, _ := init.Encode()
	require.NoError(t, c.WriteSASLFrame(body))

	_, _, err = c.ReadSASLFrame()
	require.NoError(t, err)

	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDAMQP))
	_, err = c.ReadProtocolHeader()
	require.NoError(t, err)

	open := &performatives.Open{
		ContainerID:  "idle-test",
		MaxFrameSize: 65536,
		ChannelMax:   255,
		IdleTimeOut:  500,
	}
	body, _ = open.Encode()
	require.NoError(t, c.WritePerformative(0, body))

	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorOpen, desc)
	serverOpen := perf.(*performatives.Open)
	assert.Equal(t, uint32(500), serverOpen.IdleTimeOut)

	// Server should send heartbeat within ~250ms
	c.SetIdleTimeout(2 * time.Second)
	_, _, _, _, err = c.ReadPerformative()
	require.NoError(t, err) // should be a heartbeat or any frame — no error means server is alive
}

type mockAuth struct {
	acceptUser string
	acceptPass string
}

func (m *mockAuth) Authenticate(clientID, username, password string) (bool, error) {
	return username == m.acceptUser && password == m.acceptPass, nil
}

type mockAuthz struct{}

func (m *mockAuthz) CanPublish(clientID, topic string) bool   { return true }
func (m *mockAuthz) CanSubscribe(clientID, filter string) bool { return true }

func TestIntegrationSASLPlainAuth(t *testing.T) {
	b := amqpbroker.New(nil, nil)
	auth := broker.NewAuthEngine(&mockAuth{acceptUser: "alice", acceptPass: "secret"}, &mockAuthz{})
	b.SetAuthEngine(auth)

	cfg := amqpserver.Config{
		Address:         "127.0.0.1:0",
		ShutdownTimeout: 5 * time.Second,
	}
	srv := amqpserver.New(cfg, b)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.Listen(ctx)

	var addr net.Addr
	for range 50 {
		time.Sleep(10 * time.Millisecond)
		addr = srv.Addr()
		if addr != nil {
			break
		}
	}
	require.NotNil(t, addr)

	t.Run("accepted", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr.String(), 2*time.Second)
		require.NoError(t, err)
		defer conn.Close()

		c := amqpconn.NewConnection(conn)

		require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDSASL))
		_, err = c.ReadProtocolHeader()
		require.NoError(t, err)

		_, _, err = c.ReadSASLFrame() // mechanisms
		require.NoError(t, err)

		// Send PLAIN: \0alice\0secret
		initFrame := &sasl.Init{
			Mechanism:       sasl.MechPLAIN,
			InitialResponse: []byte("\x00alice\x00secret"),
		}
		body, err := initFrame.Encode()
		require.NoError(t, err)
		require.NoError(t, c.WriteSASLFrame(body))

		desc, val, err := c.ReadSASLFrame()
		require.NoError(t, err)
		assert.Equal(t, sasl.DescriptorOutcome, desc)
		outcome := val.(*sasl.Outcome)
		assert.Equal(t, sasl.CodeOK, outcome.Code)
	})

	t.Run("rejected", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr.String(), 2*time.Second)
		require.NoError(t, err)
		defer conn.Close()

		c := amqpconn.NewConnection(conn)

		require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDSASL))
		_, err = c.ReadProtocolHeader()
		require.NoError(t, err)

		_, val, err := c.ReadSASLFrame()
		require.NoError(t, err)
		mechs := val.(*sasl.Mechanisms)
		assert.Contains(t, mechs.Mechanisms, types.Symbol("PLAIN"))

		initFrame := &sasl.Init{
			Mechanism:       sasl.MechPLAIN,
			InitialResponse: []byte("\x00alice\x00wrongpass"),
		}
		body, err := initFrame.Encode()
		require.NoError(t, err)
		require.NoError(t, c.WriteSASLFrame(body))

		desc, val, err := c.ReadSASLFrame()
		require.NoError(t, err)
		assert.Equal(t, sasl.DescriptorOutcome, desc)
		outcome := val.(*sasl.Outcome)
		assert.Equal(t, sasl.CodeAuth, outcome.Code)
	})
}

func TestIntegrationDeliverToClient(t *testing.T) {
	b, addr := startServer(t)

	c := dialAndHandshake(t, addr, "deliver-client")
	beginSession(t, c, 0)

	// Attach as receiver
	attach := &performatives.Attach{
		Name:   "recv",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/deliver"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))
	_, _, _, _, err = c.ReadPerformative() // attach resp
	require.NoError(t, err)

	grantCredit(t, c, 0, 0, 10)
	time.Sleep(50 * time.Millisecond)

	// Deliver via DeliverToClient
	msg := &storage.Message{
		Topic:      "test/deliver",
		Payload:    []byte("deliver-payload"),
		Properties: map[string]string{"key": "val"},
		QoS:        0,
	}
	err = b.DeliverToClient(context.Background(), "amqp:deliver-client", msg)
	require.NoError(t, err)

	_, desc, _, payload, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorTransfer, desc)

	recvMsg, err := message.Decode(payload)
	require.NoError(t, err)
	require.Len(t, recvMsg.Data, 1)
	assert.Equal(t, []byte("deliver-payload"), recvMsg.Data[0])
}

func TestIntegrationGracefulClose(t *testing.T) {
	_, addr := startServer(t)

	c := dialAndHandshake(t, addr, "close-test")
	beginSession(t, c, 0)

	end := &performatives.End{}
	body, _ := end.Encode()
	require.NoError(t, c.WritePerformative(0, body))

	_, desc, _, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorEnd, desc)

	cl := &performatives.Close{}
	body, _ = cl.Encode()
	require.NoError(t, c.WritePerformative(0, body))

	_, desc, _, _, err = c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorClose, desc)
}
