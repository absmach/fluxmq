// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqpbroker

import (
	"net"
	"testing"
	"time"

	amqpconn "github.com/absmach/fluxmq/amqp"
	"github.com/absmach/fluxmq/amqp/frames"
	"github.com/absmach/fluxmq/amqp/message"
	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/amqp/sasl"
	"github.com/absmach/fluxmq/amqp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBrokerAndPipe(t *testing.T) (*Broker, *amqpconn.Connection) {
	t.Helper()
	b := New(nil, nil)
	serverConn, clientConn := net.Pipe()

	go b.HandleConnection(serverConn)

	c := amqpconn.NewConnection(clientConn)
	return b, c
}

// doAMQPHandshake performs SASL + OPEN handshake and returns.
func doAMQPHandshake(t *testing.T, c *amqpconn.Connection, containerID string) {
	t.Helper()

	// Send SASL header
	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDSASL))

	// Read SASL header
	protoID, err := c.ReadProtocolHeader()
	require.NoError(t, err)
	assert.Equal(t, frames.ProtoIDSASL, protoID)

	// Read SASL Mechanisms
	desc, val, err := c.ReadSASLFrame()
	require.NoError(t, err)
	assert.Equal(t, sasl.DescriptorMechanisms, desc)
	mechs := val.(*sasl.Mechanisms)
	assert.Contains(t, mechs.Mechanisms, types.Symbol("ANONYMOUS"))

	// Send SASL Init (ANONYMOUS)
	init := &sasl.Init{Mechanism: sasl.MechANONYMOUS}
	body, err := init.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WriteSASLFrame(body))

	// Read SASL Outcome
	desc, val, err = c.ReadSASLFrame()
	require.NoError(t, err)
	assert.Equal(t, sasl.DescriptorOutcome, desc)
	outcome := val.(*sasl.Outcome)
	assert.Equal(t, sasl.CodeOK, outcome.Code)

	// Send AMQP header
	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDAMQP))
	protoID, err = c.ReadProtocolHeader()
	require.NoError(t, err)
	assert.Equal(t, frames.ProtoIDAMQP, protoID)

	// Send OPEN
	open := &performatives.Open{
		ContainerID:  containerID,
		MaxFrameSize: 65536,
		ChannelMax:   255,
	}
	openBody, err := open.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, openBody))

	// Read server OPEN
	_, desc2, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorOpen, desc2)
	serverOpen := perf.(*performatives.Open)
	assert.Equal(t, "fluxmq", serverOpen.ContainerID)
}

func doBeginSession(t *testing.T, c *amqpconn.Connection, channel uint16) {
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

	// Read Begin response
	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorBegin, desc)
	resp := perf.(*performatives.Begin)
	require.NotNil(t, resp.RemoteChannel)
	assert.Equal(t, channel, *resp.RemoteChannel)
}

func TestConnectionLifecycle(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "test-client")
	doBeginSession(t, c, 0)

	// End session
	end := &performatives.End{}
	body, err := end.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read End response
	_, desc, _, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorEnd, desc)

	// Close connection
	cl := &performatives.Close{}
	body, err = cl.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Close response
	_, desc, _, _, err = c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorClose, desc)
}

func TestAttachDetach(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "attach-client")
	doBeginSession(t, c, 0)

	// Attach sender link
	attach := &performatives.Attach{
		Name:   "sender-link",
		Handle: 0,
		Role:   performatives.RoleSender,
		Source: &performatives.Source{Address: "test/topic"},
		Target: &performatives.Target{Address: "test/topic"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Attach response
	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)
	resp := perf.(*performatives.Attach)
	assert.Equal(t, "sender-link", resp.Name)
	assert.Equal(t, performatives.RoleReceiver, resp.Role) // mirrored

	// Detach
	detach := &performatives.Detach{Handle: 0, Closed: true}
	body, err = detach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Detach response
	_, desc, _, _, err = c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorDetach, desc)
}

func TestPubSubFlow(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "pubsub-client")
	doBeginSession(t, c, 0)

	// Attach as receiver (broker sends to us)
	attach := &performatives.Attach{
		Name:   "recv-link",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/pubsub"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Attach response
	_, desc, _, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Issue credit via Flow
	h := uint32(0)
	dc := uint32(0)
	lc := uint32(10)
	flow := &performatives.Flow{
		IncomingWindow: 65535,
		NextOutgoingID: 0,
		OutgoingWindow: 65535,
		Handle:         &h,
		DeliveryCount:  &dc,
		LinkCredit:     &lc,
	}
	body, err = flow.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Give the broker a moment to process the flow
	time.Sleep(50 * time.Millisecond)

	// Publish from a goroutine since WriteTransfer blocks on net.Pipe until the client reads
	go b.Publish("test/pubsub", []byte("hello amqp"), nil)

	// Read the Transfer
	_, desc, perf, payload, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorTransfer, desc)
	transfer := perf.(*performatives.Transfer)
	assert.Equal(t, uint32(0), transfer.Handle)

	// Decode the AMQP message from the transfer payload
	msg, err := message.Decode(payload)
	require.NoError(t, err)
	require.Len(t, msg.Data, 1)
	assert.Equal(t, []byte("hello amqp"), msg.Data[0])
}

func TestTransferFromClient(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "sender-client")
	doBeginSession(t, c, 0)

	// Attach as sender (broker receives from us)
	attach := &performatives.Attach{
		Name:   "send-link",
		Handle: 0,
		Role:   performatives.RoleSender,
		Target: &performatives.Target{Address: "test/ingest"},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	_, desc, _, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Send a pre-settled transfer
	msg := &message.Message{
		Properties: &message.Properties{To: "test/ingest"},
		Data:       [][]byte{[]byte("test payload")},
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
	perfBody, err := transfer.Encode()
	require.NoError(t, err)

	// Write transfer with payload
	require.NoError(t, c.WriteTransfer(0, perfBody, msgBytes))

	// Give time for processing
	time.Sleep(50 * time.Millisecond)
}

func TestPrefixedClientID(t *testing.T) {
	assert.Equal(t, "amqp:test", PrefixedClientID("test"))
	assert.True(t, IsAMQPClient("amqp:test"))
	assert.False(t, IsAMQPClient("mqtt-client"))
}
