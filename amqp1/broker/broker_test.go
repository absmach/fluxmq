// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	amqpconn "github.com/absmach/fluxmq/amqp1"
	"github.com/absmach/fluxmq/amqp1/frames"
	"github.com/absmach/fluxmq/amqp1/message"
	"github.com/absmach/fluxmq/amqp1/performatives"
	"github.com/absmach/fluxmq/amqp1/sasl"
	"github.com/absmach/fluxmq/amqp1/types"
	corebroker "github.com/absmach/fluxmq/broker"
	coremessage "github.com/absmach/fluxmq/message"
	queuepkg "github.com/absmach/fluxmq/queue"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTopic    = "test/topic"
	testRecvLink = "recv-link"
	testSendLink = "send-link"
)

func setupBrokerAndPipe(t *testing.T) (*Broker, *amqpconn.Connection) {
	t.Helper()
	b := New(nil, nil, nil)
	serverConn, clientConn := net.Pipe()

	go b.HandleConnection(context.Background(), serverConn)

	c := amqpconn.NewConnection(clientConn)
	return b, c
}

type mockAMQP1QueueLinkManager struct {
	publishCh  chan qtypes.PublishRequest
	publishErr error
}

func (m *mockAMQP1QueueLinkManager) Publish(_ context.Context, publish qtypes.PublishRequest) error {
	if m.publishCh != nil {
		cloned := publish
		if len(publish.Properties) > 0 {
			cloned.Properties = make(map[string]string, len(publish.Properties))
			for k, v := range publish.Properties {
				cloned.Properties[k] = v
			}
		}
		m.publishCh <- cloned
	}
	return m.publishErr
}

func (m *mockAMQP1QueueLinkManager) Subscribe(context.Context, string, string, string, string, string) error {
	return nil
}

func (m *mockAMQP1QueueLinkManager) SubscribeWithCursor(context.Context, string, string, string, string, string, *qtypes.CursorOption) error {
	return nil
}

func (m *mockAMQP1QueueLinkManager) Unsubscribe(context.Context, string, string, string, string) error {
	return nil
}

func (m *mockAMQP1QueueLinkManager) Ack(context.Context, string, string, string) error {
	return nil
}

func (m *mockAMQP1QueueLinkManager) Nack(context.Context, string, string, string) error {
	return nil
}

func (m *mockAMQP1QueueLinkManager) Reject(context.Context, string, string, string, string) error {
	return nil
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorEnd, desc)

	// Close connection
	cl := &performatives.Close{}
	body, err = cl.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Close response
	desc, err = readDescriptor(t, c)
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
		Source: &performatives.Source{Address: testTopic},
		Target: &performatives.Target{Address: testTopic},
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

	// Read Flow (credit grant for sender link)
	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	// Detach
	detach := &performatives.Detach{Handle: 0, Closed: true}
	body, err = detach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Detach response
	desc, err = readDescriptor(t, c)
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
		Name:   testRecvLink,
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/pubsub"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read Attach response
	desc, err := readDescriptor(t, c)
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
	go b.Publish(context.Background(), "test/pubsub", []byte("hello amqp"), nil)

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
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: "test/ingest"},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Read Flow (credit grant for sender link)
	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

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
	// Write transfer with payload
	require.NoError(t, c.WriteTransfer(0, transfer, msgBytes))

	// Give time for processing
	time.Sleep(50 * time.Millisecond)
}

func TestTransferFromClientClientIDProperty(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	if err := b.router.Subscribe("mqtt-client", "test/ingest", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	propsCh := make(chan map[string]string, 1)
	b.SetCrossDeliver(func(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		propsCh <- cloneStringMap(props)
	})

	doAMQPHandshake(t, c, "sender-client")
	doBeginSession(t, c, 0)

	attach := &performatives.Attach{
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: testIngestAddress},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	msg := &message.Message{
		Properties:            &message.Properties{To: testIngestAddress},
		ApplicationProperties: map[string]any{testTraceKey: testTraceValue},
		Data:                  [][]byte{[]byte("payload")},
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
	require.NoError(t, c.WriteTransfer(0, transfer, msgBytes))

	select {
	case gotProps := <-propsCh:
		require.Equal(t, testTraceValue, gotProps[testTraceKey])
		require.Equal(t, PrefixedClientID("sender-client"), gotProps[coremessage.PropertyClientID])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cross-deliver callback")
	}
}

// AMQP 1.0 has no trusted listener, so every connection is a remote client:
// reserved properties are dropped on ingress and cannot be forged.
func TestTransferFromClientDropsReservedProperties(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	reserved := coremessage.ReservedPropertyPrefix + "re.trace"

	require.NoError(t, b.router.Subscribe("mqtt-client", "test/ingest", 1, storage.SubscribeOptions{}))

	propsCh := make(chan map[string]string, 1)
	b.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, props map[string]string) {
		propsCh <- cloneStringMap(props)
	})

	doAMQPHandshake(t, c, "sender-client")
	doBeginSession(t, c, 0)

	attach := &performatives.Attach{
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: testIngestAddress},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	msg := &message.Message{
		Properties:            &message.Properties{To: testIngestAddress},
		ApplicationProperties: map[string]any{reserved: testRuleTrace, testTraceKey: testTraceValue},
		Data:                  [][]byte{[]byte("payload")},
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
	require.NoError(t, c.WriteTransfer(0, transfer, msgBytes))

	select {
	case gotProps := <-propsCh:
		assert.NotContains(t, gotProps, reserved, "reserved property must not be forgeable by a remote sender")
		assert.Equal(t, testTraceValue, gotProps[testTraceKey])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cross-deliver callback")
	}
}

// Identity and origin protocol come from the authenticated connection. AMQP 1.0
// has no trusted listener, so a sender may not attribute its publication to
// another principal or claim it arrived over another protocol.
func TestTransferFromClientRejectsForgedIdentity(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	require.NoError(t, b.router.Subscribe("mqtt-client", "test/ingest", 1, storage.SubscribeOptions{}))

	propsCh := make(chan map[string]string, 1)
	b.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, props map[string]string) {
		propsCh <- cloneStringMap(props)
	})

	doAMQPHandshake(t, c, "sender-client")
	doBeginSession(t, c, 0)

	attach := &performatives.Attach{
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: testIngestAddress},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	msg := &message.Message{
		Properties: &message.Properties{To: testIngestAddress},
		ApplicationProperties: map[string]any{
			coremessage.PropertyExternalID: "victim-tenant",
			coremessage.PropertyProtocol:   string(coremessage.ProtocolHTTP),
			coremessage.PropertyClientID:   "someone-else",
		},
		Data: [][]byte{[]byte("payload")},
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
	require.NoError(t, c.WriteTransfer(0, transfer, msgBytes))

	select {
	case gotProps := <-propsCh:
		// No auth engine is configured, so no external identity resolves and the
		// forged one must be discarded rather than left in place.
		assert.NotContains(t, gotProps, coremessage.PropertyExternalID)
		assert.Equal(t, string(coremessage.ProtocolAMQP1), gotProps[coremessage.PropertyProtocol])
		assert.Equal(t, PrefixedClientID("sender-client"), gotProps[coremessage.PropertyClientID])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cross-deliver callback")
	}
}

// Pub/sub egress mirrors ingress: a remote receiver never observes
// broker-internal state, while ordinary properties are delivered.
func TestPublishOmitsReservedPropertiesFromDelivery(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	reserved := coremessage.ReservedPropertyPrefix + "re.trace"

	doAMQPHandshake(t, c, "pubsub-client")
	doBeginSession(t, c, 0)

	attachReceiver(t, b, c, "pubsub-client", "test/pubsub")

	props := map[string]string{reserved: testRuleTrace, testTraceKey: testTraceValue}
	go b.Publish(context.Background(), "test/pubsub", []byte("hello amqp"), props)

	msg := readDeliveredMessage(t, c)
	assert.NotContains(t, msg.ApplicationProperties, reserved)
	assert.Equal(t, testTraceValue, msg.ApplicationProperties[testTraceKey])
}

// Queue delivery is a second egress path and must apply the same boundary.
func TestDeliverToClientOmitsReservedProperties(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	reserved := coremessage.ReservedPropertyPrefix + "re.trace"

	doAMQPHandshake(t, c, "queue-client")
	doBeginSession(t, c, 0)

	attachReceiver(t, b, c, "queue-client", "test/pubsub")

	stored := coremessage.New("test/pubsub", []byte("hello queue"))
	stored.User.Properties = map[string]string{reserved: testRuleTrace, testTraceKey: testTraceValue}
	go b.DeliverToClient(context.Background(), corebroker.AMQP1ClientPrefix+"queue-client", stored) //nolint:errcheck // delivery error surfaces as a read timeout below

	msg := readDeliveredMessage(t, c)
	assert.NotContains(t, msg.ApplicationProperties, reserved)
	assert.Equal(t, testTraceValue, msg.ApplicationProperties[testTraceKey])
}

// attachReceiver attaches a receiver link on handle 0 for address and grants it
// credit, returning once the broker has applied the credit so the caller can
// publish without racing the Flow.
func attachReceiver(t *testing.T, b *Broker, c *amqpconn.Connection, containerID, address string) {
	t.Helper()

	attach := &performatives.Attach{
		Name:   testRecvLink,
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: address},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	require.Equal(t, performatives.DescriptorAttach, desc)

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

	require.Eventually(t, func() bool {
		return senderLinkCredit(b, containerID, 0, 0) > 0
	}, 2*time.Second, time.Millisecond, "broker did not apply link credit")
}

// senderLinkCredit reads the broker-side credit of a sender link, so tests can
// wait for a Flow to be applied instead of sleeping.
func senderLinkCredit(b *Broker, containerID string, channel uint16, handle uint32) uint32 {
	val, ok := b.connections.Load(containerID)
	if !ok {
		return 0
	}
	conn := val.(*Connection)

	conn.sessionsMu.RLock()
	sess, ok := conn.sessions[channel]
	conn.sessionsMu.RUnlock()
	if !ok {
		return 0
	}

	sess.linksMu.RLock()
	link, ok := sess.links[handle]
	sess.linksMu.RUnlock()
	if !ok {
		return 0
	}

	link.mu.Lock()
	defer link.mu.Unlock()
	return link.credit
}

// readDeliveredMessage reads one Transfer and decodes its message body.
func readDeliveredMessage(t *testing.T, c *amqpconn.Connection) *message.Message {
	t.Helper()

	_, desc, _, payload, err := c.ReadPerformative()
	require.NoError(t, err)
	require.Equal(t, performatives.DescriptorTransfer, desc)

	msg, err := message.Decode(payload)
	require.NoError(t, err)
	return msg
}

func TestQueueTransferCarriesClientID(t *testing.T) {
	mockQM := &mockAMQP1QueueLinkManager{publishCh: make(chan qtypes.PublishRequest, 1)}
	b := New(nil, nil, nil)
	b.queueLinkManager = mockQM
	serverConn, clientConn := net.Pipe()

	go b.HandleConnection(context.Background(), serverConn)

	c := amqpconn.NewConnection(clientConn)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "sender-client")
	doBeginSession(t, c, 0)

	attach := &performatives.Attach{
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: "$queue/orders/process"},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	msg := &message.Message{
		Properties:            &message.Properties{To: "$queue/orders/process"},
		ApplicationProperties: map[string]any{testTraceKey: testTraceValue},
		Data:                  [][]byte{[]byte("payload")},
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
	require.NoError(t, c.WriteTransfer(0, transfer, msgBytes))

	select {
	case publish := <-mockQM.publishCh:
		require.Equal(t, PrefixedClientID("sender-client"), publish.Source.ClientID)
		require.Equal(t, coremessage.ProtocolAMQP1, publish.Source.Protocol)
		require.Equal(t, testTraceValue, publish.Properties[testTraceKey])
		require.NotContains(t, publish.Properties, coremessage.PropertyClientID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue publish")
	}
}

// doAMQPHandshakeWithFrameSize performs handshake with a custom max frame size.
func doAMQPHandshakeWithFrameSize(t *testing.T, c *amqpconn.Connection, containerID string, maxFrameSize uint32) {
	t.Helper()

	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDSASL))
	protoID, err := c.ReadProtocolHeader()
	require.NoError(t, err)
	assert.Equal(t, frames.ProtoIDSASL, protoID)

	desc, val, err := c.ReadSASLFrame()
	require.NoError(t, err)
	assert.Equal(t, sasl.DescriptorMechanisms, desc)
	mechs := val.(*sasl.Mechanisms)
	assert.Contains(t, mechs.Mechanisms, types.Symbol("ANONYMOUS"))

	init := &sasl.Init{Mechanism: sasl.MechANONYMOUS}
	body, err := init.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WriteSASLFrame(body))

	desc, val, err = c.ReadSASLFrame()
	require.NoError(t, err)
	assert.Equal(t, sasl.DescriptorOutcome, desc)
	outcome := val.(*sasl.Outcome)
	assert.Equal(t, sasl.CodeOK, outcome.Code)

	require.NoError(t, c.WriteProtocolHeader(frames.ProtoIDAMQP))
	protoID, err = c.ReadProtocolHeader()
	require.NoError(t, err)
	assert.Equal(t, frames.ProtoIDAMQP, protoID)

	open := &performatives.Open{
		ContainerID:  containerID,
		MaxFrameSize: maxFrameSize,
		ChannelMax:   255,
	}
	openBody, err := open.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, openBody))

	_, desc2, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorOpen, desc2)
	serverOpen := perf.(*performatives.Open)
	assert.Equal(t, "fluxmq", serverOpen.ContainerID)
	assert.LessOrEqual(t, serverOpen.MaxFrameSize, maxFrameSize)
}

func TestMultiFrameTransferSend(t *testing.T) {
	b := New(nil, nil, nil)
	serverConn, clientConn := net.Pipe()

	go b.HandleConnection(context.Background(), serverConn)

	c := amqpconn.NewConnection(clientConn)
	c.SetMaxFrameSize(512)
	defer b.Close()
	defer c.Close()

	// Negotiate small frame size (512 bytes)
	doAMQPHandshakeWithFrameSize(t, c, "multiframe-send", 512)
	doBeginSession(t, c, 0)

	// Attach as receiver
	attach := &performatives.Attach{
		Name:   testRecvLink,
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/big"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Issue credit
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

	time.Sleep(50 * time.Millisecond)

	// Publish a large message (larger than 512 byte frame size)
	bigPayload := make([]byte, 2000)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 256)
	}

	go b.Publish(context.Background(), "test/big", bigPayload, nil)

	// Read frames — may be multiple with More=true
	var reassembled []byte
	var firstTransfer *performatives.Transfer
	for {
		_, desc, perf, payload, err := c.ReadPerformative()
		require.NoError(t, err)
		assert.Equal(t, performatives.DescriptorTransfer, desc)
		transfer := perf.(*performatives.Transfer)
		if firstTransfer == nil {
			firstTransfer = transfer
		}
		reassembled = append(reassembled, payload...)
		if !transfer.More {
			break
		}
	}

	// Decode the reassembled AMQP message
	msg, err := message.Decode(reassembled)
	require.NoError(t, err)
	require.Len(t, msg.Data, 1)
	assert.Equal(t, bigPayload, msg.Data[0])
}

func TestMultiFrameTransferReceive(t *testing.T) {
	b := New(nil, nil, nil)
	serverConn, clientConn := net.Pipe()

	go b.HandleConnection(context.Background(), serverConn)

	c := amqpconn.NewConnection(clientConn)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "multiframe-recv")
	doBeginSession(t, c, 0)

	// Attach as sender
	attach := &performatives.Attach{
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: "test/bigingest"},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Read Flow (credit grant)
	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	// Build a large message
	bigPayload := make([]byte, 2000)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 256)
	}
	msg := &message.Message{
		Properties: &message.Properties{To: "test/bigingest"},
		Data:       [][]byte{bigPayload},
	}
	msgBytes, err := msg.Encode()
	require.NoError(t, err)

	// Manually split into multi-frame transfer
	did := uint32(0)
	mf := uint32(0)

	// First frame: transfer with More=true + first chunk
	chunkSize := 200
	firstTransfer := &performatives.Transfer{
		Handle:        0,
		DeliveryID:    &did,
		DeliveryTag:   []byte{0x01},
		MessageFormat: &mf,
		Settled:       true,
		More:          true,
	}
	require.NoError(t, c.WriteTransferRaw(0, mustEncode(t, firstTransfer), msgBytes[:chunkSize]))

	// Middle frames
	offset := chunkSize
	for offset+chunkSize < len(msgBytes) {
		cont := &performatives.Transfer{Handle: 0, More: true}
		require.NoError(t, c.WriteTransferRaw(0, mustEncode(t, cont), msgBytes[offset:offset+chunkSize]))
		offset += chunkSize
	}

	// Last frame
	last := &performatives.Transfer{Handle: 0, More: false}
	require.NoError(t, c.WriteTransferRaw(0, mustEncode(t, last), msgBytes[offset:]))

	// Give time for processing and reassembly
	time.Sleep(100 * time.Millisecond)
}

func mustEncode(t *testing.T, p interface{ Encode() ([]byte, error) }) []byte {
	t.Helper()
	b, err := p.Encode()
	require.NoError(t, err)
	return b
}

func TestHandleExceedsMax(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "handle-exceed")
	doBeginSession(t, c, 0)

	// Attach with handle > 255 (handleMax)
	attach := &performatives.Attach{
		Name:   "bad-link",
		Handle: 999,
		Role:   performatives.RoleSender,
		Target: &performatives.Target{Address: testTopic},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Should get a Detach with error (not an Attach response)
	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorDetach, desc)
	detach := perf.(*performatives.Detach)
	assert.True(t, detach.Closed)
	assert.NotNil(t, detach.Error)
	assert.Equal(t, performatives.ErrNotAllowed, detach.Error.Condition)
}

func TestLinkSteal(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "link-steal")
	doBeginSession(t, c, 0)

	// Attach first link on handle 0
	attach1 := &performatives.Attach{
		Name:   "link-1",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/a"},
	}
	body, err := attach1.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Attach second link on same handle 0 (link steal)
	attach2 := &performatives.Attach{
		Name:   "link-2",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/b"},
	}
	body, err = attach2.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Should get Attach response for the new link
	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)
	resp := perf.(*performatives.Attach)
	assert.Equal(t, "link-2", resp.Name)
}

func TestDuplicateChannel(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "dup-channel")
	doBeginSession(t, c, 0)

	// Try to begin another session on same channel 0
	begin := &performatives.Begin{
		NextOutgoingID: 0,
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		HandleMax:      255,
	}
	body, err := begin.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Should get Close with framing error
	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorClose, desc)
	cl := perf.(*performatives.Close)
	require.NotNil(t, cl.Error)
	assert.Equal(t, performatives.ErrFramingError, cl.Error.Condition)
}

func TestPrefixedClientID(t *testing.T) {
	assert.Equal(t, "amqp:test", PrefixedClientID("test"))
	assert.True(t, IsAMQPClient("amqp:test"))
	assert.False(t, IsAMQPClient("mqtt-client"))
}

func TestUnsettledTransferDisposition(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "unsettled-client")
	doBeginSession(t, c, 0)

	// Attach as sender (broker receives)
	attach := &performatives.Attach{
		Name:                 "sender",
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: "test/unsettled"},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Read Flow (credit grant)
	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorFlow, desc)

	// Send an unsettled transfer
	msg := &message.Message{
		Properties: &message.Properties{To: "test/unsettled"},
		Data:       [][]byte{[]byte("unsettled payload")},
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
		Settled:       false, // unsettled
	}
	require.NoError(t, c.WriteTransfer(0, transfer, msgBytes))

	// Should receive Disposition(Accepted, Settled=true) from broker
	_, desc, perf, _, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorDisposition, desc)
	disp := perf.(*performatives.Disposition)
	assert.True(t, disp.Settled)
	assert.Equal(t, uint32(0), disp.First)
	_, isAccepted := disp.State.(*performatives.Accepted)
	assert.True(t, isAccepted, "expected Accepted state")
}

func TestUnsettledQueueTransferFailureIsRejected(t *testing.T) {
	failure := queuepkg.Failure{
		Code:       queuepkg.ErrorCodeUnavailable,
		Retryable:  true,
		Leader:     queuepkg.LeaderNotLocal,
		Durability: queuepkg.DurabilityNotAttempted,
	}
	b := New(nil, nil, nil)
	b.queueLinkManager = &mockAMQP1QueueLinkManager{
		publishErr: queuepkg.WithFailure(fmt.Errorf("backend detail"), failure),
	}
	serverConn, clientConn := net.Pipe()
	go b.HandleConnection(context.Background(), serverConn)
	c := amqpconn.NewConnection(clientConn)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "rejected-queue-client")
	doBeginSession(t, c, 0)

	const queueAddress = "$queue/orders/process"
	attach := &performatives.Attach{
		Name:                 testSendLink,
		Handle:               0,
		Role:                 performatives.RoleSender,
		Target:               &performatives.Target{Address: queueAddress},
		InitialDeliveryCount: 0,
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))
	_, err = readDescriptor(t, c)
	require.NoError(t, err)
	_, err = readDescriptor(t, c)
	require.NoError(t, err)

	msgBytes, err := (&message.Message{
		Properties: &message.Properties{To: queueAddress},
		Data:       [][]byte{[]byte("payload")},
	}).Encode()
	require.NoError(t, err)
	deliveryID := uint32(0)
	messageFormat := uint32(0)
	require.NoError(t, c.WriteTransfer(0, &performatives.Transfer{
		Handle:        0,
		DeliveryID:    &deliveryID,
		DeliveryTag:   []byte{0x01},
		MessageFormat: &messageFormat,
		Settled:       false,
	}, msgBytes))

	_, descriptor, value, _, err := c.ReadPerformative()
	require.NoError(t, err)
	require.Equal(t, performatives.DescriptorDisposition, descriptor)
	disposition := value.(*performatives.Disposition)
	rejected, ok := disposition.State.(*performatives.Rejected)
	require.True(t, ok)
	require.Equal(t, performatives.ErrInternalError, rejected.Error.Condition)
	require.Equal(t, string(failure.Code), rejected.Error.Info[amqp1QueueErrorCodeKey])
	require.Equal(t, failure.Retryable, rejected.Error.Info[amqp1RetryableKey])
	require.Equal(t, string(failure.Leader), rejected.Error.Info[amqp1LeaderStateKey])
	require.Equal(t, string(failure.Durability), rejected.Error.Info[amqp1DurabilityStateKey])
}

func TestCreditExhaustion(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "credit-client")
	doBeginSession(t, c, 0)

	// Attach as receiver (broker sends to us)
	attach := &performatives.Attach{
		Name:   testRecvLink,
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/credit"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Grant only 2 credits
	h := uint32(0)
	dc := uint32(0)
	lc := uint32(2)
	flow := &performatives.Flow{
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		Handle:         &h,
		DeliveryCount:  &dc,
		LinkCredit:     &lc,
	}
	body, err = flow.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	time.Sleep(50 * time.Millisecond)

	// Publish and read 2 messages (using goroutines since net.Pipe blocks)
	for i := range 2 {
		go b.Publish(context.Background(), "test/credit", []byte(fmt.Sprintf("msg-%d", i)), nil)
		desc, err := readDescriptor(t, c)
		require.NoError(t, err)
		assert.Equal(t, performatives.DescriptorTransfer, desc)
	}

	// 3rd publish should be silently dropped (no credit left)
	b.Publish(context.Background(), "test/credit", []byte("msg-dropped"), nil)

	// Re-grant credit
	lc2 := uint32(1)
	dc2 := uint32(2)
	flow2 := &performatives.Flow{
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		Handle:         &h,
		DeliveryCount:  &dc2,
		LinkCredit:     &lc2,
	}
	body, err = flow2.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	time.Sleep(50 * time.Millisecond)

	// New message should arrive with new credit
	go b.Publish(context.Background(), "test/credit", []byte("after-credit"), nil)

	_, desc, _, payload, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorTransfer, desc)
	msg, err := message.Decode(payload)
	require.NoError(t, err)
	require.Len(t, msg.Data, 1)
	assert.Equal(t, []byte("after-credit"), msg.Data[0])
}

func TestMultipleSessions(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "multi-session")

	// Begin two sessions on different channels
	doBeginSession(t, c, 0)
	doBeginSession(t, c, 1)

	// Attach receiver on session 0
	attach0 := &performatives.Attach{
		Name:   "recv-s0",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/s0"},
	}
	body, err := attach0.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))
	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Attach receiver on session 1
	attach1 := &performatives.Attach{
		Name:   "recv-s1",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/s1"},
	}
	body, err = attach1.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(1, body))
	desc, err = readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorAttach, desc)

	// Grant credit on both
	h := uint32(0)
	dc := uint32(0)
	lc := uint32(5)
	for _, ch := range []uint16{0, 1} {
		flow := &performatives.Flow{
			IncomingWindow: 65535,
			OutgoingWindow: 65535,
			Handle:         &h,
			DeliveryCount:  &dc,
			LinkCredit:     &lc,
		}
		body, err = flow.Encode()
		require.NoError(t, err)
		require.NoError(t, c.WritePerformative(ch, body))
	}

	time.Sleep(50 * time.Millisecond)

	// Publish to s0 topic only
	go b.Publish(context.Background(), "test/s0", []byte("for-s0"), nil)

	_, desc, perf, payload, err := c.ReadPerformative()
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorTransfer, desc)
	_ = perf
	msg, err := message.Decode(payload)
	require.NoError(t, err)
	require.Len(t, msg.Data, 1)
	assert.Equal(t, []byte("for-s0"), msg.Data[0])
}

func TestConnectionCloseCleanup(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()

	doAMQPHandshake(t, c, "cleanup-client")
	doBeginSession(t, c, 0)

	// Attach a receiver link so the router has a subscription
	attach := &performatives.Attach{
		Name:   "recv",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/cleanup"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))
	require.NoError(t, drainPerformative(t, c)) // attach resp

	// Verify connection is registered
	_, ok := b.connections.Load("cleanup-client")
	assert.True(t, ok)

	// Close the connection
	cl := &performatives.Close{}
	body, err = cl.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))

	// Read close response
	desc, err := readDescriptor(t, c)
	require.NoError(t, err)
	assert.Equal(t, performatives.DescriptorClose, desc)
	c.Close()

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Connection should be unregistered
	_, ok = b.connections.Load("cleanup-client")
	assert.False(t, ok, "connection should be unregistered after close")
}

func TestBrokerClose(t *testing.T) {
	b := New(nil, nil, nil)
	serverConn1, clientConn1 := net.Pipe()
	serverConn2, clientConn2 := net.Pipe()

	go b.HandleConnection(context.Background(), serverConn1)
	go b.HandleConnection(context.Background(), serverConn2)

	c1 := amqpconn.NewConnection(clientConn1)
	c2 := amqpconn.NewConnection(clientConn2)

	doAMQPHandshake(t, c1, "close-client-1")
	doAMQPHandshake(t, c2, "close-client-2")

	// Wait for server-side registration (slight race after Open response)
	time.Sleep(50 * time.Millisecond)

	// Verify both registered
	_, ok := b.connections.Load("close-client-1")
	assert.True(t, ok)
	_, ok = b.connections.Load("close-client-2")
	assert.True(t, ok)

	// Close broker
	b.Close()
	time.Sleep(100 * time.Millisecond)

	c1.Close()
	c2.Close()
}

func TestConcurrentPublishAndFlow(t *testing.T) {
	b, c := setupBrokerAndPipe(t)
	defer b.Close()
	defer c.Close()

	doAMQPHandshake(t, c, "race-client")
	doBeginSession(t, c, 0)

	// Attach receiver
	attach := &performatives.Attach{
		Name:   "recv",
		Handle: 0,
		Role:   performatives.RoleReceiver,
		Source: &performatives.Source{Address: "test/race"},
	}
	body, err := attach.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))
	require.NoError(t, drainPerformative(t, c)) // attach resp

	// Grant initial credit
	h := uint32(0)
	dc := uint32(0)
	lc := uint32(100)
	flow := &performatives.Flow{
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		Handle:         &h,
		DeliveryCount:  &dc,
		LinkCredit:     &lc,
	}
	body, err = flow.Encode()
	require.NoError(t, err)
	require.NoError(t, c.WritePerformative(0, body))
	time.Sleep(50 * time.Millisecond)

	// Drain transfers in background (stops when connection closes)
	stopDrain := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopDrain:
				return
			default:
				c.ReadPerformative() //nolint:errcheck // best-effort
			}
		}
	}()

	// Concurrently publish and update flow
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 20 {
			b.Publish(context.Background(), "test/race", []byte("msg"), nil)
		}
	}()

	go func() {
		defer wg.Done()
		for range 10 {
			newLC := uint32(100)
			newDC := uint32(0)
			f := &performatives.Flow{
				IncomingWindow: 65535,
				OutgoingWindow: 65535,
				Handle:         &h,
				DeliveryCount:  &newDC,
				LinkCredit:     &newLC,
			}
			fb, _ := f.Encode()
			c.WritePerformative(0, fb) //nolint:errcheck // best-effort
		}
	}()

	wg.Wait()
	close(stopDrain)
}
