// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	corebroker "github.com/absmach/fluxmq/broker"
	queuepkg "github.com/absmach/fluxmq/queue"
	qstorage "github.com/absmach/fluxmq/queue/storage"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	eventsExchange   = "events"
	testRuleTrace    = `["rule-a"]`
	testCustomHeader = "x-custom"
	testOtherTarget  = "other"
	testHeaderValue  = "value"
)

type mockChannelQueueManager struct {
	lastCursor        *qtypes.CursorOption
	lastPublish       qtypes.PublishRequest
	publishCalls      int
	exactStreamName   string
	exactPublish      qtypes.PublishRequest
	exactPublishCalls int
	exactPublishErr   error
	exactPublishCtx   context.Context
	queueCfg          *qtypes.QueueConfig
	createdQueues     []qtypes.QueueConfig
	updatedQueues     []qtypes.QueueConfig
	createQueueErr    error
	updateQueueErr    error
}

func (m *mockChannelQueueManager) Publish(_ context.Context, publish qtypes.PublishRequest) error {
	m.lastPublish = publish
	m.publishCalls++
	return nil
}

func (m *mockChannelQueueManager) PublishToDurableStream(ctx context.Context, queueName string, publish qtypes.PublishRequest) error {
	m.exactStreamName = queueName
	m.exactPublish = publish
	m.exactPublishCtx = ctx
	m.exactPublishCalls++
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.exactPublishErr
}

func (m *mockChannelQueueManager) Subscribe(context.Context, string, string, string, string, string) error {
	return nil
}

func (m *mockChannelQueueManager) SubscribeWithCursor(_ context.Context, _ string, _ string, _ string, _ string, _ string, cursor *qtypes.CursorOption) error {
	m.lastCursor = cursor
	return nil
}

func (m *mockChannelQueueManager) Unsubscribe(context.Context, string, string, string, string) error {
	return nil
}

func (m *mockChannelQueueManager) Ack(context.Context, string, string, string) error {
	return nil
}

func (m *mockChannelQueueManager) Nack(context.Context, string, string, string) error {
	return nil
}

func (m *mockChannelQueueManager) Reject(context.Context, string, string, string, string) error {
	return nil
}

func (m *mockChannelQueueManager) CreateQueue(_ context.Context, cfg qtypes.QueueConfig) error {
	m.createdQueues = append(m.createdQueues, cfg)
	return m.createQueueErr
}

func (m *mockChannelQueueManager) GetQueue(context.Context, string) (*qtypes.QueueConfig, error) {
	return m.queueCfg, nil
}

func (m *mockChannelQueueManager) UpdateQueue(_ context.Context, cfg qtypes.QueueConfig) error {
	m.updatedQueues = append(m.updatedQueues, cfg)
	return m.updateQueueErr
}

func (m *mockChannelQueueManager) CommitOffset(context.Context, string, string, uint64) error {
	return nil
}

type normalizingHookProvider struct {
	aliasTopic     string
	canonicalTopic string
}

func (n *normalizingHookProvider) HandleHook(_ context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookResult, error) {
	switch req.Topic {
	case n.aliasTopic, n.canonicalTopic:
		return corebroker.BlockingHookResult{Allowed: true, Topic: n.canonicalTopic}, nil
	default:
		return corebroker.BlockingHookResult{Allowed: true}, nil
	}
}

// newTestChannel builds a channel with no listener policy, which
// Connection.connectionPolicy resolves to the untrusted external mode.
func newTestChannel(t *testing.T) (*Channel, *bytes.Buffer) {
	t.Helper()
	return newTestChannelWithPolicy(t, nil)
}

// newTestChannelWithPolicy builds a channel served under policy. Trust is set
// on the policy directly rather than through a constructor so the reserved
// property boundary can be exercised independently of the local-principal
// authentication and publish-only machinery.
func newTestChannelWithPolicy(t *testing.T, policy *ConnectionPolicy) (*Channel, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	c := &Connection{
		broker:   b,
		policy:   policy,
		writer:   bufio.NewWriter(buf),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   testConnectionID,
		channels: make(map[uint16]*Channel),
	}
	ch := newChannel(c, 1)
	return ch, buf
}

// trustedTestPolicy marks a connection as service-to-service without making it
// publish-only, matching the decoupling of trust from operation mode.
func trustedTestPolicy() *ConnectionPolicy {
	return &ConnectionPolicy{mode: ConnectionPolicyExternal, trusted: true}
}

func readFramesFrom(t *testing.T, buf *bytes.Buffer, start int) []*codec.Frame {
	t.Helper()
	data := buf.Bytes()
	if start > len(data) {
		t.Fatalf("start offset beyond buffer length")
	}
	r := bytes.NewReader(data[start:])
	var frames []*codec.Frame
	for r.Len() > 0 {
		frame, err := codec.ReadFrame(r)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func TestPublishStateMachine_HeaderWithoutPublish(t *testing.T) {
	ch, buf := newTestChannel(t)

	header := &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: 0,
	}
	var payload bytes.Buffer
	if err := header.WriteContentHeader(&payload); err != nil {
		t.Fatalf("WriteContentHeader failed: %v", err)
	}

	ch.handleHeaderFrame(&codec.Frame{
		Type:    codec.FrameHeader,
		Channel: 1,
		Payload: payload.Bytes(),
	})

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	closeMsg, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	if closeMsg.ReplyCode != codec.UnexpectedFrame {
		t.Fatalf("expected UnexpectedFrame, got %d", closeMsg.ReplyCode)
	}
}

func TestPublishStateMachine_BodyWithoutHeader(t *testing.T) {
	ch, buf := newTestChannel(t)

	ch.handleBodyFrame(&codec.Frame{
		Type:    codec.FrameBody,
		Channel: 1,
		Payload: []byte("payload"),
	})

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	closeMsg, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	if closeMsg.ReplyCode != codec.UnexpectedFrame {
		t.Fatalf("expected UnexpectedFrame, got %d", closeMsg.ReplyCode)
	}
}

func TestMandatoryPublishReturn(t *testing.T) {
	ch, buf := newTestChannel(t)
	ch.confirmMode = true

	if err := ch.handleMethod(&codec.BasicPublish{
		Exchange:   "",
		RoutingKey: "no.route",
		Mandatory:  true,
	}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	payload := []byte("hello")
	header := &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: uint64(len(payload)),
		Properties: codec.BasicProperties{
			MessageID: "mid-1",
		},
	}
	var headerBuf bytes.Buffer
	if err := header.WriteContentHeader(&headerBuf); err != nil {
		t.Fatalf("WriteContentHeader failed: %v", err)
	}

	ch.handleHeaderFrame(&codec.Frame{
		Type:    codec.FrameHeader,
		Channel: 1,
		Payload: headerBuf.Bytes(),
	})
	ch.handleBodyFrame(&codec.Frame{
		Type:    codec.FrameBody,
		Channel: 1,
		Payload: payload,
	})

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 4 {
		t.Fatalf("expected 4 frames, got %d", len(frames))
	}
	decoded0, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if _, ok := decoded0.(*codec.BasicReturn); !ok {
		t.Fatalf("expected BasicReturn, got %T", decoded0)
	}
	if frames[1].Type != codec.FrameHeader {
		t.Fatalf("expected header frame, got %d", frames[1].Type)
	}
	if frames[2].Type != codec.FrameBody {
		t.Fatalf("expected body frame, got %d", frames[2].Type)
	}
	decoded3, err := frames[3].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if _, ok := decoded3.(*codec.BasicAck); !ok {
		t.Fatalf("expected BasicAck, got %T", decoded3)
	}
}

func TestPublishStateMachineStampsPublisherForCrossDeliver(t *testing.T) {
	ch, _ := newTestChannel(t)

	if err := ch.conn.broker.router.Subscribe("mqtt-client", testTelemetryRoom1, 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	calls := 0
	var gotProps map[string]string
	ch.conn.broker.SetCrossDeliver(func(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		calls++
		gotProps = props
	})

	if err := ch.handleMethod(&codec.BasicPublish{
		Exchange:   "",
		RoutingKey: testTelemetryRoom1AM,
	}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	payload := []byte("hello")
	header := &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: uint64(len(payload)),
	}
	var headerBuf bytes.Buffer
	if err := header.WriteContentHeader(&headerBuf); err != nil {
		t.Fatalf("WriteContentHeader failed: %v", err)
	}

	ch.handleHeaderFrame(&codec.Frame{
		Type:    codec.FrameHeader,
		Channel: 1,
		Payload: headerBuf.Bytes(),
	})
	ch.handleBodyFrame(&codec.Frame{
		Type:    codec.FrameBody,
		Channel: 1,
		Payload: payload,
	})

	if calls != 1 {
		t.Fatalf("expected 1 cross-deliver call, got %d", calls)
	}
	if gotProps[corebroker.ClientIDProperty] != PrefixedClientID(testConnectionID) {
		t.Fatalf("expected client_id property %q, got %q", PrefixedClientID(testConnectionID), gotProps[corebroker.ClientIDProperty])
	}
}

func TestPublishUsesHookTopic(t *testing.T) {
	ch, _ := newTestChannel(t)
	aliasTopic := "m/d1/c/ch1/messages"
	canonicalTopic := "m/26ad5c3f-cd91-4ff0-9685-0c3115643174/c/cdc8f55f-0c54-4a9f-b4aa-8c69d4a8ce15/messages"
	ch.conn.broker.SetBlockingHooks(corebroker.NewBlockingHookEngine(&normalizingHookProvider{
		aliasTopic:     aliasTopic,
		canonicalTopic: canonicalTopic,
	}, corebroker.HookFailDeny, nil, nil, nil))
	if err := ch.conn.broker.router.Subscribe("mqtt-client", canonicalTopic, 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	var gotTopic string
	var gotPayload []byte
	ch.conn.broker.SetCrossDeliver(func(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		gotTopic = topic
		gotPayload = append([]byte(nil), payload...)
	})

	payload := []byte("payload")
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: aliasTopic}
	ch.pendingHeader = &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: uint64(len(payload)),
	}
	ch.pendingBody = payload

	ch.completePublish()

	if gotTopic != canonicalTopic {
		t.Fatalf("expected topic %q, got %q", canonicalTopic, gotTopic)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("expected payload %q, got %q", payload, gotPayload)
	}
}

func TestExchangePublishUsesHookRoutingKeyForBindings(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{}
	ch.conn.broker.queueManager = mockQM

	ch.exchanges["events"] = &exchange{name: eventsExchange, typ: "direct"}
	ch.bindings = append(ch.bindings, binding{
		queue:      testOrders,
		exchange:   eventsExchange,
		routingKey: "canonical",
	})

	ch.conn.broker.SetBlockingHooks(corebroker.NewBlockingHookEngine(&normalizingHookProvider{
		aliasTopic:     "events/alias",
		canonicalTopic: "events/canonical",
	}, corebroker.HookFailDeny, nil, nil, nil))

	payload := []byte("payload")
	ch.pendingMethod = &codec.BasicPublish{Exchange: eventsExchange, RoutingKey: "alias"}
	ch.pendingHeader = &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: uint64(len(payload)),
	}
	ch.pendingBody = payload

	ch.completePublish()

	if mockQM.publishCalls != 1 {
		t.Fatalf("expected 1 queue publish, got %d", mockQM.publishCalls)
	}
	if mockQM.lastPublish.Topic != "$queue/orders/canonical" {
		t.Fatalf("expected normalized queue topic, got %q", mockQM.lastPublish.Topic)
	}
	if !bytes.Equal(mockQM.lastPublish.Payload, payload) {
		t.Fatalf("expected payload %q, got %q", payload, mockQM.lastPublish.Payload)
	}
}

func TestHandleQueuePublishCarriesClientID(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{}
	ch.conn.broker.queueManager = mockQM

	ch.handleQueuePublish("$queue/orders/process", []byte("hello"), map[string]string{"trace": "1"}, PrefixedClientID(testConnectionID))

	if mockQM.publishCalls != 1 {
		t.Fatalf("expected 1 queue publish, got %d", mockQM.publishCalls)
	}
	if mockQM.lastPublish.ClientID != PrefixedClientID(testConnectionID) {
		t.Fatalf("expected client ID %q, got %q", PrefixedClientID(testConnectionID), mockQM.lastPublish.ClientID)
	}
	if mockQM.lastPublish.Properties["trace"] != "1" {
		t.Fatalf("expected trace property preserved, got %q", mockQM.lastPublish.Properties["trace"])
	}
	if mockQM.lastPublish.Properties[corebroker.ClientIDProperty] != PrefixedClientID(testConnectionID) {
		t.Fatalf("expected client_id property %q, got %q", PrefixedClientID(testConnectionID), mockQM.lastPublish.Properties[corebroker.ClientIDProperty])
	}
}

func TestPrefetchBuffering(t *testing.T) {
	ch, buf := newTestChannel(t)
	ch.prefetchCount = 1
	ch.consumers[testCtag] = &consumer{
		tag:        testCtag,
		queue:      "q",
		mqttFilter: "q",
		noAck:      false,
	}

	props := map[string]string{qtypes.PropMessageID: "m1"}
	ch.deliverMessage("q", []byte("one"), props)
	ch.deliverMessage("q", []byte("two"), props)

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames for first delivery, got %d", len(frames))
	}
	if _, err := frames[0].Decode(); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	beforeAck := buf.Len()
	if err := ch.handleBasicAck(&codec.BasicAck{DeliveryTag: 1}); err != nil {
		t.Fatalf("handleBasicAck failed: %v", err)
	}
	framesAfter := readFramesFrom(t, buf, beforeAck)
	if len(framesAfter) != 3 {
		t.Fatalf("expected 3 frames after ack, got %d", len(framesAfter))
	}
}

func TestChannelFlowQueueing(t *testing.T) {
	ch, buf := newTestChannel(t)
	ch.flow = false
	ch.consumers[testCtag] = &consumer{
		tag:        testCtag,
		queue:      "q",
		mqttFilter: "q",
		noAck:      true,
	}

	ch.deliverMessage("q", []byte("one"), map[string]string{})
	if buf.Len() != 0 {
		t.Fatalf("expected no frames while flow is disabled")
	}

	if err := ch.handleMethod(&codec.ChannelFlow{Active: true}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 4 {
		t.Fatalf("expected 4 frames after flow resume, got %d", len(frames))
	}
	decoded0, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if _, ok := decoded0.(*codec.ChannelFlowOk); !ok {
		t.Fatalf("expected ChannelFlowOk, got %T", decoded0)
	}
}

func TestExchangeNotFoundOnPublish(t *testing.T) {
	ch, buf := newTestChannel(t)

	if err := ch.handleMethod(&codec.BasicPublish{
		Exchange:   "missing",
		RoutingKey: "rk",
	}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	closeMsg, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	if closeMsg.ReplyCode != codec.NotFound {
		t.Fatalf("expected NotFound, got %d", closeMsg.ReplyCode)
	}
}

func TestConsumerQueueMatches(t *testing.T) {
	ch, _ := newTestChannel(t)

	tests := []struct {
		name  string
		cons  consumer
		topic string
		want  bool
	}{
		{
			name: "stream queue root topic",
			cons: consumer{
				queue:     testDemoEvents,
				queueName: testDemoEvents,
				pattern:   "",
			},
			topic: "$queue/demo-events",
			want:  true,
		},
		{
			name: "stream queue routing key",
			cons: consumer{
				queue:     testDemoEvents,
				queueName: testDemoEvents,
				pattern:   "",
			},
			topic: "$queue/demo-events/user/action",
			want:  true,
		},
		{
			name: "queue filter root",
			cons: consumer{
				queue:     testQueueDemoOrders,
				queueName: testDemoOrders,
				pattern:   "#",
			},
			topic: "$queue/demo-orders",
			want:  true,
		},
		{
			name: "queue filter with routing key",
			cons: consumer{
				queue:     testQueueDemoOrders,
				queueName: testDemoOrders,
				pattern:   "#",
			},
			topic: "$queue/demo-orders/new",
			want:  true,
		},
		{
			name: "queue filter mismatch",
			cons: consumer{
				queue:     testQueueDemoOrders,
				queueName: testDemoOrders,
				pattern:   "#",
			},
			topic: "$queue/other/new",
			want:  false,
		},
		{
			name: "plain topic match",
			cons: consumer{
				queue:      testSensorWild,
				mqttFilter: testSensorWild,
			},
			topic: "sensor/temperature",
			want:  true,
		},
		{
			name: "plain topic mismatch",
			cons: consumer{
				queue:      testSensorWild,
				mqttFilter: testSensorWild,
			},
			topic: "control/restart",
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ch.consumerQueueMatches(&tc.cons, tc.topic)
			if got != tc.want {
				t.Fatalf("consumerQueueMatches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseStreamOffsetString(t *testing.T) {
	first, ok := parseStreamOffsetString("first")
	if !ok || first.Position != qtypes.CursorEarliest {
		t.Fatalf("expected earliest, got %+v", first)
	}

	last, ok := parseStreamOffsetString("last")
	if !ok || last.Position != qtypes.CursorLatest {
		t.Fatalf("expected latest, got %+v", last)
	}

	offset, ok := parseStreamOffsetString("offset=42")
	if !ok || offset.Position != qtypes.CursorOffset || offset.Offset != 42 {
		t.Fatalf("expected offset 42, got %+v", offset)
	}

	ts, ok := parseStreamOffsetString("timestamp=1700000000")
	if !ok || ts.Position != qtypes.CursorTimestamp || ts.Timestamp.IsZero() {
		t.Fatalf("expected timestamp, got %+v", ts)
	}
}

func TestHandleBasicConsumeDefaultsStreamCursorToResume(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{}
	ch.conn.broker.queueManager = mockQM
	ch.queues[testEvents] = &queueInfo{name: testEvents, queueType: string(qtypes.QueueTypeStream)}

	err := ch.handleMethod(&codec.BasicConsume{
		Queue:       testEvents,
		ConsumerTag: "stream-reader",
		NoWait:      true,
	})
	if err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	if mockQM.lastCursor == nil {
		t.Fatal("expected SubscribeWithCursor to be called")
	}
	if mockQM.lastCursor.Position != qtypes.CursorDefault {
		t.Fatalf("expected default cursor position, got %+v", mockQM.lastCursor)
	}
	if mockQM.lastCursor.Mode != qtypes.GroupModeStream {
		t.Fatalf("expected stream mode cursor, got %+v", mockQM.lastCursor)
	}
}

func TestHandleBasicConsumeInfersStreamFromQueueManager(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{
		queueCfg: &qtypes.QueueConfig{
			Name: testEvents,
			Type: qtypes.QueueTypeStream,
		},
	}
	ch.conn.broker.queueManager = mockQM

	err := ch.handleMethod(&codec.BasicConsume{
		Queue:       "$queue/events/supermq/domain/#",
		ConsumerTag: "stream-reader",
		NoWait:      true,
	})
	if err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	if mockQM.lastCursor == nil {
		t.Fatal("expected SubscribeWithCursor to be called")
	}
	if mockQM.lastCursor.Position != qtypes.CursorDefault {
		t.Fatalf("expected default cursor position, got %+v", mockQM.lastCursor)
	}
}

func TestExtractMessageTTL(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]any
		wantTTL time.Duration
		wantOK  bool
	}{
		{"nil args", nil, 0, false},
		{"empty args", map[string]any{}, 0, false},
		{"missing key", map[string]any{"x-other": 100}, 0, false},
		{"int32 millis", map[string]any{testXMessageTTL: int32(60000)}, 60 * time.Second, true},
		{"int64 millis", map[string]any{testXMessageTTL: int64(5000)}, 5 * time.Second, true},
		{"int millis", map[string]any{testXMessageTTL: 30000}, 30 * time.Second, true},
		{"string millis", map[string]any{testXMessageTTL: "1000"}, time.Second, true},
		{"zero", map[string]any{testXMessageTTL: int64(0)}, 0, true},
		{"negative", map[string]any{testXMessageTTL: int64(-1)}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractMessageTTL(tt.args)
			if ok != tt.wantOK {
				t.Fatalf("ok=%v, want %v", ok, tt.wantOK)
			}
			if got != tt.wantTTL {
				t.Fatalf("ttl=%v, want %v", got, tt.wantTTL)
			}
		})
	}
}

func TestQueueDeclareClassicCreatesQueueConfig(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{}
	ch.conn.broker.queueManager = mockQM

	err := ch.handleQueueDeclare(&codec.QueueDeclare{
		Queue:   testOrders,
		Durable: true,
	})
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	if len(mockQM.createdQueues) != 1 {
		t.Fatalf("expected 1 created queue, got %d", len(mockQM.createdQueues))
	}
	cfg := mockQM.createdQueues[0]
	if cfg.Name != testOrders {
		t.Fatalf("expected queue name 'orders', got %q", cfg.Name)
	}
	if cfg.Type != qtypes.QueueTypeClassic {
		t.Fatalf("expected classic type, got %q", cfg.Type)
	}
	if !cfg.Durable {
		t.Fatal("expected durable=true")
	}
}

func TestQueueDeclareWithMessageTTL(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{}
	ch.conn.broker.queueManager = mockQM

	err := ch.handleQueueDeclare(&codec.QueueDeclare{
		Queue:   testOrders,
		Durable: true,
		Arguments: map[string]any{
			testXMessageTTL: int32(60000), // 60 seconds in ms
		},
	})
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	if len(mockQM.createdQueues) != 1 {
		t.Fatalf("expected 1 created queue, got %d", len(mockQM.createdQueues))
	}
	cfg := mockQM.createdQueues[0]
	if cfg.MessageTTL != 60*time.Second {
		t.Fatalf("expected MessageTTL=60s, got %v", cfg.MessageTTL)
	}
}

func TestQueueDeclareStreamWithTTL(t *testing.T) {
	ch, _ := newTestChannel(t)
	mockQM := &mockChannelQueueManager{}
	ch.conn.broker.queueManager = mockQM

	err := ch.handleQueueDeclare(&codec.QueueDeclare{
		Queue:   testEvents,
		Durable: true,
		Arguments: map[string]any{
			"x-queue-type":  "stream",
			testXMessageTTL: int64(30000),
			"x-max-age":     "7d",
		},
	})
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	if len(mockQM.createdQueues) != 1 {
		t.Fatalf("expected 1 created queue, got %d", len(mockQM.createdQueues))
	}
	cfg := mockQM.createdQueues[0]
	if cfg.Type != qtypes.QueueTypeStream {
		t.Fatalf("expected stream type, got %q", cfg.Type)
	}
	if cfg.MessageTTL != 30*time.Second {
		t.Fatalf("expected MessageTTL=30s, got %v", cfg.MessageTTL)
	}
	if cfg.Retention.RetentionTime != 7*24*time.Hour {
		t.Fatalf("expected RetentionTime=7d, got %v", cfg.Retention.RetentionTime)
	}
}

func TestQueueRedeclareProtectedQueueClosesChannel(t *testing.T) {
	ch, buf := newTestChannel(t)
	existing := qtypes.DefaultQueueConfig("atom-audit", "$queue/atom-audit/#")
	existing.Type = qtypes.QueueTypeStream
	existing.Reserved = true
	mockQM := &mockChannelQueueManager{
		queueCfg:       &existing,
		createQueueErr: qstorage.ErrQueueAlreadyExists,
		updateQueueErr: queuepkg.ErrProtectedQueueMutation,
	}
	ch.conn.broker.queueManager = mockQM

	if err := ch.handleQueueDeclare(&codec.QueueDeclare{
		Queue:   existing.Name,
		Durable: true,
		Arguments: map[string]any{
			"x-queue-type": "classic",
		},
	}); err != nil {
		t.Fatalf("handleQueueDeclare() error = %v", err)
	}

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want one ChannelClose", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	closeMessage, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("method = %T, want *codec.ChannelClose", decoded)
	}
	if closeMessage.ReplyCode != codec.PreconditionFailed {
		t.Fatalf("reply code = %d, want %d", closeMessage.ReplyCode, codec.PreconditionFailed)
	}
	if closeMessage.ClassID != codec.ClassQueue || closeMessage.MethodID != codec.MethodQueueDeclare {
		t.Fatalf("close method = %d/%d, want queue.declare", closeMessage.ClassID, closeMessage.MethodID)
	}
}

// A trusted service relaying a message may state the identity and protocol it
// came from. Anyone else is stamped with their own authenticated identity, so a
// publisher cannot attribute a message to another principal or protocol. See
// TestPublishReservedHeadersIngressTrustBoundary for the reserved-prefix
// headers under the same boundary.
func TestPublishOriginHeadersTrustBoundary(t *testing.T) {
	const (
		relayedID = "pub-123"
		authedID  = "amqp-tenant-7"
	)

	tests := []struct {
		name         string
		policy       *ConnectionPolicy
		wantID       string
		wantProtocol string
	}{
		{
			name:         "trusted policy relays origin identity and protocol",
			policy:       trustedTestPolicy(),
			wantID:       relayedID,
			wantProtocol: corebroker.ProtocolHTTP,
		},
		{
			name:         "external policy stamps authenticated identity",
			policy:       NewExternalConnectionPolicy(nil, nil, 0),
			wantID:       authedID,
			wantProtocol: corebroker.ProtocolAMQP091,
		},
		{
			name:         "absent policy stamps authenticated identity",
			policy:       nil,
			wantID:       authedID,
			wantProtocol: corebroker.ProtocolAMQP091,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch, _ := newTestChannelWithPolicy(t, tc.policy)
			clientID := PrefixedClientID(ch.conn.connID)

			// Both identity sources are populated so the assertion shows which one
			// the publish path chose rather than which one happened to be set.
			engine := corebroker.NewAuthEngine(nil, nil)
			engine.SetExternalID(clientID, authedID)
			ch.conn.broker.SetAuthEngine(engine)
			if tc.policy != nil {
				tc.policy.externalAuth = engine
			}

			require.NoError(t, ch.conn.broker.router.Subscribe("mqtt-client", testTelemetryRoom1, 1, storage.SubscribeOptions{}))

			var gotProps map[string]string
			ch.conn.broker.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, props map[string]string) {
				gotProps = props
			})
			require.NoError(t, ch.handleMethod(&codec.BasicPublish{Exchange: "", RoutingKey: testTelemetryRoom1AM}))

			payload := []byte("hello")
			header := &codec.ContentHeader{
				ClassID:  codec.ClassBasic,
				Weight:   0,
				BodySize: uint64(len(payload)),
				Properties: codec.BasicProperties{
					Headers: map[string]any{
						corebroker.ExternalIDProperty: relayedID,
						corebroker.ProtocolProperty:   corebroker.ProtocolHTTP,
					},
				},
			}
			var headerBuf bytes.Buffer
			require.NoError(t, header.WriteContentHeader(&headerBuf))

			ch.handleHeaderFrame(&codec.Frame{Type: codec.FrameHeader, Channel: 1, Payload: headerBuf.Bytes()})
			ch.handleBodyFrame(&codec.Frame{Type: codec.FrameBody, Channel: 1, Payload: payload})

			require.NotNil(t, gotProps, "expected cross-deliver call")
			assert.Equal(t, tc.wantID, gotProps[corebroker.ExternalIDProperty])
			assert.Equal(t, tc.wantProtocol, gotProps[corebroker.ProtocolProperty])
		})
	}
}

// A trusted listener carries reserved properties inward so a service can pass
// internal state to whichever service consumes the message next. An externally
// authenticated publisher is a tenant or device whatever protocol it speaks, so
// its reserved headers are dropped and cannot be forged. Everything else a
// client sets stays out of the property bag either way.
func TestPublishReservedHeadersIngressTrustBoundary(t *testing.T) {
	reserved := corebroker.ReservedPropertyPrefix + "re.trace"

	tests := []struct {
		name    string
		policy  *ConnectionPolicy
		headers map[string]any
		want    map[string]string
		absent  []string
	}{
		{
			name:    "trusted policy carries reserved header",
			policy:  trustedTestPolicy(),
			headers: map[string]any{reserved: testRuleTrace},
			want:    map[string]string{reserved: testRuleTrace},
		},
		{
			name:    "trusted policy drops unreserved header",
			policy:  trustedTestPolicy(),
			headers: map[string]any{testCustomHeader: testHeaderValue},
			absent:  []string{testCustomHeader},
		},
		{
			name:    "trusted policy drops non-string reserved header",
			policy:  trustedTestPolicy(),
			headers: map[string]any{reserved: int64(1)},
			absent:  []string{reserved},
		},
		{
			name:    "trusted policy carries reserved alongside dropped custom header",
			policy:  trustedTestPolicy(),
			headers: map[string]any{reserved: testRuleTrace, testCustomHeader: testHeaderValue},
			want:    map[string]string{reserved: testRuleTrace},
			absent:  []string{testCustomHeader},
		},
		{
			name:    "external policy drops forged reserved header",
			policy:  NewExternalConnectionPolicy(nil, nil, 0),
			headers: map[string]any{reserved: testRuleTrace},
			absent:  []string{reserved},
		},
		{
			name:    "external policy drops forged reserved header alongside custom header",
			policy:  NewExternalConnectionPolicy(nil, nil, 0),
			headers: map[string]any{reserved: testRuleTrace, testCustomHeader: testHeaderValue},
			absent:  []string{reserved, testCustomHeader},
		},
		{
			name:    "absent policy drops forged reserved header",
			policy:  nil,
			headers: map[string]any{reserved: testRuleTrace},
			absent:  []string{reserved},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch, _ := newTestChannelWithPolicy(t, tc.policy)
			require.NoError(t, ch.conn.broker.router.Subscribe("mqtt-client", testTelemetryRoom1, 1, storage.SubscribeOptions{}))

			var gotProps map[string]string
			ch.conn.broker.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, props map[string]string) {
				gotProps = props
			})
			require.NoError(t, ch.handleMethod(&codec.BasicPublish{Exchange: "", RoutingKey: testTelemetryRoom1AM}))

			payload := []byte("hello")
			header := &codec.ContentHeader{
				ClassID:    codec.ClassBasic,
				Weight:     0,
				BodySize:   uint64(len(payload)),
				Properties: codec.BasicProperties{Headers: tc.headers},
			}
			var headerBuf bytes.Buffer
			require.NoError(t, header.WriteContentHeader(&headerBuf))

			ch.handleHeaderFrame(&codec.Frame{Type: codec.FrameHeader, Channel: 1, Payload: headerBuf.Bytes()})
			ch.handleBodyFrame(&codec.Frame{Type: codec.FrameBody, Channel: 1, Payload: payload})

			require.NotNil(t, gotProps, "expected cross-deliver call")
			for key, value := range tc.want {
				assert.Equal(t, value, gotProps[key])
			}
			for _, key := range tc.absent {
				assert.NotContains(t, gotProps, key)
			}
		})
	}
}

// Egress mirrors ingress: an externally authenticated consumer must not observe
// broker-internal state another service set, while ordinary properties are
// delivered as headers to everyone.
func TestDeliveryReservedHeadersEgressTrustBoundary(t *testing.T) {
	reserved := corebroker.ReservedPropertyPrefix + "re.trace"

	tests := []struct {
		name   string
		policy *ConnectionPolicy
		want   map[string]any
		absent []string
	}{
		{
			name:   "trusted policy reveals reserved property",
			policy: trustedTestPolicy(),
			want:   map[string]any{reserved: testRuleTrace, testCustomHeader: testHeaderValue},
		},
		{
			name: "service policy reveals reserved property to a first-party consumer",
			// The reason the boundary has an inward direction at all: without a
			// consumer that may read them, reserved properties would be write-only.
			policy: NewLocalServiceConnectionPolicy(nil, nil, nil, 0),
			want:   map[string]any{reserved: testRuleTrace, testCustomHeader: testHeaderValue},
		},
		{
			name:   "publish-only policy still reveals nothing it cannot consume",
			policy: NewLocalPublishOnlyConnectionPolicy(nil, nil, nil, 0),
			want:   map[string]any{reserved: testRuleTrace, testCustomHeader: testHeaderValue},
		},
		{
			name:   "external policy hides reserved property",
			policy: NewExternalConnectionPolicy(nil, nil, 0),
			want:   map[string]any{testCustomHeader: testHeaderValue},
			absent: []string{reserved},
		},
		{
			name:   "absent policy hides reserved property",
			policy: nil,
			want:   map[string]any{testCustomHeader: testHeaderValue},
			absent: []string{reserved},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch, buf := newTestChannelWithPolicy(t, tc.policy)
			cons := &consumer{tag: testCtag1, noAck: true}

			props := map[string]string{
				reserved:         testRuleTrace,
				testCustomHeader: testHeaderValue,
			}
			require.NoError(t, ch.sendDelivery(cons, testTelemetryRoom1, []byte("hello"), props))
			require.NoError(t, ch.conn.writer.Flush())

			headers := deliveryHeadersFrom(t, buf)
			for key, value := range tc.want {
				assert.Equal(t, value, headers[key])
			}
			for _, key := range tc.absent {
				assert.NotContains(t, headers, key)
			}
		})
	}
}

// deliveryHeadersFrom decodes the content header frame of a single BasicDeliver
// written to buf and returns its application headers.
func deliveryHeadersFrom(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	frames := readFramesFrom(t, buf, 0)
	require.GreaterOrEqual(t, len(frames), 2, "expected method and header frames")
	require.Equal(t, codec.FrameHeader, frames[1].Type)

	decoded, err := frames[1].Decode()
	require.NoError(t, err)
	header, ok := decoded.(*codec.ContentHeader)
	require.True(t, ok, "expected content header, got %T", decoded)
	return header.Properties.Headers
}

func TestCancelConsumerByQueue(t *testing.T) {
	t.Run("cancels matching consumer and sends BasicCancel frame", func(t *testing.T) {
		ch, buf := newTestChannel(t)

		ch.consumers[testCtag1] = &consumer{
			tag:       testCtag1,
			queueName: testEvents,
			groupID:   testWorkers,
		}
		ch.consumers["ctag-2"] = &consumer{
			tag:       "ctag-2",
			queueName: testOrders,
			groupID:   "processors",
		}

		beforeLen := buf.Len()
		ch.cancelConsumerByQueue(testEvents, testWorkers)

		// ctag-1 should be removed, ctag-2 should remain
		ch.consumersMu.RLock()
		if _, exists := ch.consumers[testCtag1]; exists {
			t.Fatal("expected ctag-1 to be removed")
		}
		if _, exists := ch.consumers["ctag-2"]; !exists {
			t.Fatal("expected ctag-2 to remain")
		}
		ch.consumersMu.RUnlock()

		// Should have written a BasicCancel frame
		frames := readFramesFrom(t, buf, beforeLen)
		if len(frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(frames))
		}
		decoded, err := frames[0].Decode()
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		cancel, ok := decoded.(*codec.BasicCancel)
		if !ok {
			t.Fatalf("expected *codec.BasicCancel, got %T", decoded)
		}
		if cancel.ConsumerTag != testCtag1 {
			t.Fatalf("expected ConsumerTag %q, got %q", testCtag1, cancel.ConsumerTag)
		}
		if !cancel.NoWait {
			t.Fatal("expected NoWait=true for server-initiated cancel")
		}
	})

	t.Run("no-op when no consumers match", func(t *testing.T) {
		ch, buf := newTestChannel(t)

		ch.consumers[testCtag1] = &consumer{
			tag:       testCtag1,
			queueName: testEvents,
			groupID:   testWorkers,
		}

		beforeLen := buf.Len()
		ch.cancelConsumerByQueue("nonexistent", "group")

		ch.consumersMu.RLock()
		if len(ch.consumers) != 1 {
			t.Fatalf("expected 1 consumer to remain, got %d", len(ch.consumers))
		}
		ch.consumersMu.RUnlock()

		if buf.Len() != beforeLen {
			t.Fatal("expected no frames to be written")
		}
	})

	t.Run("no-op on closed channel", func(t *testing.T) {
		ch, buf := newTestChannel(t)
		ch.closed.Store(true)

		ch.consumers[testCtag1] = &consumer{
			tag:       testCtag1,
			queueName: testEvents,
			groupID:   testWorkers,
		}

		beforeLen := buf.Len()
		ch.cancelConsumerByQueue(testEvents, testWorkers)

		ch.consumersMu.RLock()
		if _, exists := ch.consumers[testCtag1]; !exists {
			t.Fatal("expected consumer to remain on closed channel")
		}
		ch.consumersMu.RUnlock()

		if buf.Len() != beforeLen {
			t.Fatal("expected no frames to be written on closed channel")
		}
	})
}

// The consumer lifecycle is admitted only on a listener that permits consumers,
// and only for a queue the principal's own subscribe ACL names. A publish-only
// principal keeps its existing, narrower method set.
func TestAuthorizeLocalMethodConsumerLifecycle(t *testing.T) {
	const allowedQueue = "m"

	servicePolicy := func(authz LocalPrincipalAuthorizer) *ConnectionPolicy {
		return NewLocalServiceConnectionPolicy(nil, authz, nil, 0)
	}
	publishOnlyPolicy := func(authz LocalPrincipalAuthorizer) *ConnectionPolicy {
		return NewLocalPublishOnlyConnectionPolicy(nil, authz, nil, 0)
	}

	tests := []struct {
		name        string
		policy      func(LocalPrincipalAuthorizer) *ConnectionPolicy
		method      any
		wantAllowed bool
	}{
		{
			name:        "service consumes a permitted queue",
			policy:      servicePolicy,
			method:      &codec.BasicConsume{Queue: allowedQueue},
			wantAllowed: true,
		},
		{
			name:   "service is refused a queue outside its ACL",
			policy: servicePolicy,
			method: &codec.BasicConsume{Queue: testOtherTarget},
		},
		{
			name:        "service gets from a permitted queue",
			policy:      servicePolicy,
			method:      &codec.BasicGet{Queue: allowedQueue},
			wantAllowed: true,
		},
		{
			name:   "service is refused getting from a queue outside its ACL",
			policy: servicePolicy,
			method: &codec.BasicGet{Queue: testOtherTarget},
		},
		{
			name:        "service passively declares a permitted queue",
			policy:      servicePolicy,
			method:      &codec.QueueDeclare{Queue: allowedQueue, Passive: true},
			wantAllowed: true,
		},
		{
			name:   "service is refused passively declaring a queue outside its ACL",
			policy: servicePolicy,
			method: &codec.QueueDeclare{Queue: testOtherTarget, Passive: true},
		},
		{
			// A non-passive declare rewrites queue configuration, which the
			// subscribe ACL does not grant even for a queue it names.
			name:   "service is refused declaring a permitted queue non-passively",
			policy: servicePolicy,
			method: &codec.QueueDeclare{Queue: allowedQueue},
		},
		{
			name:        "service acknowledges a delivery",
			policy:      servicePolicy,
			method:      &codec.BasicAck{},
			wantAllowed: true,
		},
		{
			name:   "publish-only principal may not consume",
			policy: publishOnlyPolicy,
			method: &codec.BasicConsume{Queue: allowedQueue},
		},
		{
			name:   "publish-only principal may not declare a queue",
			policy: publishOnlyPolicy,
			method: &codec.QueueDeclare{Queue: allowedQueue, Passive: true},
		},
		{
			name:   "publish-only principal may not get",
			policy: publishOnlyPolicy,
			method: &codec.BasicGet{Queue: allowedQueue},
		},
		{
			name:   "publish-only principal may not acknowledge",
			policy: publishOnlyPolicy,
			method: &codec.BasicAck{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authz := &localAuthorizerStub{allowQueue: allowedQueue}
			ch, _ := newTestChannelWithPolicy(t, tc.policy(authz))
			ch.conn.localIdentity = &LocalSessionIdentity{PrincipalID: "rules-engine"}

			allowed, err := ch.authorizeLocalMethod(tc.method)
			require.NoError(t, err)
			assert.Equal(t, tc.wantAllowed, allowed)
		})
	}
}

// A service listener is trusted, so it exchanges reserved properties, and it
// relays origin identity because the messages it republishes are not its own.
func TestLocalServicePolicyTrust(t *testing.T) {
	policy := NewLocalServiceConnectionPolicy(nil, nil, nil, 0)

	assert.True(t, policy.carriesReservedProperties())
	assert.True(t, policy.usesLocalPrincipalAuth())
	assert.True(t, policy.permitsConsumers())
	assert.True(t, policy.propagatesOriginIdentity(),
		"a service relays messages it did not originate, so it may state their true origin")
}

// The publish-only listener stays exactly as it was: trusted for reserved
// properties, but never a consumer and never able to relay an origin.
func TestLocalPublishOnlyPolicyUnchanged(t *testing.T) {
	policy := NewLocalPublishOnlyConnectionPolicy(nil, nil, nil, 0)

	assert.True(t, policy.carriesReservedProperties())
	assert.True(t, policy.usesLocalPrincipalAuth())
	assert.False(t, policy.permitsConsumers())
	assert.False(t, policy.propagatesOriginIdentity(),
		"an audit publisher must not attribute its records to another origin")
}
