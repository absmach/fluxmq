// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	"github.com/absmach/fluxmq/internal/bufpool"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

// consumer represents a Basic.Consume subscription on a channel.
type consumer struct {
	tag       string
	queue     string
	queueName string
	pattern   string
	groupID   string
	noAck     bool
	exclusive bool
}

type queueInfo struct {
	name      string
	queueType string
	args      map[string]interface{}
}

// exchange represents a declared exchange (in-memory, per-connection for now).
type exchange struct {
	name       string
	typ        string // direct, fanout, topic, headers
	durable    bool
	autoDelete bool
	internal   bool
}

// binding represents a queue-to-exchange binding.
type binding struct {
	queue      string
	exchange   string
	routingKey string
	arguments  map[string]interface{}
}

// Channel represents an AMQP 0.9.1 channel multiplexed over a connection.
type Channel struct {
	conn *Connection
	id   uint16

	// Exchange/queue/binding state (local to this connection for non-durable)
	exchanges  map[string]*exchange
	queues     map[string]*queueInfo // declared queues by name
	bindings   []binding
	exchangeMu sync.RWMutex

	// Consumer management
	consumers   map[string]*consumer // tag -> consumer
	consumersMu sync.RWMutex
	nextTag     atomic.Uint64

	// Content accumulation state machine for incoming publishes
	pendingHeader       *codec.ContentHeader
	pendingBody         []byte
	pendingMethod       *codec.BasicPublish
	pendingBodySize     uint64
	pendingBodyReceived uint64

	// Unacked deliveries for manual ack mode
	unacked   map[uint64]*unackedDelivery
	unackedMu sync.Mutex

	// Flow control
	flow bool

	// Confirm mode
	confirmMode bool
	publishSeq  atomic.Uint64

	// Prefetch
	prefetchCount uint16
	prefetchSize  uint32

	// Pending deliveries for flow control/prefetch
	pendingDeliveries []pendingDelivery
	pendingMu         sync.Mutex

	// Queue sequence for server-generated names
	queueSeq atomic.Uint64

	closed atomic.Bool
}

type unackedDelivery struct {
	deliveryTag uint64
	routingKey  string
	queueName   string
	messageID   string
	groupID     string
}

type pendingDelivery struct {
	consumerTag string
	queue       string
	topic       string
	payload     []byte
	props       map[string]string
}

func newChannel(c *Connection, id uint16) *Channel {
	return &Channel{
		conn:      c,
		id:        id,
		exchanges: make(map[string]*exchange),
		queues:    make(map[string]*queueInfo),
		consumers: make(map[string]*consumer),
		unacked:   make(map[uint64]*unackedDelivery),
		flow:      true,
	}
}

func (ch *Channel) handleMethod(decoded any) error {
	switch m := decoded.(type) {
	// Channel
	case *codec.ChannelFlow:
		ch.flow = m.Active
		if err := ch.conn.writeMethod(ch.id, &codec.ChannelFlowOk{Active: m.Active}); err != nil {
			return err
		}
		if m.Active {
			ch.drainPending()
		}
		return nil
	case *codec.ChannelClose:
		ch.conn.writeMethod(ch.id, &codec.ChannelCloseOk{})
		ch.conn.closeChannel(ch.id)
		return nil
	case *codec.ChannelCloseOk:
		ch.conn.closeChannel(ch.id)
		return nil

	// Exchange
	case *codec.ExchangeDeclare:
		return ch.handleExchangeDeclare(m)
	case *codec.ExchangeDelete:
		return ch.handleExchangeDelete(m)
	case *codec.ExchangeBind:
		return ch.handleExchangeBind(m)
	case *codec.ExchangeUnbind:
		return ch.handleExchangeUnbind(m)

	// Queue
	case *codec.QueueDeclare:
		return ch.handleQueueDeclare(m)
	case *codec.QueueBind:
		return ch.handleQueueBind(m)
	case *codec.QueueUnbind:
		return ch.handleQueueUnbind(m)
	case *codec.QueuePurge:
		return ch.handleQueuePurge(m)
	case *codec.QueueDelete:
		return ch.handleQueueDelete(m)

	// Basic
	case *codec.BasicQos:
		return ch.handleBasicQos(m)
	case *codec.BasicConsume:
		return ch.handleBasicConsume(m)
	case *codec.BasicCancel:
		return ch.handleBasicCancel(m)
	case *codec.BasicPublish:
		if ch.pendingMethod != nil || ch.pendingHeader != nil {
			_ = ch.conn.sendChannelClose(ch.id, codec.CommandInvalid,
				"publish in progress", codec.ClassBasic, codec.MethodBasicPublish)
			ch.resetPendingPublish()
			return nil
		}
		m.Exchange = normalizeExchange(m.Exchange)
		if m.Exchange != "" && !ch.exchangeExists(m.Exchange) {
			_ = ch.conn.sendChannelClose(ch.id, codec.NotFound,
				"exchange not found", codec.ClassBasic, codec.MethodBasicPublish)
			ch.resetPendingPublish()
			return nil
		}
		ch.pendingMethod = m
		return nil
	case *codec.BasicGet:
		return ch.handleBasicGet(m)
	case *codec.BasicAck:
		return ch.handleBasicAck(m)
	case *codec.BasicReject:
		return ch.handleBasicReject(m)
	case *codec.BasicNack:
		return ch.handleBasicNack(m)
	case *codec.BasicRecover:
		return ch.handleBasicRecover(m)
	case *codec.BasicRecoverAsync:
		return ch.handleBasicRecoverAsync(m)

	// Tx
	case *codec.TxSelect:
		return ch.conn.writeMethod(ch.id, &codec.TxSelectOk{})
	case *codec.TxCommit:
		return ch.conn.writeMethod(ch.id, &codec.TxCommitOk{})
	case *codec.TxRollback:
		return ch.conn.writeMethod(ch.id, &codec.TxRollbackOk{})

	// Confirm
	case *codec.ConfirmSelect:
		ch.confirmMode = true
		if !m.NoWait {
			return ch.conn.writeMethod(ch.id, &codec.ConfirmSelectOk{})
		}
		return nil

	default:
		return fmt.Errorf("unhandled method on channel %d: %T", ch.id, m)
	}
}

// handleHeaderFrame processes a content header frame (part of a publish).
func (ch *Channel) handleHeaderFrame(frame *codec.Frame) {
	if ch.pendingMethod == nil || ch.pendingHeader != nil {
		_ = ch.conn.sendChannelClose(ch.id, codec.UnexpectedFrame,
			"unexpected content header", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}

	r := bytes.NewReader(frame.Payload)
	header, err := codec.ReadContentHeader(r)
	if err != nil {
		ch.conn.logger.Error("failed to read content header", "error", err)
		return
	}
	ch.pendingHeader = header
	ch.pendingBodySize = header.BodySize
	ch.pendingBodyReceived = 0
	capHint := 0
	if maxInt := uint64(^uint(0) >> 1); header.BodySize <= maxInt {
		capHint = int(header.BodySize)
	}
	ch.pendingBody = make([]byte, 0, capHint)

	// If body size is 0, the message is complete
	if header.BodySize == 0 {
		ch.completePublish()
	}
}

// handleBodyFrame processes a content body frame.
func (ch *Channel) handleBodyFrame(frame *codec.Frame) {
	if ch.pendingMethod == nil || ch.pendingHeader == nil {
		_ = ch.conn.sendChannelClose(ch.id, codec.UnexpectedFrame,
			"unexpected content body", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}

	nextSize := ch.pendingBodyReceived + uint64(len(frame.Payload))
	if nextSize > ch.pendingBodySize {
		_ = ch.conn.sendChannelClose(ch.id, codec.ContentTooLarge,
			"content body larger than header", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}

	ch.pendingBody = append(ch.pendingBody, frame.Payload...)
	ch.pendingBodyReceived = nextSize

	if ch.pendingBodyReceived == ch.pendingBodySize {
		ch.completePublish()
	}
}

// completePublish is called when all content frames for a publish have arrived.
func (ch *Channel) completePublish() {
	method := ch.pendingMethod
	header := ch.pendingHeader
	body := ch.pendingBody

	ch.resetPendingPublish()

	if method == nil || header == nil {
		return
	}

	ch.conn.broker.stats.IncrementMessagesReceived()
	ch.conn.broker.stats.AddBytesReceived(uint64(len(body)))

	// Build properties map for queue integration
	props := make(map[string]string)
	if header.Properties.ContentType != "" {
		props["content-type"] = header.Properties.ContentType
	}
	if header.Properties.ContentEncoding != "" {
		props["content-encoding"] = header.Properties.ContentEncoding
	}
	if header.Properties.CorrelationID != "" {
		props["correlation-id"] = base64.StdEncoding.EncodeToString([]byte(header.Properties.CorrelationID))
	}
	if header.Properties.ReplyTo != "" {
		props["reply-to"] = header.Properties.ReplyTo
	}
	if header.Properties.MessageID != "" {
		props[qtypes.PropMessageID] = header.Properties.MessageID
	}
	if header.Properties.Type != "" {
		props["type"] = header.Properties.Type
	}

	exchangeName := normalizeExchange(method.Exchange)
	routingKey := method.RoutingKey

	// Determine the topic from exchange+routingKey
	topic := routingKey
	if exchangeName != "" {
		topic = exchangeName + "/" + routingKey
	}

	// Stream commit: publish to $queue/<queue>/$commit with x-group-id and x-offset headers.
	if exchangeName == "" && strings.HasPrefix(routingKey, "$queue/") && strings.HasSuffix(routingKey, "/$commit") {
		qm := ch.conn.broker.getQueueManager()
		if qm == nil {
			ch.conn.logger.Warn("queue commit ignored: queue manager not configured", "routing_key", routingKey)
		} else {
			queueName := strings.TrimSuffix(strings.TrimPrefix(routingKey, "$queue/"), "/$commit")
			if queueName == "" {
				ch.conn.logger.Warn("queue commit missing queue name", "routing_key", routingKey)
			} else {
				headers := header.Properties.Headers
				groupID, ok := parseStringArg(headers[qtypes.PropCommitGroupID])
				if !ok || groupID == "" {
					ch.conn.logger.Warn("queue commit missing group id", "queue", queueName)
				} else {
					offsetVal, ok := headers[qtypes.PropCommitOffset]
					if !ok {
						ch.conn.logger.Warn("queue commit missing offset", "queue", queueName, "group", groupID)
					} else if n, ok := parseInt64Arg(offsetVal); !ok || n < 0 {
						ch.conn.logger.Warn("queue commit invalid offset", "queue", queueName, "group", groupID)
					} else if err := qm.CommitOffset(context.Background(), queueName, groupID, uint64(n)); err != nil {
						ch.conn.logger.Warn("queue commit failed", "queue", queueName, "group", groupID, "error", err)
					}
				}
			}
		}
		if ch.confirmMode {
			ch.sendPublisherAck()
		}
		return
	}

	// Direct queue publish: use $queue/ prefix on routing key with default exchange.
	if exchangeName == "" && strings.HasPrefix(routingKey, "$queue/") {
		qm := ch.conn.broker.getQueueManager()
		if qm != nil {
			err := qm.Publish(context.Background(), qtypes.PublishRequest{
				Topic:      routingKey,
				Payload:    body,
				Properties: props,
			})
			if err != nil {
				ch.conn.logger.Error("queue publish failed", "queue", routingKey, "error", err)
			}
			if ch.confirmMode {
				if err != nil {
					ch.sendPublisherNack()
				} else {
					ch.sendPublisherAck()
				}
			}
			return
		}
	}

	// RabbitMQ-style stream queue publish: default exchange with routingKey == queue name.
	if exchangeName == "" && ch.isStreamQueue(routingKey) {
		qm := ch.conn.broker.getQueueManager()
		if qm != nil {
			queueTopic := "$queue/" + routingKey
			err := qm.Publish(context.Background(), qtypes.PublishRequest{
				Topic:      queueTopic,
				Payload:    body,
				Properties: props,
			})
			if err != nil {
				ch.conn.logger.Error("queue publish failed", "queue", routingKey, "error", err)
			}
			if ch.confirmMode {
				if err != nil {
					ch.sendPublisherNack()
				} else {
					ch.sendPublisherAck()
				}
			}
			return
		}
	}

	// Check if this targets a queue via exchange bindings
	isQueuePublish := false
	var publishFailed bool
	ch.exchangeMu.RLock()
	bindings := make([]binding, 0, len(ch.bindings))
	for _, b := range ch.bindings {
		if b.exchange == exchangeName {
			bindings = append(bindings, b)
		}
	}
	ch.exchangeMu.RUnlock()

	for _, b := range bindings {
		if ch.routingKeyMatches(b.routingKey, routingKey, exchangeName) {
			// Route to the bound queue
			qm := ch.conn.broker.getQueueManager()
			if qm != nil {
				queueTopic := "$queue/" + b.queue
				if routingKey != "" {
					queueTopic = queueTopic + "/" + routingKey
				}
				if err := qm.Publish(context.Background(), qtypes.PublishRequest{
					Topic:      queueTopic,
					Payload:    body,
					Properties: props,
				}); err != nil {
					ch.conn.logger.Error("queue publish failed", "queue", b.queue, "error", err)
					publishFailed = true
				}
			}
			isQueuePublish = true
		}
	}

	if !isQueuePublish {
		// Publish to the topic-based router (pub/sub)
		if method.Mandatory {
			subs, err := ch.conn.broker.router.Match(topic)
			if err != nil || len(subs) == 0 {
				if err != nil {
					ch.conn.logger.Error("router match failed", "topic", topic, "error", err)
				}
				ch.sendBasicReturn(method, header, body)
				if ch.confirmMode {
					ch.sendPublisherAck()
				}
				return
			}
		}
		ch.conn.broker.Publish(topic, body, props)
	}

	// Publisher confirms
	if ch.confirmMode {
		if publishFailed {
			ch.sendPublisherNack()
		} else {
			ch.sendPublisherAck()
		}
	}
}

func (ch *Channel) routingKeyMatches(bindingKey, routingKey, exchangeName string) bool {
	ch.exchangeMu.RLock()
	ex := ch.exchanges[exchangeName]
	ch.exchangeMu.RUnlock()

	if ex == nil {
		// Default exchange: routing key = queue name
		return bindingKey == routingKey
	}

	switch ex.typ {
	case "fanout":
		return true
	case "direct", "":
		return bindingKey == routingKey
	case "topic":
		return topics.TopicMatch(bindingKey, routingKey)
	default:
		return bindingKey == routingKey
	}
}

func normalizeExchange(name string) string {
	if name == "amq.default" {
		return ""
	}
	return name
}

func (ch *Channel) exchangeExists(name string) bool {
	if name == "" {
		return true
	}
	ch.exchangeMu.RLock()
	_, exists := ch.exchanges[name]
	ch.exchangeMu.RUnlock()
	return exists
}

func (ch *Channel) resetPendingPublish() {
	ch.pendingMethod = nil
	ch.pendingHeader = nil
	ch.pendingBody = nil
	ch.pendingBodySize = 0
	ch.pendingBodyReceived = 0
}

func (ch *Channel) shouldQueueDelivery(cons *consumer) bool {
	if !ch.flow {
		return true
	}
	if cons.noAck {
		return false
	}
	if ch.prefetchCount == 0 {
		return false
	}
	ch.unackedMu.Lock()
	unacked := len(ch.unacked)
	ch.unackedMu.Unlock()
	return unacked >= int(ch.prefetchCount)
}

func (ch *Channel) enqueueDelivery(cons *consumer, topic string, payload []byte, props map[string]string) {
	cpPayload := make([]byte, len(payload))
	copy(cpPayload, payload)

	cpProps := make(map[string]string, len(props))
	for k, v := range props {
		cpProps[k] = v
	}

	ch.pendingMu.Lock()
	ch.pendingDeliveries = append(ch.pendingDeliveries, pendingDelivery{
		consumerTag: cons.tag,
		queue:       cons.queue,
		topic:       topic,
		payload:     cpPayload,
		props:       cpProps,
	})
	ch.pendingMu.Unlock()
}

func (ch *Channel) drainPending() {
	for {
		if ch.closed.Load() {
			return
		}
		if !ch.flow {
			return
		}
		if ch.prefetchCount > 0 {
			ch.unackedMu.Lock()
			unacked := len(ch.unacked)
			ch.unackedMu.Unlock()
			if unacked >= int(ch.prefetchCount) {
				return
			}
		}

		ch.pendingMu.Lock()
		if len(ch.pendingDeliveries) == 0 {
			ch.pendingMu.Unlock()
			return
		}
		pd := ch.pendingDeliveries[0]
		ch.pendingDeliveries = ch.pendingDeliveries[1:]
		ch.pendingMu.Unlock()

		ch.consumersMu.RLock()
		cons, ok := ch.consumers[pd.consumerTag]
		ch.consumersMu.RUnlock()
		if !ok || cons.queue != pd.queue {
			continue
		}
		if ch.shouldQueueDelivery(cons) {
			ch.pendingMu.Lock()
			ch.pendingDeliveries = append([]pendingDelivery{pd}, ch.pendingDeliveries...)
			ch.pendingMu.Unlock()
			return
		}
		if err := ch.sendDelivery(cons, pd.topic, pd.payload, pd.props); err != nil {
			ch.conn.logger.Error("failed to deliver queued message", "error", err)
			return
		}
	}
}

func (ch *Channel) sendDelivery(cons *consumer, topic string, payload []byte, props map[string]string) error {
	deliveryTag := ch.conn.nextDeliveryTag()

	if !cons.noAck {
		ch.unackedMu.Lock()
		ch.unacked[deliveryTag] = &unackedDelivery{
			deliveryTag: deliveryTag,
			routingKey:  topic,
			queueName:   cons.queueName,
			messageID:   props[qtypes.PropMessageID],
			groupID:     props[qtypes.PropGroupID],
		}
		ch.unackedMu.Unlock()
	}

	exchange := ""
	routingKey := topic
	if idx := strings.Index(topic, "/"); idx >= 0 {
		exchange = topic[:idx]
		routingKey = topic[idx+1:]
	}

	deliver := &codec.BasicDeliver{
		ConsumerTag: cons.tag,
		DeliveryTag: deliveryTag,
		Redelivered: false,
		Exchange:    exchange,
		RoutingKey:  routingKey,
	}
	methodBuf := bufpool.Get()
	defer bufpool.Put(methodBuf)
	methodFrame, err := buildMethodFrame(methodBuf, ch.id, deliver)
	if err != nil {
		return err
	}

	headers := make(map[string]interface{})
	for k, v := range props {
		switch k {
		case "content-type", "content-encoding", "correlation-id", "reply-to", qtypes.PropMessageID, "type":
			continue
		case qtypes.PropStreamOffset, qtypes.PropWorkCommittedOffset:
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				headers[k] = int64(n) // AMQP uses signed integers
			} else {
				headers[k] = v
			}
		case qtypes.PropStreamTimestamp:
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				headers[k] = n
			} else {
				headers[k] = v
			}
		case qtypes.PropWorkAcked:
			headers[k] = v == "true"
		default:
			headers[k] = v
		}
	}
	if len(headers) == 0 {
		headers = nil
	}

	correlationID := props["correlation-id"]
	if decoded, err := base64.StdEncoding.DecodeString(correlationID); err == nil {
		correlationID = string(decoded)
	}

	properties := codec.BasicProperties{
		ContentType:   props["content-type"],
		CorrelationID: correlationID,
		ReplyTo:       props["reply-to"],
		MessageID:     props[qtypes.PropMessageID],
		Type:          props["type"],
		Headers:       headers,
	}

	headerBuf := bufpool.Get()
	defer bufpool.Put(headerBuf)
	headerFrame, err := buildContentHeaderFrame(headerBuf, ch.id, uint64(len(payload)), properties)
	if err != nil {
		return err
	}

	bodyFrames := buildBodyFrames(ch.id, payload, ch.conn.frameMax)
	frames := append([]*codec.Frame{methodFrame, headerFrame}, bodyFrames...)
	if err := ch.conn.writeFrames(frames...); err != nil {
		return err
	}

	ch.conn.broker.stats.IncrementMessagesSent()
	ch.conn.broker.stats.AddBytesSent(uint64(len(payload)))
	return nil
}

func (ch *Channel) sendBasicReturn(method *codec.BasicPublish, header *codec.ContentHeader, body []byte) {
	ret := &codec.BasicReturn{
		ReplyCode:  codec.NoRoute,
		ReplyText:  "NO_ROUTE",
		Exchange:   normalizeExchange(method.Exchange),
		RoutingKey: method.RoutingKey,
	}
	methodBuf := bufpool.Get()
	defer bufpool.Put(methodBuf)
	methodFrame, err := buildMethodFrame(methodBuf, ch.id, ret)
	if err != nil {
		ch.conn.logger.Error("failed to write basic.return", "error", err)
		return
	}
	headerBuf := bufpool.Get()
	defer bufpool.Put(headerBuf)
	headerFrame, err := buildContentHeaderFrame(headerBuf, ch.id, uint64(len(body)), header.Properties)
	if err != nil {
		ch.conn.logger.Error("failed to write return header", "error", err)
		return
	}
	bodyFrames := buildBodyFrames(ch.id, body, ch.conn.frameMax)
	frames := append([]*codec.Frame{methodFrame, headerFrame}, bodyFrames...)
	if err := ch.conn.writeFrames(frames...); err != nil {
		ch.conn.logger.Error("failed to write return frames", "error", err)
		return
	}
}

func (ch *Channel) sendPublisherAck() {
	seq := ch.publishSeq.Add(1)
	ack := &codec.BasicAck{
		DeliveryTag: seq,
		Multiple:    false,
	}
	if err := ch.conn.writeMethod(ch.id, ack); err != nil {
		ch.conn.logger.Error("failed to write publisher ack", "error", err)
	}
}

func (ch *Channel) sendPublisherNack() {
	seq := ch.publishSeq.Add(1)
	nack := &codec.BasicNack{
		DeliveryTag: seq,
		Multiple:    false,
		Requeue:     false,
	}
	if err := ch.conn.writeMethod(ch.id, nack); err != nil {
		ch.conn.logger.Error("failed to write publisher nack", "error", err)
	}
}

func buildMethodFrame(buf *bytes.Buffer, channel uint16, method interface{ Write(io.Writer) error }) (*codec.Frame, error) {
	buf.Reset()
	if err := method.Write(buf); err != nil {
		return nil, err
	}
	return &codec.Frame{
		Type:    codec.FrameMethod,
		Channel: channel,
		Payload: buf.Bytes(),
	}, nil
}

func buildContentHeaderFrame(buf *bytes.Buffer, channel uint16, bodySize uint64, props codec.BasicProperties) (*codec.Frame, error) {
	buf.Reset()
	header := &codec.ContentHeader{
		ClassID:    codec.ClassBasic,
		Weight:     0,
		BodySize:   bodySize,
		Properties: props,
	}
	if err := header.WriteContentHeader(buf); err != nil {
		return nil, err
	}
	return &codec.Frame{
		Type:    codec.FrameHeader,
		Channel: channel,
		Payload: buf.Bytes(),
	}, nil
}

func buildBodyFrames(channel uint16, payload []byte, frameMax uint32) []*codec.Frame {
	maxBody := int(frameMax) - 8 // frame overhead
	if maxBody <= 0 {
		maxBody = len(payload)
	}
	var frames []*codec.Frame
	for offset := 0; offset < len(payload) || offset == 0; {
		end := offset + maxBody
		if end > len(payload) {
			end = len(payload)
		}
		frames = append(frames, &codec.Frame{
			Type:    codec.FrameBody,
			Channel: channel,
			Payload: payload[offset:end],
		})
		offset = end
		if offset == 0 {
			break // empty payload
		}
	}
	return frames
}

// Exchange methods

func (ch *Channel) handleExchangeDeclare(m *codec.ExchangeDeclare) error {
	m.Exchange = normalizeExchange(m.Exchange)
	if m.Passive {
		if !ch.exchangeExists(m.Exchange) {
			return ch.conn.sendChannelClose(ch.id, codec.NotFound,
				"exchange not found", codec.ClassExchange, codec.MethodExchangeDeclare)
		}
		if !m.NoWait {
			return ch.conn.writeMethod(ch.id, &codec.ExchangeDeclareOk{})
		}
		return nil
	}

	ch.exchangeMu.Lock()
	ch.exchanges[m.Exchange] = &exchange{
		name:       m.Exchange,
		typ:        m.Type,
		durable:    m.Durable,
		autoDelete: m.AutoDelete,
		internal:   m.Internal,
	}
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.ExchangeDeclareOk{})
	}
	return nil
}

func (ch *Channel) handleExchangeDelete(m *codec.ExchangeDelete) error {
	m.Exchange = normalizeExchange(m.Exchange)
	ch.exchangeMu.Lock()
	delete(ch.exchanges, m.Exchange)
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.ExchangeDeleteOk{})
	}
	return nil
}

func (ch *Channel) handleExchangeBind(m *codec.ExchangeBind) error {
	m.Source = normalizeExchange(m.Source)
	m.Destination = normalizeExchange(m.Destination)
	ch.exchangeMu.Lock()
	ch.bindings = append(ch.bindings, binding{
		queue:      m.Destination,
		exchange:   m.Source,
		routingKey: m.RoutingKey,
		arguments:  m.Arguments,
	})
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.ExchangeBindOk{})
	}
	return nil
}

func (ch *Channel) handleExchangeUnbind(m *codec.ExchangeUnbind) error {
	m.Source = normalizeExchange(m.Source)
	m.Destination = normalizeExchange(m.Destination)
	ch.exchangeMu.Lock()
	filtered := ch.bindings[:0]
	for _, b := range ch.bindings {
		if !(b.exchange == m.Source && b.queue == m.Destination && b.routingKey == m.RoutingKey) {
			filtered = append(filtered, b)
		}
	}
	ch.bindings = filtered
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.ExchangeUnbindOk{})
	}
	return nil
}

// Queue methods

func (ch *Channel) handleQueueDeclare(m *codec.QueueDeclare) error {
	if m.Passive {
		ch.exchangeMu.RLock()
		_, exists := ch.queues[m.Queue]
		ch.exchangeMu.RUnlock()
		if !exists {
			return ch.conn.sendChannelClose(ch.id, codec.NotFound,
				"queue not found", codec.ClassQueue, codec.MethodQueueDeclare)
		}
		if !m.NoWait {
			return ch.conn.writeMethod(ch.id, &codec.QueueDeclareOk{
				Queue:         m.Queue,
				MessageCount:  0,
				ConsumerCount: 0,
			})
		}
		return nil
	}

	if m.Queue == "" {
		seq := ch.queueSeq.Add(1)
		m.Queue = fmt.Sprintf("amq.gen-%s-%d", ch.conn.connID, seq)
	}

	queueType := extractQueueType(m.Arguments)

	ch.exchangeMu.Lock()
	ch.queues[m.Queue] = &queueInfo{
		name:      m.Queue,
		queueType: queueType,
		args:      m.Arguments,
	}
	ch.exchangeMu.Unlock()

	if queueType == string(qtypes.QueueTypeStream) {
		qm := ch.conn.broker.getQueueManager()
		if qm != nil {
			queueTopicPattern := "$queue/" + m.Queue + "/#"
			var cfg qtypes.QueueConfig
			if m.Durable {
				cfg = qtypes.DefaultQueueConfig(m.Queue, queueTopicPattern)
			} else {
				cfg = qtypes.DefaultEphemeralQueueConfig(m.Queue, queueTopicPattern)
			}
			cfg.Type = qtypes.QueueTypeStream
			cfg.Retention = extractStreamRetention(m.Arguments)
			if err := qm.CreateQueue(context.Background(), cfg); err != nil {
				// If it already exists, attempt to update retention/type only.
				if existing, err := qm.GetQueue(context.Background(), m.Queue); err == nil && existing != nil {
					existing.Type = qtypes.QueueTypeStream
					existing.Retention = cfg.Retention
					if !m.Durable {
						existing.Durable = false
						if existing.ExpiresAfter == 0 {
							existing.ExpiresAfter = 5 * time.Minute
						}
					}
					if err := qm.UpdateQueue(context.Background(), *existing); err != nil {
						ch.conn.logger.Warn("failed to update stream queue config", "queue", m.Queue, "error", err)
					}
				}
			}
		}
	}

	// Auto-bind queue to default exchange with routing key = queue name
	ch.exchangeMu.Lock()
	ch.bindings = append(ch.bindings, binding{
		queue:      m.Queue,
		exchange:   "",
		routingKey: m.Queue,
	})
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.QueueDeclareOk{
			Queue:         m.Queue,
			MessageCount:  0,
			ConsumerCount: 0,
		})
	}
	return nil
}

func (ch *Channel) handleQueueBind(m *codec.QueueBind) error {
	m.Exchange = normalizeExchange(m.Exchange)
	ch.exchangeMu.Lock()
	ch.bindings = append(ch.bindings, binding{
		queue:      m.Queue,
		exchange:   m.Exchange,
		routingKey: m.RoutingKey,
		arguments:  m.Arguments,
	})
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.QueueBindOk{})
	}
	return nil
}

func (ch *Channel) handleQueueUnbind(m *codec.QueueUnbind) error {
	m.Exchange = normalizeExchange(m.Exchange)
	ch.exchangeMu.Lock()
	filtered := ch.bindings[:0]
	for _, b := range ch.bindings {
		if !(b.queue == m.Queue && b.exchange == m.Exchange && b.routingKey == m.RoutingKey) {
			filtered = append(filtered, b)
		}
	}
	ch.bindings = filtered
	ch.exchangeMu.Unlock()

	return ch.conn.writeMethod(ch.id, &codec.QueueUnbindOk{})
}

func (ch *Channel) handleQueuePurge(m *codec.QueuePurge) error {
	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.QueuePurgeOk{MessageCount: 0})
	}
	return nil
}

func (ch *Channel) handleQueueDelete(m *codec.QueueDelete) error {
	ch.exchangeMu.Lock()
	delete(ch.queues, m.Queue)
	// Remove bindings for this queue
	filtered := ch.bindings[:0]
	for _, b := range ch.bindings {
		if b.queue != m.Queue {
			filtered = append(filtered, b)
		}
	}
	ch.bindings = filtered
	ch.exchangeMu.Unlock()

	// Unsubscribe from queue manager
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		clientID := PrefixedClientID(ch.conn.connID)
		qm.Unsubscribe(context.Background(), m.Queue, "", clientID, "")
	}

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.QueueDeleteOk{MessageCount: 0})
	}
	return nil
}

// Basic methods

func (ch *Channel) handleBasicQos(m *codec.BasicQos) error {
	ch.prefetchCount = m.PrefetchCount
	ch.prefetchSize = m.PrefetchSize
	return ch.conn.writeMethod(ch.id, &codec.BasicQosOk{})
}

func (ch *Channel) handleBasicConsume(m *codec.BasicConsume) error {
	tag := m.ConsumerTag
	if tag == "" {
		tag = fmt.Sprintf("ctag-%s-%d", ch.conn.connID, ch.nextTag.Add(1))
	}

	queueFilter := m.Queue
	queueName, pattern := "", ""
	isQueue := isQueueTopic(queueFilter)
	if isQueue {
		queueName, pattern = parseQueueFilter(queueFilter)
	}

	queueInfo := ch.getQueueInfo(queueFilter)
	streamCursor, hasStreamOffset := extractStreamOffset(m.Arguments)
	isStream := (queueInfo != nil && queueInfo.queueType == string(qtypes.QueueTypeStream)) || hasStreamOffset
	if isStream && !isQueue {
		queueName = queueFilter
		pattern = ""
	}

	groupID := extractConsumerGroup(m.Arguments)
	if isStream && groupID == "" {
		groupID = tag
	}

	ch.consumersMu.Lock()
	if _, exists := ch.consumers[tag]; exists {
		ch.consumersMu.Unlock()
		return ch.conn.sendChannelClose(ch.id, codec.NotAllowed,
			fmt.Sprintf("consumer tag %q already exists", tag), codec.ClassBasic, codec.MethodBasicConsume)
	}
	ch.consumers[tag] = &consumer{
		tag:       tag,
		queue:     queueFilter,
		queueName: queueName,
		pattern:   pattern,
		groupID:   groupID,
		noAck:     m.NoAck,
		exclusive: m.Exclusive,
	}
	ch.consumersMu.Unlock()

	ch.conn.broker.stats.IncrementConsumers()

	// Subscribe to the queue via queue manager
	qm := ch.conn.broker.getQueueManager()
	if qm != nil && (isQueue || isStream) && queueName != "" {
		clientID := PrefixedClientID(ch.conn.connID)
		subGroupID := groupID
		if isStream {
			cursor := streamCursor
			if cursor == nil {
				cursor = &qtypes.CursorOption{Position: qtypes.CursorLatest}
			}
			cursor.Mode = qtypes.GroupModeStream
			if autoCommit := extractAutoCommit(m.Arguments); autoCommit != nil {
				cursor.AutoCommit = autoCommit
			}
			if err := qm.SubscribeWithCursor(context.Background(), queueName, pattern, clientID, subGroupID, "", cursor); err != nil {
				ch.conn.logger.Error("queue subscribe with cursor failed", "queue", queueName, "error", err)
			}
		} else if err := qm.Subscribe(context.Background(), queueName, pattern, clientID, subGroupID, ""); err != nil {
			ch.conn.logger.Error("queue subscribe failed", "queue", queueName, "error", err)
		}
	}

	// Subscribe via the topic router for pub/sub delivery (non-queue topics).
	if !isQueue && !isStream {
		connID := ch.conn.connID
		ch.conn.broker.router.Subscribe(connID, queueFilter, 1, storage.SubscribeOptions{})
	}

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.BasicConsumeOk{ConsumerTag: tag})
	}
	return nil
}

func (ch *Channel) handleBasicCancel(m *codec.BasicCancel) error {
	ch.consumersMu.Lock()
	cons, exists := ch.consumers[m.ConsumerTag]
	delete(ch.consumers, m.ConsumerTag)
	ch.consumersMu.Unlock()

	if exists {
		ch.conn.broker.stats.DecrementConsumers()

		// Unsubscribe from queue manager
		qm := ch.conn.broker.getQueueManager()
		if qm != nil && cons.queueName != "" {
			clientID := PrefixedClientID(ch.conn.connID)
			qm.Unsubscribe(context.Background(), cons.queueName, cons.pattern, clientID, cons.groupID)
		}

		if cons.queueName == "" {
			ch.conn.broker.router.Unsubscribe(ch.conn.connID, cons.queue)
		}
	}

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.BasicCancelOk{ConsumerTag: m.ConsumerTag})
	}
	return nil
}

func (ch *Channel) handleBasicGet(m *codec.BasicGet) error {
	// For now, return GetEmpty since we use push-based delivery
	return ch.conn.writeMethod(ch.id, &codec.BasicGetEmpty{})
}

func (ch *Channel) handleBasicAck(m *codec.BasicAck) error {
	var deliveries []*unackedDelivery
	ch.unackedMu.Lock()
	if m.Multiple {
		for tag, ud := range ch.unacked {
			if tag <= m.DeliveryTag {
				deliveries = append(deliveries, ud)
				delete(ch.unacked, tag)
			}
		}
	} else {
		if ud, ok := ch.unacked[m.DeliveryTag]; ok {
			deliveries = append(deliveries, ud)
			delete(ch.unacked, m.DeliveryTag)
		}
	}
	ch.unackedMu.Unlock()
	for _, ud := range deliveries {
		ch.ackDelivery(ud)
	}
	ch.drainPending()
	return nil
}

func (ch *Channel) handleBasicReject(m *codec.BasicReject) error {
	var deliveries []*unackedDelivery
	var requeue bool
	ch.unackedMu.Lock()
	if ud, ok := ch.unacked[m.DeliveryTag]; ok {
		deliveries = append(deliveries, ud)
		requeue = m.Requeue
		delete(ch.unacked, m.DeliveryTag)
	}
	ch.unackedMu.Unlock()
	for _, ud := range deliveries {
		if requeue {
			ch.nackDelivery(ud)
		} else {
			ch.rejectDelivery(ud)
		}
	}
	ch.drainPending()
	return nil
}

func (ch *Channel) handleBasicNack(m *codec.BasicNack) error {
	var deliveries []*unackedDelivery
	var requeue bool
	ch.unackedMu.Lock()
	if m.Multiple {
		for tag, ud := range ch.unacked {
			if tag <= m.DeliveryTag {
				deliveries = append(deliveries, ud)
				delete(ch.unacked, tag)
			}
		}
	} else {
		if ud, ok := ch.unacked[m.DeliveryTag]; ok {
			deliveries = append(deliveries, ud)
			delete(ch.unacked, m.DeliveryTag)
		}
	}
	ch.unackedMu.Unlock()
	requeue = m.Requeue
	for _, ud := range deliveries {
		if requeue {
			ch.nackDelivery(ud)
		} else {
			ch.rejectDelivery(ud)
		}
	}
	ch.drainPending()
	return nil
}

func (ch *Channel) handleBasicRecover(_ *codec.BasicRecover) error {
	// Redeliver all unacked messages - simplified: just ack them
	return ch.conn.writeMethod(ch.id, &codec.BasicRecoverOk{})
}

func (ch *Channel) handleBasicRecoverAsync(_ *codec.BasicRecoverAsync) error {
	return nil
}

func (ch *Channel) ackDelivery(ud *unackedDelivery) {
	if ud.messageID == "" {
		return
	}
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		if err := qm.Ack(context.Background(), ud.queueName, ud.messageID, ud.groupID); err != nil {
			ch.conn.logger.Warn("queue ack failed", "queue", ud.queueName, "message_id", ud.messageID, "group_id", ud.groupID, "error", err)
		}
	}
}

func (ch *Channel) nackDelivery(ud *unackedDelivery) {
	if ud.messageID == "" {
		return
	}
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		if err := qm.Nack(context.Background(), ud.queueName, ud.messageID, ud.groupID); err != nil {
			ch.conn.logger.Warn("queue nack failed", "queue", ud.queueName, "message_id", ud.messageID, "group_id", ud.groupID, "error", err)
		}
	}
}

func (ch *Channel) rejectDelivery(ud *unackedDelivery) {
	if ud.messageID == "" {
		return
	}
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		if err := qm.Reject(context.Background(), ud.queueName, ud.messageID, ud.groupID, "rejected by client"); err != nil {
			ch.conn.logger.Warn("queue reject failed", "queue", ud.queueName, "message_id", ud.messageID, "group_id", ud.groupID, "error", err)
		}
	}
}

// deliverMessage delivers a message to all consumers on this channel whose queue matches the topic.
func (ch *Channel) deliverMessage(topic string, payload []byte, props map[string]string) {
	if ch.closed.Load() {
		return
	}

	ch.consumersMu.RLock()
	consumers := make([]*consumer, 0, len(ch.consumers))
	for _, cons := range ch.consumers {
		consumers = append(consumers, cons)
	}
	ch.consumersMu.RUnlock()

	for _, cons := range consumers {
		if !ch.consumerQueueMatches(cons, topic) {
			continue
		}

		if ch.shouldQueueDelivery(cons) {
			ch.enqueueDelivery(cons, topic, payload, props)
			continue
		}

		if err := ch.sendDelivery(cons, topic, payload, props); err != nil {
			ch.conn.logger.Error("failed to deliver message", "error", err)
			return
		}
	}
}

// consumerQueueMatches checks if a consumer's queue matches the given topic.
func (ch *Channel) consumerQueueMatches(cons *consumer, topic string) bool {
	if cons.queueName != "" {
		queueTopic := "$queue/" + cons.queueName
		switch {
		case topic == cons.queueName, topic == queueTopic:
			// Empty pattern means the root queue topic.
			return cons.pattern == "" || cons.pattern == "#"
		case strings.HasPrefix(topic, queueTopic+"/"):
			if cons.pattern == "" {
				return true
			}
			routingKey := strings.TrimPrefix(topic, queueTopic+"/")
			return topics.TopicMatch(cons.pattern, routingKey)
		}
	}

	if cons.queue == topic {
		return true
	}
	return topics.TopicMatch(cons.queue, topic)
}

// cleanup releases all resources held by this channel.
func (ch *Channel) cleanup() {
	ch.closed.Store(true)

	ch.consumersMu.Lock()
	consumers := make([]*consumer, 0, len(ch.consumers))
	for _, c := range ch.consumers {
		consumers = append(consumers, c)
	}
	ch.consumers = make(map[string]*consumer)
	ch.consumersMu.Unlock()

	qm := ch.conn.broker.getQueueManager()
	clientID := PrefixedClientID(ch.conn.connID)

	for _, cons := range consumers {
		ch.conn.broker.stats.DecrementConsumers()
		if qm != nil && cons.queueName != "" {
			qm.Unsubscribe(context.Background(), cons.queueName, cons.pattern, clientID, cons.groupID)
		}
		if cons.queueName == "" {
			ch.conn.broker.router.Unsubscribe(ch.conn.connID, cons.queue)
		}
	}
}

func isQueueTopic(topic string) bool {
	return strings.HasPrefix(topic, "$queue/")
}

func parseQueueFilter(filter string) (queueName, pattern string) {
	if !strings.HasPrefix(filter, "$queue/") {
		return "", ""
	}

	rest := strings.TrimPrefix(filter, "$queue/")
	if rest == "" {
		return "", ""
	}

	parts := strings.SplitN(rest, "/", 2)
	queueName = parts[0]

	if len(parts) > 1 {
		pattern = parts[1]
	}

	return queueName, pattern
}

func extractConsumerGroup(args map[string]interface{}) string {
	if len(args) == 0 {
		return ""
	}

	val, ok := args["x-consumer-group"]
	if !ok {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (ch *Channel) getQueueInfo(name string) *queueInfo {
	ch.exchangeMu.RLock()
	defer ch.exchangeMu.RUnlock()
	return ch.queues[name]
}

func (ch *Channel) isStreamQueue(name string) bool {
	if info := ch.getQueueInfo(name); info != nil && info.queueType == string(qtypes.QueueTypeStream) {
		return true
	}
	if strings.HasPrefix(name, "$queue/") {
		base := strings.TrimPrefix(name, "$queue/")
		if info := ch.getQueueInfo(base); info != nil && info.queueType == string(qtypes.QueueTypeStream) {
			return true
		}
	}
	return false
}

func extractQueueType(args map[string]interface{}) string {
	if len(args) == 0 {
		return string(qtypes.QueueTypeClassic)
	}
	val, ok := args["x-queue-type"]
	if !ok {
		return string(qtypes.QueueTypeClassic)
	}
	switch v := val.(type) {
	case string:
		if v == "" {
			return string(qtypes.QueueTypeClassic)
		}
		return strings.ToLower(v)
	case []byte:
		if len(v) == 0 {
			return string(qtypes.QueueTypeClassic)
		}
		return strings.ToLower(string(v))
	default:
		return string(qtypes.QueueTypeClassic)
	}
}

func extractStreamRetention(args map[string]interface{}) qtypes.RetentionPolicy {
	var policy qtypes.RetentionPolicy
	if len(args) == 0 {
		return policy
	}

	if val, ok := args["x-max-age"]; ok {
		if d, ok := parseDurationArg(val); ok {
			policy.RetentionTime = d
		}
	}
	if val, ok := args["x-max-length-bytes"]; ok {
		if n, ok := parseInt64Arg(val); ok {
			policy.RetentionBytes = n
		}
	}
	if val, ok := args["x-max-length"]; ok {
		if n, ok := parseInt64Arg(val); ok {
			policy.RetentionMessages = n
		}
	}

	return policy
}

func extractStreamOffset(args map[string]interface{}) (*qtypes.CursorOption, bool) {
	if len(args) == 0 {
		return nil, false
	}
	val, ok := args[qtypes.PropStreamOffset]
	if !ok {
		return nil, false
	}

	switch v := val.(type) {
	case string:
		return parseStreamOffsetString(v)
	case []byte:
		return parseStreamOffsetString(string(v))
	case int:
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: uint64(v)}, true
	case int64:
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: uint64(v)}, true
	case uint64:
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: v}, true
	case uint32:
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: uint64(v)}, true
	case time.Time:
		return &qtypes.CursorOption{Position: qtypes.CursorTimestamp, Timestamp: v}, true
	default:
		return nil, false
	}
}

func parseStreamOffsetString(val string) (*qtypes.CursorOption, bool) {
	if val == "" {
		return nil, false
	}
	v := strings.ToLower(strings.TrimSpace(val))
	switch v {
	case "first":
		return &qtypes.CursorOption{Position: qtypes.CursorEarliest}, true
	case "last", "next":
		return &qtypes.CursorOption{Position: qtypes.CursorLatest}, true
	}

	if strings.HasPrefix(v, "offset=") {
		v = strings.TrimPrefix(v, "offset=")
	}
	if strings.HasPrefix(v, "timestamp=") {
		raw := strings.TrimPrefix(v, "timestamp=")
		if ts, ok := parseUnixTimestamp(raw); ok {
			return &qtypes.CursorOption{Position: qtypes.CursorTimestamp, Timestamp: ts}, true
		}
	}

	if off, err := strconv.ParseUint(v, 10, 64); err == nil {
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: off}, true
	}

	return nil, false
}

func parseUnixTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	if val > 1e12 {
		return time.UnixMilli(val), true
	}
	return time.Unix(val, 0), true
}

func parseDurationArg(val any) (time.Duration, bool) {
	switch v := val.(type) {
	case time.Duration:
		return v, true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		if d, err := time.ParseDuration(trimmed); err == nil {
			return d, true
		}
		upper := strings.ToUpper(trimmed)
		if strings.HasSuffix(upper, "D") {
			num := strings.TrimSuffix(upper, "D")
			if f, err := strconv.ParseFloat(num, 64); err == nil {
				return time.Duration(f * float64(24*time.Hour)), true
			}
		}
		if strings.HasSuffix(upper, "W") {
			num := strings.TrimSuffix(upper, "W")
			if f, err := strconv.ParseFloat(num, 64); err == nil {
				return time.Duration(f * float64(7*24*time.Hour)), true
			}
		}
	case int:
		return time.Duration(v) * time.Second, true
	case int64:
		return time.Duration(v) * time.Second, true
	case uint64:
		return time.Duration(v) * time.Second, true
	}
	return 0, false
}

func parseInt64Arg(val any) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case uint64:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case uint32:
		return int64(v), true
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return n, true
		}
	case []byte:
		if n, err := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

func parseStringArg(val any) (string, bool) {
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v), true
	case []byte:
		return strings.TrimSpace(string(v)), true
	}
	return "", false
}

func extractAutoCommit(args map[string]interface{}) *bool {
	if len(args) == 0 {
		return nil
	}
	val, ok := args["x-auto-commit"]
	if !ok {
		return nil
	}
	switch v := val.(type) {
	case bool:
		return &v
	case string:
		b := v == "true"
		return &b
	}
	return nil
}
