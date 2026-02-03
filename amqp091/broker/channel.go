// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/amqp091/codec"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

// consumer represents a Basic.Consume subscription on a channel.
type consumer struct {
	tag       string
	queue     string
	noAck     bool
	exclusive bool
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
	queues     map[string]bool // set of declared queue names
	bindings   []binding
	exchangeMu sync.RWMutex

	// Consumer management
	consumers   map[string]*consumer // tag -> consumer
	consumersMu sync.RWMutex
	nextTag     atomic.Uint64

	// Content accumulation state machine for incoming publishes
	pendingHeader *codec.ContentHeader
	pendingBody   []byte
	pendingMethod *codec.BasicPublish

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

	closed atomic.Bool
}

type unackedDelivery struct {
	deliveryTag uint64
	exchange    string
	routingKey  string
	queueName   string
	messageID   string
	groupID     string
}

func newChannel(c *Connection, id uint16) *Channel {
	return &Channel{
		conn:      c,
		id:        id,
		exchanges: make(map[string]*exchange),
		queues:    make(map[string]bool),
		consumers: make(map[string]*consumer),
		unacked:   make(map[uint64]*unackedDelivery),
		flow:      true,
	}
}

func (ch *Channel) handleMethod(decoded interface{}) error {
	switch m := decoded.(type) {
	// Channel
	case *codec.ChannelFlow:
		ch.flow = m.Active
		return ch.conn.writeMethod(ch.id, &codec.ChannelFlowOk{Active: m.Active})
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
	r := bytes.NewReader(frame.Payload)
	header, err := codec.ReadContentHeader(r)
	if err != nil {
		ch.conn.logger.Error("failed to read content header", "error", err)
		return
	}
	ch.pendingHeader = header
	ch.pendingBody = make([]byte, 0, header.BodySize)

	// If body size is 0, the message is complete
	if header.BodySize == 0 {
		ch.completePublish()
	}
}

// handleBodyFrame processes a content body frame.
func (ch *Channel) handleBodyFrame(frame *codec.Frame) {
	ch.pendingBody = append(ch.pendingBody, frame.Payload...)

	if ch.pendingHeader != nil && uint64(len(ch.pendingBody)) >= ch.pendingHeader.BodySize {
		ch.completePublish()
	}
}

// completePublish is called when all content frames for a publish have arrived.
func (ch *Channel) completePublish() {
	method := ch.pendingMethod
	header := ch.pendingHeader
	body := ch.pendingBody

	ch.pendingMethod = nil
	ch.pendingHeader = nil
	ch.pendingBody = nil

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
		props["correlation-id"] = header.Properties.CorrelationID
	}
	if header.Properties.ReplyTo != "" {
		props["reply-to"] = header.Properties.ReplyTo
	}
	if header.Properties.MessageID != "" {
		props["message-id"] = header.Properties.MessageID
	}
	if header.Properties.Type != "" {
		props["type"] = header.Properties.Type
	}

	exchangeName := method.Exchange
	routingKey := method.RoutingKey

	// Determine the topic from exchange+routingKey
	topic := routingKey
	if exchangeName != "" && exchangeName != "amq.default" {
		topic = exchangeName + "/" + routingKey
	}

	// Check if this targets a queue via exchange bindings
	isQueuePublish := false
	ch.exchangeMu.RLock()
	for _, b := range ch.bindings {
		if b.exchange == exchangeName && ch.routingKeyMatches(b.routingKey, routingKey, exchangeName) {
			// Route to the bound queue
			qm := ch.conn.broker.getQueueManager()
			if qm != nil {
				queueTopic := "$queue/" + b.queue
				if routingKey != "" {
					queueTopic = queueTopic + "/" + routingKey
				}
				if err := qm.Publish(context.Background(), queueTopic, body, props); err != nil {
					ch.conn.logger.Error("queue publish failed", "queue", b.queue, "error", err)
				}
			}
			isQueuePublish = true
		}
	}
	ch.exchangeMu.RUnlock()

	if !isQueuePublish {
		// Publish to the topic-based router (pub/sub)
		ch.conn.broker.Publish(topic, body, props)
	}

	// Publisher confirms
	if ch.confirmMode {
		seq := ch.publishSeq.Add(1)
		ack := &codec.BasicAck{
			DeliveryTag: seq,
			Multiple:    false,
		}
		ch.conn.writeMethod(ch.id, ack)
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

// Exchange methods

func (ch *Channel) handleExchangeDeclare(m *codec.ExchangeDeclare) error {
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
	ch.exchangeMu.Lock()
	delete(ch.exchanges, m.Exchange)
	ch.exchangeMu.Unlock()

	if !m.NoWait {
		return ch.conn.writeMethod(ch.id, &codec.ExchangeDeleteOk{})
	}
	return nil
}

func (ch *Channel) handleExchangeBind(m *codec.ExchangeBind) error {
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
	ch.exchangeMu.Lock()
	ch.queues[m.Queue] = true
	ch.exchangeMu.Unlock()

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

	ch.consumersMu.Lock()
	if _, exists := ch.consumers[tag]; exists {
		ch.consumersMu.Unlock()
		return ch.conn.sendChannelClose(ch.id, codec.NotAllowed,
			fmt.Sprintf("consumer tag %q already exists", tag), codec.ClassBasic, codec.MethodBasicConsume)
	}
	ch.consumers[tag] = &consumer{
		tag:       tag,
		queue:     m.Queue,
		noAck:     m.NoAck,
		exclusive: m.Exclusive,
	}
	ch.consumersMu.Unlock()

	ch.conn.broker.stats.IncrementConsumers()

	// Subscribe to the queue via queue manager
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		clientID := PrefixedClientID(ch.conn.connID)
		if err := qm.Subscribe(context.Background(), m.Queue, "", clientID, "", ""); err != nil {
			ch.conn.logger.Error("queue subscribe failed", "queue", m.Queue, "error", err)
		}
	}

	// Also subscribe via the topic router for pub/sub delivery
	connID := ch.conn.connID
	ch.conn.broker.router.Subscribe(connID, m.Queue, 1, storage.SubscribeOptions{})

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
		if qm != nil {
			clientID := PrefixedClientID(ch.conn.connID)
			qm.Unsubscribe(context.Background(), cons.queue, "", clientID, "")
		}

		ch.conn.broker.router.Unsubscribe(ch.conn.connID, cons.queue)
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
	ch.unackedMu.Lock()
	if m.Multiple {
		for tag, ud := range ch.unacked {
			if tag <= m.DeliveryTag {
				ch.ackDelivery(ud)
				delete(ch.unacked, tag)
			}
		}
	} else {
		if ud, ok := ch.unacked[m.DeliveryTag]; ok {
			ch.ackDelivery(ud)
			delete(ch.unacked, m.DeliveryTag)
		}
	}
	ch.unackedMu.Unlock()
	return nil
}

func (ch *Channel) handleBasicReject(m *codec.BasicReject) error {
	ch.unackedMu.Lock()
	if ud, ok := ch.unacked[m.DeliveryTag]; ok {
		if m.Requeue {
			ch.nackDelivery(ud)
		} else {
			ch.rejectDelivery(ud)
		}
		delete(ch.unacked, m.DeliveryTag)
	}
	ch.unackedMu.Unlock()
	return nil
}

func (ch *Channel) handleBasicNack(m *codec.BasicNack) error {
	ch.unackedMu.Lock()
	if m.Multiple {
		for tag, ud := range ch.unacked {
			if tag <= m.DeliveryTag {
				if m.Requeue {
					ch.nackDelivery(ud)
				} else {
					ch.rejectDelivery(ud)
				}
				delete(ch.unacked, tag)
			}
		}
	} else {
		if ud, ok := ch.unacked[m.DeliveryTag]; ok {
			if m.Requeue {
				ch.nackDelivery(ud)
			} else {
				ch.rejectDelivery(ud)
			}
			delete(ch.unacked, m.DeliveryTag)
		}
	}
	ch.unackedMu.Unlock()
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
		qm.Ack(context.Background(), ud.queueName, ud.messageID, ud.groupID)
	}
}

func (ch *Channel) nackDelivery(ud *unackedDelivery) {
	if ud.messageID == "" {
		return
	}
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		qm.Nack(context.Background(), ud.queueName, ud.messageID, ud.groupID)
	}
}

func (ch *Channel) rejectDelivery(ud *unackedDelivery) {
	if ud.messageID == "" {
		return
	}
	qm := ch.conn.broker.getQueueManager()
	if qm != nil {
		qm.Reject(context.Background(), ud.queueName, ud.messageID, ud.groupID, "rejected by client")
	}
}

// deliverMessage delivers a message to all consumers on this channel whose queue matches the topic.
func (ch *Channel) deliverMessage(topic string, payload []byte, props map[string]string) {
	if ch.closed.Load() {
		return
	}

	ch.consumersMu.RLock()
	defer ch.consumersMu.RUnlock()

	for _, cons := range ch.consumers {
		if !ch.consumerQueueMatches(cons, topic) {
			continue
		}

		deliveryTag := ch.conn.nextDeliveryTag()

		// Track for ack if not noAck
		if !cons.noAck {
			ch.unackedMu.Lock()
			ch.unacked[deliveryTag] = &unackedDelivery{
				deliveryTag: deliveryTag,
				routingKey:  topic,
				queueName:   cons.queue,
				messageID:   props["message-id"],
				groupID:     props["group-id"],
			}
			ch.unackedMu.Unlock()
		}

		// Determine exchange and routing key from topic
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
		if err := ch.conn.writeMethod(ch.id, deliver); err != nil {
			ch.conn.logger.Error("failed to write deliver", "error", err)
			return
		}

		// Build and send content header
		properties := codec.BasicProperties{
			ContentType:   props["content-type"],
			CorrelationID: props["correlation-id"],
			ReplyTo:       props["reply-to"],
			MessageID:     props["message-id"],
			Type:          props["type"],
		}
		header := &codec.ContentHeader{
			ClassID:    codec.ClassBasic,
			Weight:     0,
			BodySize:   uint64(len(payload)),
			Properties: properties,
		}
		var headerBuf bytes.Buffer
		if err := header.WriteContentHeader(&headerBuf); err != nil {
			ch.conn.logger.Error("failed to write content header", "error", err)
			return
		}
		headerFrame := &codec.Frame{
			Type:    codec.FrameHeader,
			Channel: ch.id,
			Payload: headerBuf.Bytes(),
		}
		if err := ch.conn.writeFrame(headerFrame); err != nil {
			ch.conn.logger.Error("failed to write header frame", "error", err)
			return
		}

		// Send body frame(s)
		maxBody := int(ch.conn.frameMax) - 8 // frame overhead
		if maxBody <= 0 {
			maxBody = len(payload)
		}
		for offset := 0; offset < len(payload) || offset == 0; {
			end := offset + maxBody
			if end > len(payload) {
				end = len(payload)
			}
			bodyFrame := &codec.Frame{
				Type:    codec.FrameBody,
				Channel: ch.id,
				Payload: payload[offset:end],
			}
			if err := ch.conn.writeFrame(bodyFrame); err != nil {
				ch.conn.logger.Error("failed to write body frame", "error", err)
				return
			}
			offset = end
			if offset == 0 {
				break // empty payload
			}
		}

		ch.conn.broker.stats.IncrementMessagesSent()
		ch.conn.broker.stats.AddBytesSent(uint64(len(payload)))
	}
}

// consumerQueueMatches checks if a consumer's queue matches the given topic.
func (ch *Channel) consumerQueueMatches(cons *consumer, topic string) bool {
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
		if qm != nil {
			qm.Unsubscribe(context.Background(), cons.queue, "", clientID, "")
		}
		ch.conn.broker.router.Unsubscribe(ch.conn.connID, cons.queue)
	}
}
