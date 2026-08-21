// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/bufpool"
	queuepkg "github.com/absmach/fluxmq/queue"
	qstorage "github.com/absmach/fluxmq/queue/storage"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

// consumer represents a Basic.Consume subscription on a channel.
type consumer struct {
	tag        string
	queue      string
	mqttFilter string
	queueName  string
	pattern    string
	groupID    string
	noAck      bool
	exclusive  bool
}

type queueInfo struct {
	name      string
	queueType string
	args      map[string]any
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
	arguments  map[string]any
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

	serverClosing atomic.Bool
	closed        atomic.Bool
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
	if ch.serverClosing.Load() {
		switch decoded.(type) {
		case *codec.ChannelClose, *codec.ChannelCloseOk:
		default:
			return nil
		}
	}

	if ch.conn.connectionPolicy().usesLocalPrincipalAuth() {
		allowed, err := ch.authorizeLocalMethod(decoded)
		if err != nil {
			return err
		}
		if !allowed {
			return nil
		}
	}

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
		ch.conn.writeMethod(ch.id, &codec.ChannelCloseOk{}) //nolint:errcheck // best-effort reply during channel close
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
			_ = ch.sendChannelClose(codec.CommandInvalid,
				"publish in progress", codec.ClassBasic, codec.MethodBasicPublish)
			ch.resetPendingPublish()
			return nil
		}
		exchangeName := normalizeExchange(m.Exchange)
		if exchangeName != "" && !ch.exchangeExists(exchangeName) {
			_ = ch.sendChannelClose(codec.NotFound,
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

func (ch *Channel) authorizeLocalMethod(decoded any) (bool, error) {
	principalID := ""
	if ch.conn.localIdentity != nil {
		principalID = ch.conn.localIdentity.PrincipalID
	}
	switch method := decoded.(type) {
	case *codec.ChannelFlow, *codec.ChannelClose, *codec.ChannelCloseOk, *codec.ConfirmSelect:
		return true, nil
	case *codec.QueueDeclare:
		// A non-passive declare creates the queue and, when it already exists,
		// rewrites its type, retention, TTL and durability. That is a
		// configuration write, and the subscribe ACL grants reads: a principal
		// permitted to consume a queue must not be able to reshape it. Services
		// consume pre-provisioned queues, so they only need to assert existence.
		if !method.Passive {
			return ch.denyLocalMethod(decoded, principalID)
		}
		return ch.authorizeLocalQueueName(method.Queue, principalID, decoded)
	case *codec.BasicConsume:
		return ch.authorizeLocalSubscribe(method.Queue, principalID, decoded)
	case *codec.BasicQos, *codec.BasicCancel, *codec.BasicAck, *codec.BasicNack, *codec.BasicReject:
		// Channel-scoped consumer lifecycle. These name no queue of their own;
		// they can only affect a consumer the subscribe ACL already allowed.
		if ch.conn.permitsConsumers() {
			return true, nil
		}
		return ch.denyLocalMethod(decoded, principalID)
	case *codec.BasicPublish:
		if ch.conn.canPublishLocal(method.Exchange, method.RoutingKey).Allowed() {
			return true, nil
		}
		ch.conn.broker.stats.IncrementLocalPublishDenials()
		ch.conn.logger.Warn("amqp091_local_authorization",
			"auth_mode", "local",
			"outcome", "denied",
			"reason", "publish_acl_mismatch",
			"principal_id", principalID,
			"exchange", method.Exchange,
			"routing_key", method.RoutingKey)
		return false, ch.sendChannelClose(codec.AccessRefused, "publish not authorized", codec.ClassBasic, codec.MethodBasicPublish)
	default:
		return ch.denyLocalMethod(decoded, principalID)
	}
}

// authorizeLocalQueueName authorizes AMQP methods whose queue field is the
// queue name itself. Queue.Declare uses the bare name "m"; unlike
// Basic.Consume, its field is not a "$queue/m" subscription address.
func (ch *Channel) authorizeLocalQueueName(queue, principalID string, decoded any) (bool, error) {
	if !ch.conn.permitsConsumers() {
		return ch.denyLocalMethod(decoded, principalID)
	}
	if queue != "" && ch.conn.canSubscribeLocal(queue) {
		return true, nil
	}

	classID, methodID := methodIdentifier(decoded)
	ch.conn.broker.stats.IncrementLocalSubscribeDenials()
	ch.conn.logger.Warn("amqp091_local_authorization",
		"auth_mode", "local",
		"outcome", "denied",
		"reason", "subscribe_acl_mismatch",
		"principal_id", principalID,
		"queue", queue,
		"resolved_queue", queue)
	return false, ch.sendChannelClose(codec.AccessRefused, "subscribe not authorized", classID, methodID)
}

// authorizeLocalSubscribe admits a queue-scoped consumer operation only for a
// principal whose role permits consumers and whose subscribe ACL names the
// queue.
//
// The ACL names queues, so the wire value is resolved first and the resolved
// queue name is what gets authorized: a client addresses "$queue/m" while the
// ACL says "m", and comparing the two directly would refuse every legitimate
// consumer. A filter that resolves to no queue is refused rather than admitted
// as a pub/sub subscription, because no ACL entry grants one.
func (ch *Channel) authorizeLocalSubscribe(queue, principalID string, decoded any) (bool, error) {
	if !ch.conn.permitsConsumers() {
		return ch.denyLocalMethod(decoded, principalID)
	}

	route := ch.conn.broker.routeResolver.Resolve(queue)
	reason := "subscribe_acl_mismatch"
	switch {
	case route.Kind != corebroker.RouteQueue:
		reason = "not_a_queue_address"
	case ch.conn.canSubscribeLocal(route.QueueName):
		return true, nil
	}

	classID, methodID := methodIdentifier(decoded)
	ch.conn.broker.stats.IncrementLocalSubscribeDenials()
	ch.conn.logger.Warn("amqp091_local_authorization",
		"auth_mode", "local",
		"outcome", "denied",
		"reason", reason,
		"principal_id", principalID,
		"queue", queue,
		"resolved_queue", route.QueueName)
	return false, ch.sendChannelClose(codec.AccessRefused, "subscribe not authorized", classID, methodID)
}

func (ch *Channel) denyLocalMethod(decoded any, principalID string) (bool, error) {
	classID, methodID := methodIdentifier(decoded)
	ch.conn.broker.stats.IncrementLocalOperationDenials()
	ch.conn.logger.Warn("amqp091_local_authorization",
		"auth_mode", "local",
		"outcome", "denied",
		"reason", "operation_not_permitted",
		"principal_id", principalID,
		"class_id", classID,
		"method_id", methodID)
	return false, ch.sendChannelClose(codec.AccessRefused, "operation not permitted for local principal", classID, methodID)
}

func (ch *Channel) sendChannelClose(code uint16, text string, classID, methodID uint16) error {
	ch.serverClosing.Store(true)
	ch.resetPendingPublish()
	return ch.conn.sendChannelClose(ch.id, code, text, classID, methodID)
}

func methodIdentifier(method any) (uint16, uint16) {
	switch method.(type) {
	case *codec.ExchangeDeclare:
		return codec.ClassExchange, codec.MethodExchangeDeclare
	case *codec.ExchangeDelete:
		return codec.ClassExchange, codec.MethodExchangeDelete
	case *codec.ExchangeBind:
		return codec.ClassExchange, codec.MethodExchangeBind
	case *codec.ExchangeUnbind:
		return codec.ClassExchange, codec.MethodExchangeUnbind
	case *codec.QueueDeclare:
		return codec.ClassQueue, codec.MethodQueueDeclare
	case *codec.QueueBind:
		return codec.ClassQueue, codec.MethodQueueBind
	case *codec.QueueUnbind:
		return codec.ClassQueue, codec.MethodQueueUnbind
	case *codec.QueuePurge:
		return codec.ClassQueue, codec.MethodQueuePurge
	case *codec.QueueDelete:
		return codec.ClassQueue, codec.MethodQueueDelete
	case *codec.BasicQos:
		return codec.ClassBasic, codec.MethodBasicQos
	case *codec.BasicConsume:
		return codec.ClassBasic, codec.MethodBasicConsume
	case *codec.BasicCancel:
		return codec.ClassBasic, codec.MethodBasicCancel
	case *codec.BasicGet:
		return codec.ClassBasic, codec.MethodBasicGet
	case *codec.BasicAck:
		return codec.ClassBasic, codec.MethodBasicAck
	case *codec.BasicReject:
		return codec.ClassBasic, codec.MethodBasicReject
	case *codec.BasicNack:
		return codec.ClassBasic, codec.MethodBasicNack
	case *codec.BasicRecover:
		return codec.ClassBasic, codec.MethodBasicRecover
	case *codec.BasicRecoverAsync:
		return codec.ClassBasic, codec.MethodBasicRecoverAsync
	case *codec.TxSelect:
		return codec.ClassTx, codec.MethodTxSelect
	case *codec.TxCommit:
		return codec.ClassTx, codec.MethodTxCommit
	case *codec.TxRollback:
		return codec.ClassTx, codec.MethodTxRollback
	default:
		return 0, 0
	}
}

// handleHeaderFrame processes a content header frame (part of a publish).
func (ch *Channel) handleHeaderFrame(frame *codec.Frame) {
	if ch.serverClosing.Load() {
		return
	}
	if ch.pendingMethod == nil || ch.pendingHeader != nil {
		_ = ch.sendChannelClose(codec.UnexpectedFrame,
			"unexpected content header", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}

	r := bytes.NewReader(frame.Payload)
	header, err := codec.ReadContentHeader(r)
	if err != nil {
		// A header that cannot be parsed leaves the channel mid-publish with no
		// way to frame what follows, so close it rather than waiting for body
		// frames that can never be matched.
		ch.conn.logger.Error("failed to read content header", "error", err)
		_ = ch.sendChannelClose(codec.FrameError,
			"malformed content header", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}
	if limit := ch.conn.connectionPolicy().maxMessageSize; limit > 0 && header.BodySize > limit {
		_ = ch.sendChannelClose(codec.ContentTooLarge,
			"message exceeds maximum size", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}
	ch.pendingHeader = header
	ch.pendingBodySize = header.BodySize
	ch.pendingBodyReceived = 0
	// Reserve for what has arrived, not for what was promised. The advertised
	// body size is attacker-controlled up to max_message_size, so allocating it
	// up front lets stalled publishers reserve max_connections × that size
	// without sending a single body byte. append grows the buffer as frames
	// actually arrive, which keeps a publisher's memory cost proportional to
	// the bytes it transmits.
	ch.pendingBody = make([]byte, 0, initialBodyCapacity(header.BodySize))

	// If body size is 0, the message is complete
	if header.BodySize == 0 {
		ch.completePublish()
	}
}

// maxInitialBodyCapacity caps the buffer reserved for an incoming message body
// before any of it has arrived. Bodies at or below it are still allocated once,
// so the common small-message path does not grow; larger ones grow as frames
// arrive.
const maxInitialBodyCapacity = 64 * 1024

// initialBodyCapacity converts an advertised body size into the capacity to
// reserve for it up front.
func initialBodyCapacity(bodySize uint64) int {
	if bodySize > maxInitialBodyCapacity {
		return maxInitialBodyCapacity
	}
	return int(bodySize)
}

// handleBodyFrame processes a content body frame.
func (ch *Channel) handleBodyFrame(frame *codec.Frame) {
	if ch.serverClosing.Load() {
		return
	}
	if ch.pendingMethod == nil || ch.pendingHeader == nil {
		_ = ch.sendChannelClose(codec.UnexpectedFrame,
			"unexpected content body", codec.ClassBasic, codec.MethodBasicPublish)
		ch.resetPendingPublish()
		return
	}

	nextSize := ch.pendingBodyReceived + uint64(len(frame.Payload))
	if nextSize > ch.pendingBodySize {
		_ = ch.sendChannelClose(codec.ContentTooLarge,
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

	clientID := PrefixedClientID(ch.conn.connID)
	policy := ch.conn.connectionPolicy()

	// A relayed origin identity is honored only from a trusted service. Anyone
	// else gets their own authenticated identity stamped, so a publisher cannot
	// attribute a message to another principal or to another protocol.
	relayedID := ""
	if ch.conn.propagatesOriginIdentity() {
		relayedID, _ = header.Properties.Headers[corebroker.ExternalIDProperty].(string)
	}
	if relayedID != "" {
		props[corebroker.ExternalIDProperty] = relayedID
	} else if externalID := ch.conn.externalID(clientID); externalID != "" {
		props[corebroker.ExternalIDProperty] = externalID
	}

	props[corebroker.ProtocolProperty] = corebroker.ProtocolAMQP091
	if ch.conn.propagatesOriginIdentity() {
		if v, ok := header.Properties.Headers[corebroker.ProtocolProperty].(string); ok && v != "" {
			props[corebroker.ProtocolProperty] = v
		}
	}

	// Broker-internal properties are accepted only from a trusted listener.
	// An externally authenticated client is a tenant or device regardless of
	// which protocol it speaks, so its reserved headers are dropped here rather
	// than forwarded as broker state.
	if policy.carriesReservedProperties() {
		for key, value := range header.Properties.Headers {
			if !corebroker.IsReservedProperty(key) {
				continue
			}
			if v, ok := value.(string); ok {
				props[key] = v
			}
		}
	}

	exchangeName := normalizeExchange(method.Exchange)
	routingKey := method.RoutingKey

	// Determine the topic from exchange+routingKey
	topic := routingKey
	if exchangeName != "" {
		topic = exchangeName + "/" + routingKey
	}

	props = corebroker.AddClientIDProperty(props, clientID)
	originalTopic := topic

	grant := LocalPublishGrantNone
	if policy.usesLocalPrincipalAuth() {
		// Re-evaluate at completion so an ACL reduction made while content frames
		// are in flight takes effect immediately. The grant this returns is also
		// what selects the delivery path below, so both come from one snapshot.
		grant = ch.conn.canPublishLocal(method.Exchange, method.RoutingKey)
		if !grant.Allowed() {
			ch.conn.broker.stats.IncrementLocalPublishDenials()
			ch.conn.logger.Warn("amqp091_local_authorization",
				"auth_mode", "local",
				"outcome", "denied",
				"reason", "publish_acl_changed",
				"principal_id", ch.conn.externalID(clientID),
				"exchange", method.Exchange,
				"routing_key", method.RoutingKey)
			_ = ch.sendChannelClose(codec.AccessRefused, "publish not authorized", codec.ClassBasic, codec.MethodBasicPublish)
			return
		}
	} else {
		hookReq, ok := ch.conn.applyHook(context.Background(), corebroker.BlockingHookRequest{
			Hook:       corebroker.HookAuthOnPublish,
			ClientID:   clientID,
			ExternalID: ch.conn.externalID(clientID),
			Topic:      topic,
			Payload:    body,
			Properties: props,
		})
		if !ok {
			ch.conn.logger.Warn("publish hook denied", "client_id", clientID, "topic", topic)
			_ = ch.sendChannelClose(codec.AccessRefused, "publish hook denied", codec.ClassBasic, codec.MethodBasicPublish)
			return
		}
		topic, body, props = hookReq.Topic, hookReq.Payload, hookReq.Properties
		if auth := policy.externalAuth; auth != nil {
			if !auth.CanPublish(clientID, topic) {
				ch.conn.logger.Warn("publish denied", "client_id", clientID, "topic", topic)
				_ = ch.sendChannelClose(codec.AccessRefused, "publish not authorized", codec.ClassBasic, codec.MethodBasicPublish)
				return
			}
		}
	}
	// The permission that authorized the publication decides how it is routed,
	// not the listener it arrived on. An exact routing key names a protected
	// durable stream and is appended and synced before the publisher is
	// confirmed; a prefix names no queue and is an ordinary topic publish. Both
	// listeners must agree, or one permissions.publish entry would mean two
	// different delivery contracts depending on which port the principal used.
	if grant == LocalPublishGrantExactTarget {
		if exchangeName != "" {
			ch.rejectLocalStreamPublish(fmt.Errorf("local durable stream publish requires the default exchange"))
			return
		}
		ch.handleLocalDurableStreamPublish(routingKey, body, props, clientID)
		return
	}

	// Route through resolver for default-exchange queue operations.
	resolver := ch.conn.broker.routeResolver
	// A prefix grant is checked against no queues entry, so it must not be able
	// to *address* one. Without this it could route into a queue two ways: a
	// routing key under a "$queue/"-shaped prefix, or one that happens to name a
	// configured stream. Both would take a queue-publish path on a permission
	// that never established the durability contract that path carries.
	//
	// This does not keep the publication out of every queue, and is not meant
	// to. A queue whose own topics pattern matches an ordinary topic captures
	// every publish on it, whatever protocol or principal it came from, so a
	// prefix publication is persisted exactly as an equivalent MQTT publish
	// would be. Persistence is decided by queue configuration; this branch
	// decides only that a prefix may not name a queue itself.
	if grant == LocalPublishGrantPrefix {
		ch.publishToPubSub(topics.AMQPTopicToMQTT(topic), body, props, method, header)
		return
	}
	if exchangeName == "" {
		route := resolver.Resolve(topic)
		switch route.Kind {
		case corebroker.RouteQueueCommit:
			ch.handleQueueCommit(route, header)
			if ch.confirmMode {
				ch.sendPublisherAck()
			}
			return
		case corebroker.RouteQueue:
			ch.handleQueuePublish(route.PublishTopic, body, props, clientID)
			return
		case corebroker.RouteQueueMalformed:
			// Publishing it would enqueue a message into the queue the client
			// was trying to control.
			ch.conn.logger.Warn("queue control verb is not the last level",
				"topic", topic, "verb", route.ControlVerb)
			return
		case corebroker.RouteQueueAck:
			// AMQP 0.9.1 does not use ack-via-publish; skip.
		case corebroker.RoutePubSub:
			// Fall through to stream queue check and exchange routing below.
		}
	}

	// RabbitMQ-style stream queue publish: default exchange with routingKey == queue name.
	if exchangeName == "" && ch.isStreamQueue(topic) {
		queueTopic := resolver.QueueTopic(topic)
		ch.handleQueuePublish(queueTopic, body, props, clientID)
		return
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

	bindingRoutingKey, checkExchangeBindings := routingKey, true
	if exchangeName != "" && topic != originalTopic {
		prefix := exchangeName + "/"
		if strings.HasPrefix(topic, prefix) {
			bindingRoutingKey = strings.TrimPrefix(topic, prefix)
		} else {
			checkExchangeBindings = false
		}
	}

	for _, b := range bindings {
		if checkExchangeBindings && ch.routingKeyMatches(b.routingKey, bindingRoutingKey, exchangeName) {
			// Route to the bound queue
			qm := ch.conn.broker.queueManager
			if qm != nil {
				queueTopic := resolver.QueueTopic(b.queue, bindingRoutingKey)
				if err := qm.Publish(context.Background(), qtypes.PublishRequest{
					ClientID:   clientID,
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
		pubsubTopic := topic
		if exchangeName == "" {
			pubsubTopic = topics.AMQPTopicToMQTT(topic)
		}
		ch.publishToPubSub(pubsubTopic, body, props, method, header)
		return
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

// publishToPubSub delivers through the topic router and settles the publisher
// confirm. It is the only delivery path a routing-key-prefix grant may take,
// because such a grant is authorized against no queue.
func (ch *Channel) publishToPubSub(pubsubTopic string, body []byte, props map[string]string, method *codec.BasicPublish, header *codec.ContentHeader) {
	if method.Mandatory {
		subs, err := ch.conn.broker.router.Match(pubsubTopic)
		if err != nil || len(subs) == 0 {
			if err != nil {
				ch.conn.logger.Error("router match failed", "topic", pubsubTopic, "error", err)
			}
			ch.sendBasicReturn(method, header, body)
			if ch.confirmMode {
				ch.sendPublisherAck()
			}
			return
		}
	}

	publishFailed := ch.conn.broker.Publish(ch.conn.publishContext(), pubsubTopic, body, props) != nil
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
	if cons.queueName == "" {
		routingKey = topics.MQTTTopicToAMQP(topic)
	} else if idx := strings.Index(topic, "/"); idx >= 0 {
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

	// Broker-internal properties are revealed only to a trusted listener, so an
	// externally authenticated consumer cannot observe state another service set.
	carryReserved := ch.conn.connectionPolicy().carriesReservedProperties()

	headers := make(map[string]any)
	for k, v := range props {
		if !carryReserved && corebroker.IsReservedProperty(k) {
			continue
		}
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
			return ch.sendChannelClose(codec.NotFound,
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

// queueExists reports whether a passive declaration should succeed. The
// channel-local map only holds queues this channel declared, so a
// pre-provisioned queue — the only kind a local principal may consume — is
// found through the queue manager instead.
func (ch *Channel) queueExists(name string) bool {
	if name == "" {
		return false
	}

	ch.exchangeMu.RLock()
	_, declared := ch.queues[name]
	ch.exchangeMu.RUnlock()
	if declared {
		return true
	}

	qm := ch.conn.broker.queueManager
	if qm == nil {
		return false
	}
	cfg, err := qm.GetQueue(context.Background(), name)
	return err == nil && cfg != nil
}

func (ch *Channel) handleQueueDeclare(m *codec.QueueDeclare) error {
	if m.Passive {
		if !ch.queueExists(m.Queue) {
			return ch.sendChannelClose(codec.NotFound,
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

	qm := ch.conn.broker.queueManager
	if qm != nil {
		queueTopicPattern := ch.conn.broker.routeResolver.QueueTopic(m.Queue, "#")
		var cfg qtypes.QueueConfig
		if m.Durable {
			cfg = qtypes.DefaultQueueConfig(m.Queue, queueTopicPattern)
		} else {
			cfg = qtypes.DefaultEphemeralQueueConfig(m.Queue, queueTopicPattern)
		}
		cfg.Type = qtypes.QueueType(queueType)

		if queueType == string(qtypes.QueueTypeStream) {
			cfg.Retention = extractStreamRetention(m.Arguments)
		}
		if ttl, ok := extractMessageTTL(m.Arguments); ok {
			cfg.MessageTTL = ttl
		}

		if err := qm.CreateQueue(context.Background(), cfg); err != nil {
			if errors.Is(err, queuepkg.ErrProtectedQueueMutation) {
				return ch.sendChannelClose(codec.PreconditionFailed,
					"protected queue contract cannot be changed", codec.ClassQueue, codec.MethodQueueDeclare)
			}
			if existing, getErr := qm.GetQueue(context.Background(), m.Queue); getErr == nil && existing != nil {
				existing.Type = qtypes.QueueType(queueType)
				if queueType == string(qtypes.QueueTypeStream) {
					existing.Retention = cfg.Retention
				}
				if ttl, ok := extractMessageTTL(m.Arguments); ok {
					existing.MessageTTL = ttl
				}
				if !m.Durable {
					existing.Durable = false
					if existing.ExpiresAfter == 0 {
						existing.ExpiresAfter = 5 * time.Minute
					}
				}
				if err := qm.UpdateQueue(context.Background(), *existing); err != nil {
					if errors.Is(err, queuepkg.ErrProtectedQueueMutation) {
						return ch.sendChannelClose(codec.PreconditionFailed,
							"protected queue contract cannot be changed", codec.ClassQueue, codec.MethodQueueDeclare)
					}
					ch.conn.logger.Warn("failed to update queue config", "queue", m.Queue, "error", err)
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

	qm := ch.conn.broker.queueManager
	if qm != nil {
		clientID := PrefixedClientID(ch.conn.connID)
		ctx, cancel := context.WithTimeout(context.Background(), clusterOpTimeout)
		qm.Unsubscribe(ctx, m.Queue, "", clientID, "") //nolint:errcheck // best-effort cleanup on queue delete
		cancel()
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

	clientID := PrefixedClientID(ch.conn.connID)
	externalID := ch.conn.externalID(clientID)
	queueFilter := m.Queue

	req, ok := ch.conn.applyHook(context.Background(), corebroker.BlockingHookRequest{
		Hook:       corebroker.HookAuthOnSubscribe,
		ClientID:   clientID,
		ExternalID: externalID,
		Topic:      queueFilter,
	})
	if !ok {
		ch.conn.logger.Warn("subscribe hook denied", "client_id", clientID, "filter", queueFilter)
		return ch.sendChannelClose(codec.AccessRefused, "subscribe hook denied", codec.ClassBasic, codec.MethodBasicConsume)
	}
	queueFilter = req.Topic

	if auth := ch.conn.connectionPolicy().externalAuth; auth != nil {
		if !auth.CanSubscribe(clientID, queueFilter) {
			ch.conn.logger.Warn("subscribe denied", "client_id", clientID, "filter", queueFilter)
			return ch.sendChannelClose(codec.AccessRefused, "subscribe not authorized", codec.ClassBasic, codec.MethodBasicConsume)
		}
	}

	route := ch.conn.broker.routeResolver.Resolve(queueFilter)
	isQueue := route.Kind == corebroker.RouteQueue
	queueName, pattern := route.QueueName, route.Pattern
	mqttFilter := ""
	if !isQueue {
		mqttFilter = topics.AMQPFilterToMQTT(queueFilter)
	}

	qm := ch.conn.broker.queueManager
	queueInfo := ch.getQueueInfo(queueFilter)
	streamCursor, hasStreamOffset := extractStreamOffset(m.Arguments)
	isStream := (queueInfo != nil && queueInfo.queueType == string(qtypes.QueueTypeStream)) || hasStreamOffset
	if !isStream && qm != nil && queueName != "" {
		if cfg, err := qm.GetQueue(context.Background(), queueName); err == nil && cfg != nil && cfg.Type == qtypes.QueueTypeStream {
			isStream = true
		}
	}
	if isStream && !isQueue {
		queueName = queueFilter
		pattern = ""
	}

	groupID := extractConsumerGroup(m.Arguments)
	if isStream && groupID == "" {
		groupID = tag
	}
	if queueName != "" && groupID == "" {
		groupID = queuepkg.DefaultConsumerGroupID(clientID)
	}

	cons := &consumer{
		tag:        tag,
		queue:      queueFilter,
		mqttFilter: mqttFilter,
		queueName:  queueName,
		pattern:    pattern,
		groupID:    groupID,
		noAck:      m.NoAck,
		exclusive:  m.Exclusive,
	}

	ch.consumersMu.Lock()
	if _, exists := ch.consumers[tag]; exists {
		ch.consumersMu.Unlock()
		return ch.sendChannelClose(codec.NotAllowed,
			fmt.Sprintf("consumer tag %q already exists", tag), codec.ClassBasic, codec.MethodBasicConsume)
	}
	// Reserve the consumer before subscribing because queue delivery can begin
	// as soon as the manager registers it. A failed subscription removes this
	// reservation before the channel reports the failure.
	ch.consumers[tag] = cons
	ch.conn.retainQueueRegistration(cons)
	ch.conn.broker.stats.IncrementConsumers()
	ch.consumersMu.Unlock()

	// rollbackConsumer drops the reservation and reports whether it released the
	// last owner of the connection-level queue-manager registration.
	rollbackConsumer := func() (registrationUnused bool) {
		ch.consumersMu.Lock()
		removed := false
		if current, exists := ch.consumers[tag]; exists && current == cons {
			delete(ch.consumers, tag)
			removed = true
		}
		ch.consumersMu.Unlock()
		if !removed {
			return false
		}

		ch.conn.broker.stats.DecrementConsumers()
		return ch.conn.releaseQueueRegistration(cons)
	}

	// Subscribe to the queue via queue manager
	if (isQueue || isStream) && queueName != "" {
		if qm == nil {
			rollbackConsumer()
			return ch.sendChannelClose(codec.InternalError, "queue manager unavailable", codec.ClassBasic, codec.MethodBasicConsume)
		}

		subGroupID := groupID
		var (
			subscribeErr        error
			subscriptionStarted bool
		)
		if isStream {
			cursor := streamCursor
			if cursor == nil {
				cursor = &qtypes.CursorOption{Position: qtypes.CursorDefault}
			}
			cursor.Mode = qtypes.GroupModeStream
			if autoCommit := extractAutoCommit(m.Arguments); autoCommit != nil {
				cursor.AutoCommit = autoCommit
			}
			if ch.conn.connectionPolicy().usesLocalPrincipalAuth() {
				existing, ok := qm.(corebroker.ExistingQueueSubscriber)
				if !ok {
					subscribeErr = fmt.Errorf("queue manager does not support non-mutating subscriptions")
				} else {
					subscriptionStarted = true
					subscribeErr = existing.SubscribeExistingWithCursor(context.Background(), queueName, pattern, clientID, subGroupID, "", cursor)
				}
			} else {
				subscriptionStarted = true
				subscribeErr = qm.SubscribeWithCursor(context.Background(), queueName, pattern, clientID, subGroupID, "", cursor)
			}
		} else if ch.conn.connectionPolicy().usesLocalPrincipalAuth() {
			existing, ok := qm.(corebroker.ExistingQueueSubscriber)
			if !ok {
				subscribeErr = fmt.Errorf("queue manager does not support non-mutating subscriptions")
			} else {
				subscriptionStarted = true
				subscribeErr = existing.SubscribeExisting(context.Background(), queueName, pattern, clientID, subGroupID, "")
			}
		} else {
			subscriptionStarted = true
			subscribeErr = qm.Subscribe(context.Background(), queueName, pattern, clientID, subGroupID, "")
		}

		if subscribeErr != nil {
			registrationUnused := rollbackConsumer()
			// A manager can fail after partially registering group state. Cleanup is
			// best effort; errors raised before registration need no cleanup, and an
			// in-use connection registration must remain intact.
			if subscriptionStarted && registrationUnused &&
				!errors.Is(subscribeErr, qstorage.ErrQueueNotFound) && !errors.Is(subscribeErr, queuepkg.ErrQueueNotStream) {
				_ = qm.Unsubscribe(context.Background(), queueName, pattern, clientID, subGroupID)
			}
			ch.conn.logger.Error("queue subscribe failed", "queue", queueName, "error", subscribeErr)
			switch {
			case errors.Is(subscribeErr, qstorage.ErrQueueNotFound):
				return ch.sendChannelClose(codec.NotFound, "queue not found", codec.ClassBasic, codec.MethodBasicConsume)
			case errors.Is(subscribeErr, queuepkg.ErrQueueNotStream):
				return ch.sendChannelClose(codec.PreconditionFailed, "queue is not a stream", codec.ClassBasic, codec.MethodBasicConsume)
			default:
				return ch.sendChannelClose(codec.InternalError, "queue subscription failed", codec.ClassBasic, codec.MethodBasicConsume)
			}
		}
	}

	// Subscribe via the topic router for pub/sub delivery (non-queue topics).
	if !isQueue && !isStream {
		clientID := PrefixedClientID(ch.conn.connID)
		if err := ch.conn.broker.router.Subscribe(clientID, mqttFilter, 1, storage.SubscribeOptions{}); err != nil {
			ch.conn.logger.Error("pubsub subscribe failed", "filter", queueFilter, "mqtt_filter", mqttFilter, "error", err)
		}
		if cl := ch.conn.broker.cluster; cl != nil {
			if err := cl.AddSubscription(context.Background(), clientID, mqttFilter, 1, storage.SubscribeOptions{}); err != nil {
				ch.conn.logger.Error("cluster add subscription failed", "filter", queueFilter, "mqtt_filter", mqttFilter, "error", err)
			}
		}
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

		clientID := PrefixedClientID(ch.conn.connID)
		ctx, cancel := context.WithTimeout(context.Background(), clusterOpTimeout)
		defer cancel()

		qm := ch.conn.broker.queueManager
		if cons.queueName != "" {
			if registrationUnused := ch.conn.releaseQueueRegistration(cons); registrationUnused && qm != nil {
				qm.Unsubscribe(ctx, cons.queueName, cons.pattern, clientID, cons.groupID) //nolint:errcheck // best-effort cleanup on final consumer cancel
			}
		}

		if cons.queueName == "" {
			if cons.mqttFilter == "" {
				ch.conn.logger.Warn("pubsub unsubscribe skipped: missing canonical filter", "consumer_tag", cons.tag)
			} else {
				if err := ch.conn.broker.router.Unsubscribe(clientID, cons.mqttFilter); err != nil {
					ch.conn.logger.Warn("pubsub unsubscribe failed", "mqtt_filter", cons.mqttFilter, "error", err)
				}
				if cl := ch.conn.broker.cluster; cl != nil {
					if err := cl.RemoveSubscription(ctx, clientID, cons.mqttFilter); err != nil {
						ch.conn.logger.Error("cluster remove subscription failed", "mqtt_filter", cons.mqttFilter, "error", err)
					}
				}
			}
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
	qm := ch.conn.broker.queueManager
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
	qm := ch.conn.broker.queueManager
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
	qm := ch.conn.broker.queueManager
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
		resolver := ch.conn.broker.routeResolver
		queueTopic := resolver.QueueTopic(cons.queueName)
		switch {
		case topic == cons.queueName, topic == queueTopic:
			return cons.pattern == "" || cons.pattern == "#"
		case strings.HasPrefix(topic, queueTopic+"/"):
			if cons.pattern == "" {
				return true
			}
			routingKey := strings.TrimPrefix(topic, queueTopic+"/")
			return topics.TopicMatch(cons.pattern, routingKey)
		}
	}

	if cons.mqttFilter == "" {
		return false
	}
	if cons.mqttFilter == topic {
		return true
	}
	return topics.TopicMatch(cons.mqttFilter, topic)
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

	qm := ch.conn.broker.queueManager
	clientID := PrefixedClientID(ch.conn.connID)
	ctx, cancel := context.WithTimeout(context.Background(), clusterOpTimeout)
	defer cancel()

	for _, cons := range consumers {
		ch.conn.broker.stats.DecrementConsumers()
		if cons.queueName != "" {
			if registrationUnused := ch.conn.releaseQueueRegistration(cons); registrationUnused && qm != nil {
				qm.Unsubscribe(ctx, cons.queueName, cons.pattern, clientID, cons.groupID) //nolint:errcheck // best-effort cleanup for the final owner during channel close
			}
		}
		if cons.queueName == "" {
			if cons.mqttFilter == "" {
				ch.conn.logger.Warn("pubsub unsubscribe skipped: missing canonical filter", "consumer_tag", cons.tag)
				continue
			}
			if err := ch.conn.broker.router.Unsubscribe(clientID, cons.mqttFilter); err != nil {
				ch.conn.logger.Warn("pubsub unsubscribe failed", "mqtt_filter", cons.mqttFilter, "error", err)
			}
			if cl := ch.conn.broker.cluster; cl != nil {
				if err := cl.RemoveSubscription(ctx, clientID, cons.mqttFilter); err != nil {
					ch.conn.logger.Error("cluster remove subscription failed", "mqtt_filter", cons.mqttFilter, "error", err)
				}
			}
		}
	}
}

// cancelConsumerByQueue sends a server-initiated basic.cancel for any consumer
// on this channel that matches the given queue and group. Per AMQP 0.9.1 spec,
// the server sends basic.cancel with NoWait=true for server-initiated cancellation.
// This does NOT call qm.Unsubscribe because the consumer was already removed
// by the queue manager's stale heartbeat cleanup.
func (ch *Channel) cancelConsumerByQueue(queueName, groupID string) {
	if ch.closed.Load() {
		return
	}

	ch.consumersMu.Lock()
	var toCancel []*consumer
	for tag, cons := range ch.consumers {
		// The manager reports the group it registered, which a pattern
		// qualifies. Comparing the raw group would never match a patterned
		// consumer, leaving it uncancelled and its registration held.
		if cons.queueName == queueName &&
			corebroker.EffectiveConsumerGroupID(cons.groupID, cons.pattern) == groupID {
			toCancel = append(toCancel, cons)
			delete(ch.consumers, tag)
		}
	}
	ch.consumersMu.Unlock()

	for _, cons := range toCancel {
		ch.conn.releaseQueueRegistration(cons)
		ch.conn.broker.stats.DecrementConsumers()
		if err := ch.conn.writeMethod(ch.id, &codec.BasicCancel{
			ConsumerTag: cons.tag,
			NoWait:      true,
		}); err != nil {
			ch.conn.logger.Warn("failed to send server-initiated basic.cancel",
				slog.String("consumer_tag", cons.tag),
				slog.String("queue", queueName),
				slog.String("error", err.Error()))
		}
	}
}

// handleQueuePublish publishes a message to the queue manager and handles confirm mode.
func (ch *Channel) handleQueuePublish(queueTopic string, body []byte, props map[string]string, clientID string) {
	qm := ch.conn.broker.queueManager
	if qm == nil {
		return
	}
	props = corebroker.AddClientIDProperty(props, clientID)
	err := qm.Publish(context.Background(), qtypes.PublishRequest{
		ClientID:   clientID,
		Topic:      queueTopic,
		Payload:    body,
		Properties: props,
	})
	if err != nil {
		ch.conn.logger.Error("queue publish failed", "queue", queueTopic, "error", err)
	}
	if ch.confirmMode {
		if err != nil {
			ch.sendPublisherNack()
		} else {
			ch.sendPublisherAck()
		}
	}
}

func (ch *Channel) handleLocalDurableStreamPublish(queueName string, body []byte, props map[string]string, clientID string) {
	qm := ch.conn.broker.queueManager
	publisher, ok := qm.(durableStreamQueuePublisher)
	if qm == nil || !ok {
		ch.rejectLocalStreamPublish(fmt.Errorf("durable exact stream publisher is unavailable"))
		return
	}
	// Abandoned appends keep running and keep their payload, so a stream that
	// already has the maximum waiting on storage refuses new work outright
	// rather than starting a barrier that nothing will wait for.
	if !ch.conn.broker.durableAppends.acquire(queueName) {
		ch.conn.broker.stats.IncrementLocalPublishRejections()
		ch.conn.logger.Warn("amqp091_local_publish",
			"auth_mode", "local",
			"outcome", "rejected",
			"reason", "durable_append_backlog",
			"queue", queueName)
		ch.abandonLocalStreamPublish(fmt.Errorf("stream %q already has the maximum durable appends waiting on storage", queueName))
		return
	}

	props = corebroker.AddClientIDProperty(props, clientID)
	ctx, cancel := context.WithTimeout(ch.conn.publishContext(), localPublishTimeout)
	defer cancel()

	// The append and its fsync cannot be interrupted once the storage layer has
	// entered them, so the deadline is enforced here rather than inside the
	// store. Run the barrier on its own goroutine and stop waiting when the
	// deadline passes: the connection goroutine stays responsive and the session
	// can be closed, instead of a stalled disk holding a listener slot open
	// indefinitely. The abandoned append may still complete and become visible,
	// which is why a NACK is not proof that the record was not written. The slot
	// is released only when the barrier really finishes.
	result := make(chan error, 1)
	go func() {
		defer ch.conn.broker.durableAppends.release(queueName)
		result <- publisher.PublishToDurableStream(ctx, queueName, qtypes.PublishRequest{
			ClientID:   clientID,
			Topic:      ch.conn.broker.routeResolver.QueueTopic(queueName),
			Payload:    body,
			Properties: props,
		})
	}()

	select {
	case err := <-result:
		if err != nil {
			ch.rejectLocalStreamPublish(err)
			return
		}
		if ch.confirmMode {
			ch.sendPublisherAck()
		}
	case <-ctx.Done():
		ch.conn.broker.stats.IncrementLocalPublishTimeouts()
		ch.abandonLocalStreamPublish(fmt.Errorf("durable stream barrier did not complete: %w", ctx.Err()))
	}
}

// rejectLocalStreamPublish reports a publication that storage refused. The
// channel stays usable because the outcome is final and the publisher can
// retry on it.
func (ch *Channel) rejectLocalStreamPublish(err error) {
	ch.conn.logger.Error("local durable stream publish failed", "error", err)
	if ch.confirmMode {
		ch.sendPublisherNack()
		return
	}
	_ = ch.sendChannelClose(codec.InternalError,
		"durable stream publish failed", codec.ClassBasic, codec.MethodBasicPublish)
}

// abandonLocalStreamPublish reports a publication whose outcome FluxMQ no
// longer knows, because it stopped waiting for the barrier or refused to start
// one. The channel is closed after the NACK: retrying on it would queue more
// work behind storage that has not recovered, and the publisher must treat the
// record as undetermined rather than failed.
func (ch *Channel) abandonLocalStreamPublish(err error) {
	ch.conn.logger.Error("local durable stream publish abandoned", "error", err)
	if ch.confirmMode {
		ch.sendPublisherNack()
	}
	_ = ch.sendChannelClose(codec.InternalError,
		"durable stream publish abandoned", codec.ClassBasic, codec.MethodBasicPublish)
}

// handleQueueCommit processes a stream offset commit routed via the resolver.
func (ch *Channel) handleQueueCommit(route corebroker.RouteResult, header *codec.ContentHeader) {
	qm := ch.conn.broker.queueManager
	if qm == nil {
		ch.conn.logger.Warn("queue commit ignored: queue manager not configured", "queue", route.QueueName)
		return
	}
	queueName := route.QueueName
	if queueName == "" {
		ch.conn.logger.Warn("queue commit missing queue name", "topic", route.PublishTopic)
		return
	}
	headers := header.Properties.Headers
	groupID, ok := parseStringArg(headers[qtypes.PropCommitGroupID])
	if !ok || groupID == "" {
		ch.conn.logger.Warn("queue commit missing group id", "queue", queueName)
		return
	}
	offsetVal, ok := headers[qtypes.PropCommitOffset]
	if !ok {
		ch.conn.logger.Warn("queue commit missing offset", "queue", queueName, "group", groupID)
		return
	}
	n, ok := parseInt64Arg(offsetVal)
	if !ok || n < 0 {
		ch.conn.logger.Warn("queue commit invalid offset", "queue", queueName, "group", groupID)
		return
	}
	if err := qm.CommitOffset(context.Background(), queueName, groupID, uint64(n)); err != nil {
		ch.conn.logger.Warn("queue commit failed", "queue", queueName, "group", groupID, "error", err)
	}
}

func extractConsumerGroup(args map[string]any) string {
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

// isStreamQueue reports whether a publish target is a stream, checking the
// names this channel declared before falling back to the globally configured
// queues. It runs on every default-exchange publication, so it resolves the at
// most two candidate names in place rather than building a slice of them.
func (ch *Channel) isStreamQueue(name string) bool {
	if ch.isDeclaredStreamQueue(name) {
		return true
	}

	queueName, _ := corebroker.ParseQueueFilter(name)
	sameName := queueName == "" || queueName == name
	if !sameName && ch.isDeclaredStreamQueue(queueName) {
		return true
	}

	qm := ch.conn.broker.queueManager
	if qm == nil {
		return false
	}
	if ch.isConfiguredStreamQueue(qm, name) {
		return true
	}
	return !sameName && ch.isConfiguredStreamQueue(qm, queueName)
}

// isDeclaredStreamQueue checks the queues declared on this channel.
func (ch *Channel) isDeclaredStreamQueue(name string) bool {
	info := ch.getQueueInfo(name)
	return info != nil && info.queueType == string(qtypes.QueueTypeStream)
}

// isConfiguredStreamQueue checks queues that exist without a channel-local
// declaration, such as streams provisioned from the broker configuration.
func (ch *Channel) isConfiguredStreamQueue(qm channelQueueManager, name string) bool {
	cfg, err := qm.GetQueue(ch.conn.publishContext(), name)
	return err == nil && cfg != nil && cfg.Type == qtypes.QueueTypeStream
}

func extractQueueType(args map[string]any) string {
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

func extractStreamRetention(args map[string]any) qtypes.RetentionPolicy {
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

// extractMessageTTL parses x-message-ttl from queue arguments.
// Per AMQP 0.9.1 spec, x-message-ttl is in milliseconds.
func extractMessageTTL(args map[string]any) (time.Duration, bool) {
	if len(args) == 0 {
		return 0, false
	}
	val, ok := args["x-message-ttl"]
	if !ok {
		return 0, false
	}
	ms, ok := parseInt64Arg(val)
	if !ok || ms < 0 {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

func extractStreamOffset(args map[string]any) (*qtypes.CursorOption, bool) {
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
	case int32:
		return int64(v), true
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

func extractAutoCommit(args map[string]any) *bool {
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
