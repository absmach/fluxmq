// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/amqp/message"
	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/storage"
)

// Link represents an AMQP link (sender or receiver from the broker's perspective).
type Link struct {
	session  *Session
	name     string
	handle   uint32
	isSender bool // true = broker sends TO client (client role = receiver)
	address  string

	// Flow control (for sender links)
	credit        uint32
	deliveryCount uint32

	// Pending deliveries for unsettled transfers
	pending   map[uint32]*pendingDelivery // deliveryID -> delivery
	pendingMu sync.Mutex

	// Queue subscription info
	isQueue       bool
	queueName     string
	consumerGroup string

	mu     sync.Mutex
	logger *slog.Logger
}

type pendingDelivery struct {
	deliveryID uint32
	messageID  string
	queueName  string
	groupID    string
}

func newLink(s *Session, attach *performatives.Attach) *Link {
	// Determine address from source/target based on role
	var address string
	isSender := attach.Role // if client is receiver, broker is sender

	if isSender {
		// Broker sends to client: address from source
		if attach.Source != nil {
			address = attach.Source.Address
		}
	} else {
		// Broker receives from client: address from target
		if attach.Target != nil {
			address = attach.Target.Address
		}
	}

	l := &Link{
		session:  s,
		name:     attach.Name,
		handle:   attach.Handle,
		isSender: isSender,
		address:  address,
		pending:  make(map[uint32]*pendingDelivery),
		logger:   s.conn.logger,
	}

	// Detect queue topic
	if strings.HasPrefix(address, "$queue/") {
		l.isQueue = true
		parts := strings.SplitN(strings.TrimPrefix(address, "$queue/"), "/", 2)
		l.queueName = parts[0]
	}

	// Extract consumer group from attach properties
	if attach.Properties != nil {
		if cg, ok := attach.Properties["consumer-group"]; ok {
			l.consumerGroup, _ = cg.(string)
		}
	}

	return l
}

// subscribe registers this link with the router or queue manager.
func (l *Link) subscribe() {
	// Check subscribe authorization
	auth := l.session.conn.broker.auth
	if auth != nil {
		clientID := PrefixedClientID(l.session.conn.containerID)
		if !auth.CanSubscribe(clientID, l.address) {
			l.logger.Warn("subscribe denied", "client", clientID, "address", l.address)
			return
		}
	}

	if l.isQueue {
		// Queue subscription
		clientID := PrefixedClientID(l.session.conn.containerID)
		ctx := context.Background()
		qm := l.session.conn.broker.queueManager
		if qm == nil {
			l.logger.Warn("queue manager not available for subscription", "address", l.address)
			return
		}

		pattern := ""
		if strings.HasPrefix(l.address, "$queue/") {
			rest := strings.TrimPrefix(l.address, "$queue/")
			parts := strings.SplitN(rest, "/", 2)
			if len(parts) > 1 {
				pattern = parts[1]
			}
		}

		if err := qm.Subscribe(ctx, l.queueName, pattern, clientID, l.consumerGroup, ""); err != nil {
			l.logger.Error("queue subscribe failed", "address", l.address, "error", err)
		}
	} else {
		// Regular pub/sub via the AMQP router
		l.session.conn.broker.router.Subscribe(
			l.session.conn.containerID,
			l.address,
			1, // default QoS
			storage.SubscribeOptions{},
		)
	}
}

// detach cleans up the link's subscriptions.
func (l *Link) detach() {
	if l.isQueue {
		clientID := PrefixedClientID(l.session.conn.containerID)
		ctx := context.Background()
		qm := l.session.conn.broker.queueManager
		if qm != nil {
			qm.Unsubscribe(ctx, l.queueName, "", clientID, l.consumerGroup)
		}
	} else if l.isSender {
		l.session.conn.broker.router.Unsubscribe(l.session.conn.containerID, l.address)
	}
}

// receiveTransfer handles an incoming transfer from the client (client is sender).
func (l *Link) receiveTransfer(transfer *performatives.Transfer, payload []byte) {
	// Decode the AMQP message from payload
	msg, err := message.Decode(payload)
	if err != nil {
		l.logger.Error("failed to decode AMQP message", "error", err)
		return
	}

	// Determine the topic from the message or link address
	topic := l.address
	if msg.Properties != nil && msg.Properties.To != "" {
		topic = msg.Properties.To
	}

	// Extract payload data
	var data []byte
	if len(msg.Data) > 0 {
		data = msg.Data[0]
	}

	// Check publish authorization
	auth := l.session.conn.broker.auth
	if auth != nil {
		clientID := PrefixedClientID(l.session.conn.containerID)
		if !auth.CanPublish(clientID, topic) {
			l.logger.Warn("publish denied", "client", clientID, "topic", topic)
			return
		}
	}

	if l.isQueue || strings.HasPrefix(topic, "$queue/") {
		// Publish to queue
		qm := l.session.conn.broker.queueManager
		if qm != nil {
			props := make(map[string]string)
			for k, v := range msg.ApplicationProperties {
				if s, ok := v.(string); ok {
					props[k] = s
				}
			}
			if err := qm.Publish(context.Background(), topic, data, props); err != nil {
				l.logger.Error("queue publish failed", "topic", topic, "error", err)
			}
		}
	} else {
		// Publish to AMQP router (pub/sub)
		props := make(map[string]string)
		for k, v := range msg.ApplicationProperties {
			if s, ok := v.(string); ok {
				props[k] = s
			}
		}
		l.session.conn.broker.Publish(topic, data, props)
	}

	// Settle if pre-settled by sender
	if !transfer.Settled && transfer.DeliveryID != nil {
		// Send disposition with Accepted
		disp := &performatives.Disposition{
			Role:    performatives.RoleReceiver,
			First:   *transfer.DeliveryID,
			Settled: true,
			State:   &performatives.Accepted{},
		}
		body, _ := disp.Encode()
		l.session.conn.conn.WritePerformative(l.session.localCh, body)
	}
}

// sendMessage constructs and sends a message to the client via this link.
func (l *Link) sendMessage(topic string, payload []byte, props map[string]string, qos byte) {
	l.mu.Lock()
	if l.credit == 0 {
		l.mu.Unlock()
		return
	}
	l.credit--
	l.deliveryCount++
	l.mu.Unlock()

	deliveryID := l.session.allocateDeliveryID()
	settled := qos == 0

	// Build AMQP message
	msg := &message.Message{
		Properties: &message.Properties{
			To: topic,
		},
		Data: [][]byte{payload},
	}
	if len(props) > 0 {
		msg.ApplicationProperties = make(map[string]any, len(props))
		for k, v := range props {
			msg.ApplicationProperties[k] = v
		}
	}

	msgBytes, err := msg.Encode()
	if err != nil {
		l.logger.Error("failed to encode message", "error", err)
		return
	}

	// Build transfer performative
	msgFormat := uint32(0)
	transfer := &performatives.Transfer{
		Handle:        l.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   uint32ToBytes(deliveryID),
		MessageFormat: &msgFormat,
		Settled:       settled,
	}

	if err := l.session.conn.conn.WriteTransfer(l.session.localCh, transfer, msgBytes); err != nil {
		l.logger.Error("failed to send transfer", "error", err)
		return
	}

	// Track unsettled delivery
	if !settled {
		l.pendingMu.Lock()
		pd := &pendingDelivery{deliveryID: deliveryID}
		if msgID, ok := props["message-id"]; ok {
			pd.messageID = msgID
			pd.queueName, _ = props["queue"]
			pd.groupID, _ = props["group-id"]
		}
		l.pending[deliveryID] = pd
		l.pendingMu.Unlock()
	}
}

// sendAMQPMessage sends a pre-built AMQP message to the client.
func (l *Link) sendAMQPMessage(msg interface{}, qos byte) {
	amqpMsg, ok := msg.(*message.Message)
	if !ok {
		return
	}

	l.mu.Lock()
	if l.credit == 0 {
		l.mu.Unlock()
		return
	}
	l.credit--
	l.deliveryCount++
	l.mu.Unlock()

	deliveryID := l.session.allocateDeliveryID()
	settled := qos == 0

	msgBytes, err := amqpMsg.Encode()
	if err != nil {
		return
	}

	msgFormat := uint32(0)
	transfer := &performatives.Transfer{
		Handle:        l.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   uint32ToBytes(deliveryID),
		MessageFormat: &msgFormat,
		Settled:       settled,
	}

	l.session.conn.conn.WriteTransfer(l.session.localCh, transfer, msgBytes)

	if !settled && amqpMsg.ApplicationProperties != nil {
		l.pendingMu.Lock()
		pd := &pendingDelivery{deliveryID: deliveryID}
		if msgID, ok := amqpMsg.ApplicationProperties["message-id"]; ok {
			pd.messageID, _ = msgID.(string)
		}
		if qn, ok := amqpMsg.ApplicationProperties["queue"]; ok {
			pd.queueName, _ = qn.(string)
		}
		if gid, ok := amqpMsg.ApplicationProperties["group-id"]; ok {
			pd.groupID, _ = gid.(string)
		}
		l.pending[deliveryID] = pd
		l.pendingMu.Unlock()
	}
}

// handleDisposition processes a disposition for deliveries on this link.
func (l *Link) handleDisposition(disp *performatives.Disposition) {
	first := disp.First
	last := first
	if disp.Last != nil {
		last = *disp.Last
	}

	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()

	qm := l.session.conn.broker.queueManager

	for id := first; id <= last; id++ {
		pd, ok := l.pending[id]
		if !ok {
			continue
		}

		if pd.messageID != "" && qm != nil {
			ctx := context.Background()
			switch disp.State.(type) {
			case *performatives.Accepted:
				qm.Ack(ctx, pd.queueName, pd.messageID, pd.groupID)
			case *performatives.Rejected:
				qm.Reject(ctx, pd.queueName, pd.messageID, pd.groupID, "rejected by client")
			case *performatives.Released:
				qm.Nack(ctx, pd.queueName, pd.messageID, pd.groupID)
			}
		}

		if disp.Settled {
			delete(l.pending, id)
		}
	}
}

// drainPending is called when credit becomes available to send pending messages.
func (l *Link) drainPending() {
	// For now, delivery is push-based from the broker side.
	// This would be used for buffered messages if we implement message queueing per link.
}

func uint32ToBytes(v uint32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
