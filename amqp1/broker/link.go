// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/amqp1/message"
	"github.com/absmach/fluxmq/amqp1/performatives"
	amqptypes "github.com/absmach/fluxmq/amqp1/types"
	qtypes "github.com/absmach/fluxmq/queue/types"
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
	isQueue         bool
	queueName       string
	consumerGroup   string
	capabilityBased bool // detected via capability (not $queue/ prefix)
	cursor          *qtypes.CursorOption

	// Management node
	isManagement bool

	mu     sync.Mutex
	logger *slog.Logger
}

type pendingDelivery struct {
	deliveryID uint32
	messageID  string
	queueName  string
	groupID    string
}

const managementAddress = "$management"

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

	// Detect management address
	if address == managementAddress {
		l.isManagement = true
		return l
	}

	// Detect queue via capabilities first (native AMQP addressing)
	if isSender && attach.Source != nil && performatives.HasCapability(attach.Source.Capabilities, performatives.CapQueue) {
		l.isQueue = true
		l.capabilityBased = true
		l.queueName = address
	} else if !isSender && attach.Target != nil && performatives.HasCapability(attach.Target.Capabilities, performatives.CapQueue) {
		l.isQueue = true
		l.capabilityBased = true
		l.queueName = address
	} else if strings.HasPrefix(address, "$queue/") {
		// Fallback: $queue/ prefix detection for backward compat
		l.isQueue = true
		parts := strings.SplitN(strings.TrimPrefix(address, "$queue/"), "/", 2)
		l.queueName = parts[0]
	}

	// Extract consumer group from attach properties
	if attach.Properties != nil {
		if cg, ok := attach.Properties["consumer-group"]; ok {
			l.consumerGroup, _ = cg.(string)
		}
		// Extract cursor for consumer links
		if isSender {
			if cursorVal, ok := attach.Properties["cursor"]; ok {
				l.cursor = parseCursor(cursorVal)
			}
		}
	}

	return l
}

func parseCursor(val any) *qtypes.CursorOption {
	switch v := val.(type) {
	case string:
		switch v {
		case "earliest":
			return &qtypes.CursorOption{Position: qtypes.CursorEarliest}
		case "latest":
			return &qtypes.CursorOption{Position: qtypes.CursorLatest}
		default:
			var offset uint64
			if _, err := fmt.Sscanf(v, "%d", &offset); err == nil {
				return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: offset}
			}
		}
	case uint64:
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: v}
	case uint32:
		return &qtypes.CursorOption{Position: qtypes.CursorOffset, Offset: uint64(v)}
	}
	return nil
}

// subscribe registers this link with the router or queue manager.
func (l *Link) subscribe() {
	// Check subscribe authorization
	auth := l.session.conn.broker.getAuth()
	if auth != nil {
		clientID := PrefixedClientID(l.session.conn.containerID)
		if !auth.CanSubscribe(clientID, l.address) {
			l.logger.Warn("subscribe denied", "client", clientID, "address", l.address)
			return
		}
	}

	if l.isQueue {
		clientID := PrefixedClientID(l.session.conn.containerID)
		ctx := context.Background()
		qm := l.session.conn.broker.getQueueManager()
		if qm == nil {
			l.logger.Warn("queue manager not available for subscription", "address", l.address)
			return
		}

		pattern := ""
		if !l.capabilityBased && strings.HasPrefix(l.address, "$queue/") {
			rest := strings.TrimPrefix(l.address, "$queue/")
			parts := strings.SplitN(rest, "/", 2)
			if len(parts) > 1 {
				pattern = parts[1]
			}
		}

		if l.cursor != nil {
			if err := qm.SubscribeWithCursor(ctx, l.queueName, pattern, clientID, l.consumerGroup, "", l.cursor); err != nil {
				l.logger.Error("queue subscribe with cursor failed", "address", l.address, "error", err)
			}
		} else {
			if err := qm.Subscribe(ctx, l.queueName, pattern, clientID, l.consumerGroup, ""); err != nil {
				l.logger.Error("queue subscribe failed", "address", l.address, "error", err)
			}
		}
	} else {
		// Regular pub/sub via the AMQP router
		l.session.conn.broker.router.Subscribe(
			l.session.conn.containerID,
			l.address,
			1, // default QoS
			storage.SubscribeOptions{},
		)

		if cl := l.session.conn.broker.getCluster(); cl != nil {
			clientID := PrefixedClientID(l.session.conn.containerID)
			if err := cl.AddSubscription(context.Background(), clientID, l.address, 1, storage.SubscribeOptions{}); err != nil {
				l.logger.Error("cluster add subscription failed", "address", l.address, "error", err)
			}
		}
	}
}

// detach cleans up the link's subscriptions.
func (l *Link) detach() {
	if l.isQueue {
		clientID := PrefixedClientID(l.session.conn.containerID)
		ctx := context.Background()
		qm := l.session.conn.broker.getQueueManager()
		if qm != nil {
			qm.Unsubscribe(ctx, l.queueName, "", clientID, l.consumerGroup)
		}
	} else if l.isSender {
		l.session.conn.broker.router.Unsubscribe(l.session.conn.containerID, l.address)

		if cl := l.session.conn.broker.getCluster(); cl != nil {
			clientID := PrefixedClientID(l.session.conn.containerID)
			if err := cl.RemoveSubscription(context.Background(), clientID, l.address); err != nil {
				l.logger.Error("cluster remove subscription failed", "address", l.address, "error", err)
			}
		}
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

	// Handle management requests
	if l.isManagement {
		l.handleManagementTransfer(transfer, msg)
		return
	}

	l.session.conn.broker.stats.IncrementMessagesReceived()
	l.session.conn.broker.stats.AddBytesReceived(uint64(len(payload)))
	if m := l.session.conn.broker.getMetrics(); m != nil {
		m.RecordMessageReceived(int64(len(payload)))
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
	auth := l.session.conn.broker.getAuth()
	if auth != nil {
		clientID := PrefixedClientID(l.session.conn.containerID)
		if !auth.CanPublish(clientID, topic) {
			l.logger.Warn("publish denied", "client", clientID, "topic", topic)
			return
		}
	}

	if l.isQueue || strings.HasPrefix(topic, "$queue/") {
		qm := l.session.conn.broker.getQueueManager()
		if qm != nil {
			// For capability-based links, construct the publish topic
			publishTopic := topic
			if l.capabilityBased && !strings.HasPrefix(topic, "$queue/") {
				publishTopic = "$queue/" + l.queueName
				if msg.Properties != nil && msg.Properties.Subject != "" {
					publishTopic = publishTopic + "/" + msg.Properties.Subject
				}
			}

			props := make(map[string]string)
			for k, v := range msg.ApplicationProperties {
				if s, ok := v.(string); ok {
					props[k] = s
				}
			}
			if err := qm.Publish(context.Background(), qtypes.PublishRequest{
				Topic:      publishTopic,
				Payload:    data,
				Properties: props,
			}); err != nil {
				l.logger.Error("queue publish failed", "topic", publishTopic, "error", err)
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
		body, err := disp.Encode()
		if err != nil {
			l.logger.Error("failed to encode disposition", "error", err)
			return
		}
		if err := l.session.conn.conn.WritePerformative(l.session.localCh, body); err != nil {
			l.logger.Error("failed to send disposition", "error", err)
		}
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

	deliveryID, ok := l.session.consumeOutgoingWindow()
	if !ok {
		return
	}
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

	l.session.conn.broker.stats.IncrementMessagesSent()
	l.session.conn.broker.stats.AddBytesSent(uint64(len(msgBytes)))
	if m := l.session.conn.broker.getMetrics(); m != nil {
		m.RecordMessageSent(int64(len(msgBytes)))
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

	deliveryID, ok := l.session.consumeOutgoingWindow()
	if !ok {
		return
	}
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

	if err := l.session.conn.conn.WriteTransfer(l.session.localCh, transfer, msgBytes); err != nil {
		l.logger.Error("failed to send transfer", "error", err)
		return
	}

	l.session.conn.broker.stats.IncrementMessagesSent()
	l.session.conn.broker.stats.AddBytesSent(uint64(len(msgBytes)))
	if m := l.session.conn.broker.getMetrics(); m != nil {
		m.RecordMessageSent(int64(len(msgBytes)))
	}

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

	qm := l.session.conn.broker.getQueueManager()

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

// handleManagementTransfer processes a management request and sends the response
// on the paired management receiver link in the same session.
func (l *Link) handleManagementTransfer(transfer *performatives.Transfer, msg *message.Message) {
	handler := newManagementHandler(l.session.conn.broker)
	resp := handler.handleRequest(msg)

	// Settle the incoming transfer
	if !transfer.Settled && transfer.DeliveryID != nil {
		disp := &performatives.Disposition{
			Role:    performatives.RoleReceiver,
			First:   *transfer.DeliveryID,
			Settled: true,
			State:   &performatives.Accepted{},
		}
		body, err := disp.Encode()
		if err == nil {
			l.session.conn.conn.WritePerformative(l.session.localCh, body)
		}
	}

	// Find the paired management sender link (broker→client) in this session
	replyLink := l.session.findManagementSenderLink()
	if replyLink == nil {
		l.logger.Warn("no management reply link found in session")
		return
	}

	replyLink.sendAMQPMessage(resp, 0)
}

// sendDetachError sends a detach with an error condition back to the client.
func (l *Link) sendDetachError(condition amqptypes.Symbol, description string) {
	resp := &performatives.Detach{
		Handle: l.handle,
		Closed: true,
		Error: &performatives.Error{
			Condition:   condition,
			Description: description,
		},
	}
	body, err := resp.Encode()
	if err != nil {
		l.logger.Error("failed to encode detach error", "error", err)
		return
	}
	if err := l.session.conn.conn.WritePerformative(l.session.localCh, body); err != nil {
		l.logger.Error("failed to send detach error", "error", err)
	}
}

func uint32ToBytes(v uint32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
