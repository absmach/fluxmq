// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func (c *Client) readLoop() {
	rt := c.loadWriteRuntime()
	if rt == nil || rt.doneCh == nil {
		return
	}
	defer close(rt.doneCh)

	conn := rt.conn

	for {
		if conn == nil {
			return
		}

		pkt, err := c.readPacketFromConn(conn)

		if err != nil {
			if err == io.EOF || c.state.get() != StateConnected {
				return
			}
			c.handleConnectionLost(err)
			return
		}

		c.updateActivity()
		c.handlePacket(pkt)
	}
}

func (c *Client) handlePacket(pkt packets.ControlPacket) {
	switch pkt.Type() {
	case packets.PublishType:
		c.handlePublish(pkt)
	case packets.PubAckType:
		c.handlePubAck(pkt)
	case packets.PubRecType:
		c.handlePubRec(pkt)
	case packets.PubRelType:
		c.handlePubRel(pkt)
	case packets.PubCompType:
		c.handlePubComp(pkt)
	case packets.SubAckType:
		c.handleSubAck(pkt)
	case packets.UnsubAckType:
		c.handleUnsubAck(pkt)
	case packets.PingRespType:
		c.pingMu.Lock()
		c.waitingPing = false
		c.lastPingSent = time.Time{}
		c.pingMu.Unlock()
	case packets.DisconnectType:
		c.handleServerDisconnect(pkt)
	case packets.AuthType:
		c.handleAuth(pkt)
	}
}

func (c *Client) handlePublish(pkt packets.ControlPacket) {
	msg, ok := c.decodeIncomingPublish(pkt)
	if !ok || msg == nil {
		return
	}

	switch msg.QoS {
	case 0:
		c.deliverMessage(msg)
	case 1:
		c.deliverMessage(msg)
		c.sendPubAck(msg.PacketID)
	case 2:
		c.qos2IncomingMu.Lock()
		c.qos2Incoming[msg.PacketID] = msg
		c.qos2IncomingMu.Unlock()
		_ = c.store.StoreInbound(msg.PacketID, msg)
		c.sendPubRec(msg.PacketID)
	}
}

func (c *Client) deliverMessage(msg *Message) {
	if msg == nil {
		return
	}
	c.deliverWithSharedDispatcher(msg)
}

// deliverWithSharedDispatcher keeps the current connection-wide dispatch model.
// This seam allows introducing per-subscription delivery without touching callers.

func (c *Client) deliverWithSharedDispatcher(msg *Message) {
	msgCh, msgStop := c.dispatcherChannels()
	if msgCh == nil {
		c.handleDeliveredMessage(msg)
		return
	}

	if c.opts.OrderMatters {
		select {
		case msgCh <- msg:
		case <-msgStop:
		}
		return
	}

	policy := c.opts.SlowConsumerPolicy
	if policy == "" {
		policy = SlowConsumerDropNew
	}

	msgBytes := int64(mqttMessageSize(msg))

	switch policy {
	case SlowConsumerDropOldest:
		c.deliverDropOldest(msgCh, msgStop, msg, msgBytes)
	case SlowConsumerBlockWithTimeout:
		c.deliverBlockWithTimeout(msgCh, msgStop, msg, msgBytes)
	default:
		c.deliverDropNew(msgCh, msgStop, msg, msgBytes)
	}
}

func (c *Client) handleDeliveredMessage(msg *Message) {
	// Check if this is a queue message
	if isQueueTopic(msg.Topic) {
		c.handleQueueMessage(msg)
		return
	}

	// Call OnMessageV2 if set (provides full message context)
	if c.opts.OnMessageV2 != nil {
		c.opts.OnMessageV2(msg)
		return
	}

	// Fall back to OnMessage for backward compatibility
	if c.opts.OnMessage != nil {
		c.opts.OnMessage(msg.Topic, msg.Payload, msg.QoS)
	}
}

func (c *Client) startDispatcher() {
	c.stopDispatcher()

	size := c.opts.MessageChanSize
	if size <= 0 {
		size = DefaultMessageChanSize
	}

	msgCh := make(chan *Message, size)
	msgStop := make(chan struct{})
	c.dispatchMu.Lock()
	c.msgCh = msgCh
	c.msgStop = msgStop
	c.dispatchRT.Store(&dispatchRuntime{
		msgCh:   msgCh,
		msgStop: msgStop,
	})
	c.dispatchMu.Unlock()

	workers := 1
	if !c.opts.OrderMatters {
		workers = runtime.GOMAXPROCS(0)
		if workers < 2 {
			workers = 2
		}
		if workers > 8 {
			workers = 8
		}
	}

	for i := 0; i < workers; i++ {
		c.dispatchWg.Add(1)
		go func(ch <-chan *Message, stop <-chan struct{}) {
			defer c.dispatchWg.Done()
			for {
				select {
				case <-stop:
					return
				case msg, ok := <-ch:
					if !ok {
						return
					}
					c.releasePending(int64(mqttMessageSize(msg)))
					c.handleDeliveredMessage(msg)
				}
			}
		}(msgCh, msgStop)
	}
}

func (c *Client) stopDispatcher() {
	c.dispatchMu.Lock()
	msgStop := c.msgStop
	if msgStop == nil {
		c.dispatchMu.Unlock()
		return
	}
	c.msgStop = nil
	c.msgCh = nil
	c.dispatchRT.Store(nil)
	close(msgStop)
	c.dispatchMu.Unlock()
	c.dispatchWg.Wait()
}

// handleQueueMessage processes a queue message and calls the appropriate handler.

func (c *Client) handleQueueMessage(msg *Message) {
	// Get queue subscription
	sub, ok := c.queueSubs.get(msg.Topic)
	if !ok || sub.handler == nil {
		// No handler registered, fall through to default handlers
		if c.opts.OnMessageV2 != nil {
			c.opts.OnMessageV2(msg)
		} else if c.opts.OnMessage != nil {
			c.opts.OnMessage(msg.Topic, msg.Payload, msg.QoS)
		}
		return
	}

	// Extract queue message metadata from user properties
	var messageID string
	var groupID string
	var offset uint64

	if msg.UserProperties != nil {
		if msgID, ok := msg.UserProperties["message-id"]; ok {
			messageID = msgID
		}
		if gid, ok := msg.UserProperties["group-id"]; ok {
			groupID = gid
		}
		if off, ok := msg.UserProperties["offset"]; ok {
			if parsed, err := strconv.ParseUint(off, 10, 64); err == nil {
				offset = parsed
			}
		} else if seq, ok := msg.UserProperties["sequence"]; ok {
			if parsed, err := strconv.ParseUint(seq, 10, 64); err == nil {
				offset = parsed
			}
		}
	}

	if c.queueAckCache != nil {
		c.queueAckCache.set(messageID, groupID)
	}

	// Create queue message with ack/nack/reject methods
	queueMsg := &QueueMessage{
		Message:   msg,
		MessageID: messageID,
		GroupID:   groupID,
		Offset:    offset,
		Sequence:  offset,
		client:    c,
		queueName: sub.queueName,
	}

	// Call handler
	sub.handler(queueMsg)
}

func (c *Client) handlePubAck(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)
	c.store.DeleteOutbound(packetID)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) handlePubRec(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)

	c.pending.updateQoS2State(packetID, 1) // Now waiting for PUBCOMP
	c.sendPubRel(packetID)
}

func (c *Client) handlePubRel(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)

	c.qos2IncomingMu.Lock()
	msg, exists := c.qos2Incoming[packetID]
	if exists {
		delete(c.qos2Incoming, packetID)
	}
	c.qos2IncomingMu.Unlock()

	if !exists {
		if storedMsg, ok := c.store.GetInbound(packetID); ok {
			msg = storedMsg
			exists = true
		}
	}

	if exists {
		_ = c.store.DeleteInbound(packetID)
	}

	if exists && msg != nil {
		c.deliverMessage(msg)
	}

	c.sendPubComp(packetID)
}

func (c *Client) handlePubComp(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)
	c.store.DeleteOutbound(packetID)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) handleSubAck(pkt packets.ControlPacket) {
	packetID, returnCodes, isV5 := subAckDetails(pkt)

	var err error
	for _, rc := range returnCodes {
		if isV5 {
			if rc >= 0x80 {
				err = ErrSubscribeFailed
				break
			}
			continue
		}
		if rc == 0x80 {
			err = ErrSubscribeFailed
			break
		}
	}

	c.pending.complete(packetID, err, returnCodes)
}

func (c *Client) handleUnsubAck(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) sendPubAck(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubAckType, packetID, 0))
}

func (c *Client) sendPubRec(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubRecType, packetID, 0))
}

func (c *Client) sendPubRel(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubRelType, packetID, 1))
}

func (c *Client) sendPubComp(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubCompType, packetID, 0))
}

func (c *Client) sendControlAck(data []byte) {
	if err := c.queueControlWrite(data, c.writeDeadline(nil)); err != nil {
		c.reportAsyncError(err)
	}
}

func (c *Client) handleServerDisconnect(pkt packets.ControlPacket) {
	if !c.isMQTTv5() {
		return
	}

	d := pkt.(*v5.Disconnect)
	reason := fmt.Sprintf("server disconnect: reason code 0x%02X", d.ReasonCode)
	if d.Properties != nil && d.Properties.ReasonString != "" {
		reason += " (" + d.Properties.ReasonString + ")"
	}

	c.handleConnectionLost(fmt.Errorf("%w: %s", ErrConnectionLost, reason))
}

// handleConnectAuth handles AUTH packets received during the CONNECT handshake.

func (c *Client) handleConnectAuth(conn net.Conn, auth *v5.Auth) error {
	if c.opts.OnAuth == nil {
		return ErrNoAuthHandler
	}

	method := c.opts.AuthMethod
	var data []byte
	if auth.Properties != nil {
		if auth.Properties.AuthMethod != "" {
			if c.opts.AuthMethod != "" && c.opts.AuthMethod != auth.Properties.AuthMethod {
				return ErrAuthMethodMismatch
			}
			method = auth.Properties.AuthMethod
		}
		data = auth.Properties.AuthData
	}

	if method == "" {
		return ErrAuthMethodMissing
	}

	responseData, err := c.opts.OnAuth(auth.ReasonCode, method, data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAuthFailed, err)
	}

	resp := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18, // Continue Authentication
		Properties: &v5.AuthProperties{
			AuthMethod: method,
			AuthData:   responseData,
		},
	}

	return resp.Pack(conn)
}

// handleAuth handles AUTH packets received during an active session (re-authentication).

func (c *Client) handleAuth(pkt packets.ControlPacket) {
	if !c.isMQTTv5() {
		return
	}

	auth := pkt.(*v5.Auth)
	if c.opts.OnAuth == nil {
		return
	}

	method := c.opts.AuthMethod
	var data []byte
	if auth.Properties != nil {
		if auth.Properties.AuthMethod != "" {
			if c.opts.AuthMethod != "" && c.opts.AuthMethod != auth.Properties.AuthMethod {
				c.handleConnectionLost(fmt.Errorf("%w: %s", ErrAuthMethodMismatch, auth.Properties.AuthMethod))
				return
			}
			method = auth.Properties.AuthMethod
		}
		data = auth.Properties.AuthData
	}

	if method == "" {
		c.handleConnectionLost(ErrAuthMethodMissing)
		return
	}

	responseData, err := c.opts.OnAuth(auth.ReasonCode, method, data)
	if err != nil {
		c.handleConnectionLost(fmt.Errorf("%w: %v", ErrAuthFailed, err))
		return
	}

	// Reason code 0x00 means success — no response needed
	if auth.ReasonCode == 0x00 {
		return
	}

	resp := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18, // Continue Authentication
		Properties: &v5.AuthProperties{
			AuthMethod: method,
			AuthData:   responseData,
		},
	}

	c.queueControlWriteNoWait(resp.Encode())
}

// SendAuth initiates re-authentication by sending an AUTH packet to the server (MQTT 5.0).

func (c *Client) SendAuth(reasonCode byte, authData []byte) error {
	if !c.isMQTTv5() {
		return ErrAuthNotV5
	}
	if c.state.get() != StateConnected {
		return ErrNotConnected
	}
	if c.opts.AuthMethod == "" {
		return ErrAuthMethodMissing
	}

	pkt := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  reasonCode,
		Properties: &v5.AuthProperties{
			AuthMethod: c.opts.AuthMethod,
			AuthData:   authData,
		},
	}

	return c.queueControlWrite(pkt.Encode(), time.Time{})
}

func (c *Client) dispatcherChannels() (chan *Message, chan struct{}) {
	if rt := c.dispatchRT.Load(); rt != nil {
		return rt.msgCh, rt.msgStop
	}

	c.dispatchMu.RLock()
	defer c.dispatchMu.RUnlock()
	return c.msgCh, c.msgStop
}

func (c *Client) deliverDropNew(msgCh chan *Message, msgStop chan struct{}, msg *Message, msgBytes int64) {
	if !c.reservePending(msgBytes) {
		c.onSlowConsumer(msg)
		return
	}

	select {
	case msgCh <- msg:
		c.slowConsumerNotified.Store(false)
	case <-msgStop:
		c.releasePending(msgBytes)
	default:
		c.releasePending(msgBytes)
		c.onSlowConsumer(msg)
	}
}

func (c *Client) deliverDropOldest(msgCh chan *Message, msgStop chan struct{}, msg *Message, msgBytes int64) {
	// Try to reserve pending capacity; if full, drop one queued message to make room.
	if !c.reservePending(msgBytes) {
		if !c.dropOldestQueued(msgCh, msgStop) {
			c.onSlowConsumer(msg)
			return
		}
		// Retry once after freeing one slot.
		if !c.reservePending(msgBytes) {
			c.onSlowConsumer(msg)
			return
		}
	}

	select {
	case msgCh <- msg:
		c.slowConsumerNotified.Store(false)
	case <-msgStop:
		c.releasePending(msgBytes)
	default:
		// Channel full — drop oldest to make room.
		c.releasePending(msgBytes)
		if !c.dropOldestQueued(msgCh, msgStop) {
			c.onSlowConsumer(msg)
			return
		}
		// Re-reserve and send.
		if !c.reservePending(msgBytes) {
			c.onSlowConsumer(msg)
			return
		}
		select {
		case msgCh <- msg:
			c.slowConsumerNotified.Store(false)
		case <-msgStop:
			c.releasePending(msgBytes)
		default:
			c.releasePending(msgBytes)
			c.onSlowConsumer(msg)
		}
	}
}

func (c *Client) deliverBlockWithTimeout(msgCh chan *Message, msgStop chan struct{}, msg *Message, msgBytes int64) {
	timeout := c.opts.SlowConsumerBlockTimeout
	if timeout <= 0 {
		c.deliverDropNew(msgCh, msgStop, msg, msgBytes)
		return
	}

	deadline := time.Now().Add(timeout)

	// Wait for pending capacity using condition variable.
	c.pendingMu.Lock()
	for !c.canReservePendingLocked(msgBytes) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.pendingMu.Unlock()
			c.onSlowConsumer(msg)
			return
		}
		// Schedule a wakeup at the deadline in case no release happens.
		timer := time.AfterFunc(remaining, func() { c.pendingCond.Signal() })
		c.pendingCond.Wait()
		timer.Stop()
	}
	c.pendingMessages++
	c.pendingBytes += msgBytes
	c.pendingMu.Unlock()

	remaining := time.Until(deadline)
	if remaining <= 0 {
		c.releasePending(msgBytes)
		c.onSlowConsumer(msg)
		return
	}

	timer := time.NewTimer(remaining)
	select {
	case msgCh <- msg:
		timer.Stop()
		c.slowConsumerNotified.Store(false)
	case <-msgStop:
		timer.Stop()
		c.releasePending(msgBytes)
	case <-timer.C:
		c.releasePending(msgBytes)
		c.onSlowConsumer(msg)
	}
}

func (c *Client) dropOldestQueued(msgCh chan *Message, msgStop chan struct{}) bool {
	select {
	case <-msgStop:
		return false
	case oldest := <-msgCh:
		c.releasePending(int64(mqttMessageSize(oldest)))
		c.onSlowConsumer(oldest)
		return true
	default:
		return false
	}
}

func (c *Client) reservePending(msgBytes int64) bool {
	if c.opts.MaxPendingMessages <= 0 && c.opts.MaxPendingBytes <= 0 {
		return true
	}

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if !c.canReservePendingLocked(msgBytes) {
		return false
	}

	c.pendingMessages++
	c.pendingBytes += msgBytes

	return true
}

// canReservePendingLocked checks whether there is capacity for msgBytes.
// Must be called with pendingMu held.

func (c *Client) canReservePendingLocked(msgBytes int64) bool {
	if c.opts.MaxPendingMessages > 0 && c.pendingMessages+1 > c.opts.MaxPendingMessages {
		return false
	}
	if c.opts.MaxPendingBytes > 0 && c.pendingBytes+msgBytes > c.opts.MaxPendingBytes {
		return false
	}
	return true
}

func (c *Client) releasePending(msgBytes int64) {
	if c.opts.MaxPendingMessages <= 0 && c.opts.MaxPendingBytes <= 0 {
		return
	}

	c.pendingMu.Lock()
	if c.pendingMessages > 0 {
		c.pendingMessages--
	}
	if c.pendingBytes >= msgBytes {
		c.pendingBytes -= msgBytes
	} else {
		c.pendingBytes = 0
	}
	c.pendingCond.Signal()
	c.pendingMu.Unlock()
}

func (c *Client) onSlowConsumer(msg *Message) {
	meta := &DroppedMessage{
		Direction: DroppedMessageInbound,
		Reason:    DroppedReasonSlowConsumer,
		Timestamp: time.Now().UTC(),
	}
	if msg != nil {
		meta.Topic = msg.Topic
		meta.QoS = msg.QoS
		meta.PayloadSize = len(msg.Payload)
	}

	c.droppedMessages.Add(1)
	c.reportDroppedMessage(c.prepareDroppedMeta(meta, DroppedMessageInbound, DroppedReasonSlowConsumer))
	if c.slowConsumerNotified.CompareAndSwap(false, true) {
		c.reportAsyncError(ErrSlowConsumer)
	}
}

func (c *Client) notifyDrain() {
	select {
	case c.drainCh <- struct{}{}:
	default:
	}
}

func (c *Client) reportAsyncError(err error) {
	if err == nil || c.opts.OnAsyncError == nil {
		return
	}
	enqueueAsyncCallback(func() {
		c.opts.OnAsyncError(err)
	})
}

func (c *Client) reportDroppedMessage(meta *DroppedMessage) {
	if meta == nil || c.opts.OnDroppedMessage == nil {
		return
	}
	copyMeta := *meta
	enqueueAsyncCallback(func() {
		c.opts.OnDroppedMessage(&copyMeta)
	})
}

func enqueueAsyncCallback(fn func()) {
	if fn == nil {
		return
	}

	callbackQueueOnce.Do(func() {
		callbackQueueCh = make(chan func(), defaultCallbackQueueSize)
		go func(callbackCh <-chan func()) {
			for fn := range callbackCh {
				if fn == nil {
					continue
				}
				func() {
					defer func() { recover() }()
					fn()
				}()
			}
		}(callbackQueueCh)
	})

	select {
	case callbackQueueCh <- fn:
	default:
	}
}

func (c *Client) prepareDroppedMeta(meta *DroppedMessage, dir DroppedMessageDirection, reason DroppedMessageReason) *DroppedMessage {
	if meta == nil {
		return &DroppedMessage{
			Direction: dir,
			Reason:    reason,
			Timestamp: time.Now().UTC(),
		}
	}
	out := *meta
	if out.Direction == "" {
		out.Direction = dir
	}
	if out.Reason == "" {
		out.Reason = reason
	}
	if out.Timestamp.IsZero() {
		out.Timestamp = time.Now().UTC()
	}
	return &out
}

func mqttMessageSize(msg *Message) int {
	if msg == nil {
		return 0
	}

	size := len(msg.Topic) + len(msg.Payload) + len(msg.ContentType) + len(msg.ResponseTopic) + len(msg.CorrelationData) + 32
	for k, v := range msg.UserProperties {
		size += len(k) + len(v)
	}
	size += len(msg.SubscriptionIDs) * 4
	return size
}

// DroppedMessages returns count of messages dropped due to slow consumer pressure.

func (c *Client) DroppedMessages() uint64 {
	return c.droppedMessages.Load()
}
