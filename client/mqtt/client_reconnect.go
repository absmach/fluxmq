// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"math/rand/v2"
	"time"
)

func (c *Client) handleConnectionLost(err error) {
	c.lifecycleMu.Lock()
	if !c.state.transition(StateConnected, StateDisconnected) {
		c.lifecycleMu.Unlock()
		return
	}
	c.lifecycleMu.Unlock()
	c.notifyDrain()

	c.stopKeepAlive()
	c.cleanup(context.Background(), ErrConnectionLost) //nolint:errcheck

	if c.opts.OnConnectionLost != nil {
		go c.opts.OnConnectionLost(err)
	}

	if c.opts.AutoReconnect && !c.state.isClosed() {
		go c.reconnect()
	}
}

func (c *Client) reconnect() {
	c.reconnMu.Lock()
	defer c.reconnMu.Unlock()

	if !c.state.transition(StateDisconnected, StateReconnecting) {
		return
	}

	delay := c.opts.ReconnectBackoff
	attempt := 0

	for !c.state.isClosed() {
		attempt++

		if c.opts.OnReconnecting != nil {
			c.opts.OnReconnecting(attempt)
		}

		err := c.Connect(context.Background())
		if err == nil {
			return
		}
		if c.opts.MaxReconnectAttempts > 0 && attempt >= c.opts.MaxReconnectAttempts {
			if c.opts.OnReconnectFailed != nil {
				go c.opts.OnReconnectFailed(err)
			}
			return
		}

		sleepFor := delay
		if c.opts.ReconnectJitter > 0 {
			sleepFor += time.Duration(rand.Int64N(int64(c.opts.ReconnectJitter) + 1))
		}

		// Exponential backoff
		time.Sleep(sleepFor)
		delay *= 2
		if delay > c.opts.MaxReconnectWait {
			delay = c.opts.MaxReconnectWait
		}
	}
}

func (c *Client) restoreState() {
	if !c.state.isConnected() {
		return
	}

	c.restoreSubscriptions()
	c.restoreQueueSubscriptions()
	c.restoreOutboundMessages()
}

func (c *Client) restoreSubscriptions() {
	records := c.subscriptions.snapshot()
	if len(records) == 0 {
		return
	}

	for _, rec := range records {
		var err error
		if rec.opt != nil {
			err = c.subscribeWithOptions(nil, []*SubscribeOption{rec.opt})
		} else {
			err = c.subscribe(nil, map[string]byte{rec.topic: rec.qos})
		}
		if err != nil {
			c.reportAsyncError(err)
		}
	}
}

func (c *Client) restoreQueueSubscriptions() {
	c.queueSubs.mu.RLock()
	subs := make([]*queueSubscription, 0, len(c.queueSubs.subs))
	for _, sub := range c.queueSubs.subs {
		copied := *sub
		subs = append(subs, &copied)
	}
	c.queueSubs.mu.RUnlock()

	for _, sub := range subs {
		var err error
		if c.isMQTTv5() {
			userProps := make(map[string]string)
			if sub.consumerGroup != "" {
				userProps["consumer-group"] = sub.consumerGroup
			}
			err = c.subscribeWithUserProperties(nil, "$queue/"+sub.queueName, 1, userProps)
		} else {
			err = c.subscribe(nil, map[string]byte{"$queue/" + sub.queueName: 1})
		}
		if err != nil {
			c.reportAsyncError(err)
		}
	}
}

func (c *Client) restoreOutboundMessages() {
	msgs := c.store.GetAllOutbound()
	if len(msgs) == 0 {
		return
	}

	// Reset clears stale packet IDs so nextPacketID() cannot collide with them
	// during re-queue.
	_ = c.store.Reset()
	usedStoreIDs := make(map[uint16]struct{}, len(msgs))

	storeForRetry := func(msg *Message, preferredID uint16) {
		if msg == nil || msg.QoS == 0 {
			return
		}
		candidate := preferredID
		if candidate == 0 {
			candidate = 1
		}
		for tries := 0; tries < 0xFFFF; tries++ {
			if _, used := usedStoreIDs[candidate]; !used {
				stored := msg.Copy()
				if stored == nil {
					return
				}
				stored.PacketID = candidate
				if err := c.store.StoreOutbound(candidate, stored); err != nil {
					c.reportAsyncError(err)
				} else {
					usedStoreIDs[candidate] = struct{}{}
				}
				return
			}
			candidate++
			if candidate == 0 {
				candidate = 1
			}
		}
		c.reportAsyncError(ErrMaxInflight)
	}

	for i, msg := range msgs {
		if msg == nil {
			continue
		}

		if msg.QoS == 0 {
			_ = c.sendPublish(nil, msg, 0)
			continue
		}

		originalPacketID := msg.PacketID

		replay := msg.Copy()
		if replay == nil {
			storeForRetry(msg, msg.PacketID)
			continue
		}
		replay.Dup = true

		packetID, staged, _, err := c.stageQoSPublish(replay)
		if err == ErrMaxInflight {
			// Inflight slots exhausted — preserve all remaining messages.
			for _, remaining := range msgs[i:] {
				if remaining != nil && remaining.QoS > 0 {
					storeForRetry(remaining, remaining.PacketID)
				}
			}
			c.reportAsyncError(err)
			return
		}
		if err != nil {
			c.reportAsyncError(err)
			storeForRetry(msg, originalPacketID)
			continue
		}
		usedStoreIDs[packetID] = struct{}{}

		if err := c.sendPublish(nil, staged, packetID); err != nil {
			c.pending.remove(packetID)
			c.reportAsyncError(err)
			continue
		}
	}
}

func (c *Client) handleDisconnectedPublish(msg *Message) error {
	if err := c.bufferDisconnectedPublish(msg); err != nil {
		if err == ErrReconnectBufferFull {
			c.reportAsyncError(err)
			meta := &DroppedMessage{
				Direction: DroppedMessageOutbound,
				Reason:    DroppedReasonReconnectBufferFull,
				Timestamp: time.Now().UTC(),
			}
			if msg != nil {
				meta.PayloadSize = len(msg.Payload)
				meta.Topic = msg.Topic
				meta.QoS = msg.QoS
			}
			c.reportDroppedMessage(c.prepareDroppedMeta(meta, DroppedMessageOutbound, DroppedReasonReconnectBufferFull))
		}
		return err
	}
	return nil
}

func (c *Client) bufferDisconnectedPublish(msg *Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}
	if c.state.isClosed() {
		return ErrClientClosed
	}
	if !c.opts.AutoReconnect || c.opts.ReconnectBufSize <= 0 {
		return ErrNotConnected
	}

	state := c.state.get()
	if state != StateDisconnected && state != StateReconnecting && state != StateConnecting {
		return ErrNotConnected
	}

	wire := c.encodePublishPacket(msg, 0, false, nil)
	msgBytes := len(wire)
	if msgBytes <= 0 {
		msgBytes = 1
	}
	entry := &reconnectBuffered{
		wire:       wire,
		topic:      msg.Topic,
		qos:        msg.QoS,
		payloadSz:  len(msg.Payload),
		bufferedSz: msgBytes,
	}

	c.reconnectBufMu.Lock()
	defer c.reconnectBufMu.Unlock()

	if entry.bufferedSz > c.opts.ReconnectBufSize || c.reconnectBufBytes+entry.bufferedSz > c.opts.ReconnectBufSize {
		return ErrReconnectBufferFull
	}

	c.reconnectBuf = append(c.reconnectBuf, entry)
	c.reconnectBufBytes += entry.bufferedSz

	return nil
}

func (c *Client) flushReconnectBuffer() {
	if !c.state.isConnected() {
		return
	}

	c.reconnectBufMu.Lock()
	entries := c.reconnectBuf
	c.reconnectBuf = nil
	c.reconnectBufBytes = 0
	c.reconnectBufMu.Unlock()

	for i, entry := range entries {
		if entry == nil {
			continue
		}
		if len(entry.wire) == 0 {
			continue
		}

		if entry.qos == 0 {
			if err := c.queuePublishRawWrite(nil, entry.wire, c.writeDeadline(nil), entry.topic, entry.qos, entry.payloadSz, true); err != nil {
				c.prependReconnectBuffer(entries[i:])
				c.reportAsyncError(err)
				return
			}
			continue
		}

		msg, err := c.decodeBufferedPublish(entry.wire)
		if err != nil {
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}

		packetID, _, _, err := c.stageQoSPublish(msg)
		if err != nil {
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}

		if err := patchPublishPacketID(entry.wire, packetID); err != nil {
			c.rollbackStagedPublish(packetID)
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}

		if err := c.queuePublishRawWrite(nil, entry.wire, c.writeDeadline(nil), entry.topic, entry.qos, entry.payloadSz, true); err != nil {
			c.rollbackStagedPublish(packetID)
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}
	}
}

func (c *Client) prependReconnectBuffer(entries []*reconnectBuffered) {
	if len(entries) == 0 {
		return
	}

	total := 0
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		sz := entry.bufferedSz
		if sz <= 0 {
			sz = len(entry.wire)
			if sz <= 0 {
				sz = 1
			}
			entry.bufferedSz = sz
		}
		total += sz
	}

	c.reconnectBufMu.Lock()
	defer c.reconnectBufMu.Unlock()

	combined := make([]*reconnectBuffered, 0, len(entries)+len(c.reconnectBuf))
	combined = append(combined, entries...)
	combined = append(combined, c.reconnectBuf...)
	c.reconnectBuf = combined
	c.reconnectBufBytes += total
}

func (c *Client) reconnectBufferDepth() int {
	c.reconnectBufMu.Lock()
	defer c.reconnectBufMu.Unlock()
	return len(c.reconnectBuf)
}
