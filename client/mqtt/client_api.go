// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func (c *Client) openPending(opType pendingType, msg *Message) (uint16, *pendingOp, error) {
	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return 0, nil, ErrMaxInflight
	}

	op, err := c.pending.add(packetID, opType, msg)
	if err != nil {
		return 0, nil, err
	}

	return packetID, op, nil
}

func (c *Client) waitPendingAck(ctx context.Context, packetID uint16, op *pendingOp) error {
	if op == nil {
		return nil
	}
	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		return err
	}
	return nil
}

func (c *Client) sendAndAwaitPending(ctx context.Context, opType pendingType, msg *Message, send func(packetID uint16) error) error {
	packetID, op, err := c.openPending(opType, msg)
	if err != nil {
		return err
	}

	if err := send(packetID); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return c.waitPendingAck(ctx, packetID, op)
}

func (c *Client) rollbackStoredPublish(packetID uint16) {
	c.pending.remove(packetID)
	c.store.DeleteOutbound(packetID)
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	msg := NewMessage(topic, payload, qos, retain)
	_, _, err := c.publishInternal(ctx, msg, true)
	return err
}

// PublishMessage sends a message with optional MQTT 5.0 publish properties.
// For MQTT 3.1.1, publish properties are ignored.

func (c *Client) PublishMessage(ctx context.Context, msg *Message) error {
	_, _, err := c.publishInternal(ctx, msg, true)
	return err
}

func (c *Client) validatePublishMessage(msg *Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}
	if msg.QoS > 2 {
		return ErrInvalidQoS
	}
	if msg.Topic == "" {
		return ErrInvalidTopic
	}
	return nil
}

func (c *Client) publishInternal(ctx context.Context, msg *Message, waitAck bool) (*pendingOp, uint16, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
	}
	if err := c.validatePublishMessage(msg); err != nil {
		return nil, 0, err
	}

	if c.draining.Load() {
		return nil, 0, ErrDraining
	}

	if !c.state.isConnected() {
		return nil, 0, c.handleDisconnectedPublish(msg)
	}

	if msg.QoS == 0 {
		return nil, 0, c.sendPublish(ctx, msg, 0)
	}

	publishMsg := msg.Copy()
	if publishMsg == nil {
		return nil, 0, ErrInvalidMessage
	}

	packetID, op, err := c.openPending(pendingPublish, publishMsg)
	if err != nil {
		return nil, 0, err
	}
	publishMsg.PacketID = packetID

	if err := c.store.StoreOutbound(packetID, publishMsg); err != nil {
		c.pending.remove(packetID)
		return nil, 0, err
	}

	if err := c.sendPublish(ctx, publishMsg, packetID); err != nil {
		c.rollbackStoredPublish(packetID)
		return nil, 0, err
	}

	if !waitAck {
		return op, packetID, nil
	}

	if err := c.waitPendingAck(ctx, packetID, op); err != nil {
		c.store.DeleteOutbound(packetID)
		return nil, 0, err
	}
	return nil, packetID, nil
}

// PublishAsync publishes without blocking for acknowledgments and returns a completion token.

func (c *Client) PublishAsync(ctx context.Context, topic string, payload []byte, qos byte, retain bool) *PublishToken {
	tok := &PublishToken{token: newToken()}
	msg := NewMessage(topic, payload, qos, retain)
	op, packetID, err := c.publishInternal(ctx, msg, false)
	if err != nil {
		tok.complete(err)
		return tok
	}
	tok.MessageID = packetID
	if op != nil {
		tok.bindPending(op, c.opts.AckTimeout)
		return tok
	}
	tok.complete(nil)
	return tok
}

// PublishMessageAsync publishes a message with properties without blocking for acknowledgments.

func (c *Client) PublishMessageAsync(ctx context.Context, msg *Message) *PublishToken {
	tok := &PublishToken{token: newToken()}
	op, packetID, err := c.publishInternal(ctx, msg, false)
	if err != nil {
		tok.complete(err)
		return tok
	}
	tok.MessageID = packetID
	if op != nil {
		tok.bindPending(op, c.opts.AckTimeout)
		return tok
	}
	tok.complete(nil)
	return tok
}

func (c *Client) sendPublish(ctx context.Context, msg *Message, packetID uint16) error {
	return c.sendPublishWithDeadline(ctx, msg, packetID, c.writeDeadline(ctx))
}

func (c *Client) sendPublishWithDeadline(ctx context.Context, msg *Message, packetID uint16, deadline time.Time) error {
	if msg == nil {
		return ErrInvalidMessage
	}

	data := c.encodePublishPacket(msg, packetID, true, nil)
	c.updateActivity()
	return c.queuePublishWrite(ctx, data, deadline, msg, msg.QoS > 0)
}

func (c *Client) Subscribe(ctx context.Context, topics map[string]byte) error {
	if err := c.subscribe(ctx, topics); err != nil {
		return err
	}
	for topic, qos := range topics {
		c.subscriptions.setBasic(topic, qos)
	}
	return nil
}

// subscribe performs a protocol subscribe exchange without mutating stored subscription state.

func (c *Client) subscribe(ctx context.Context, topics map[string]byte) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(topics) == 0 {
		return ErrInvalidTopic
	}
	for topic, qos := range topics {
		if topic == "" {
			return ErrInvalidTopic
		}
		if qos > 2 {
			return ErrInvalidQoS
		}
	}

	return c.sendAndAwaitPending(ctx, pendingSubscribe, nil, func(packetID uint16) error {
		return c.sendSubscribe(ctx, packetID, topics)
	})
}

// SubscribeSingle is a convenience method for subscribing to a single topic.

func (c *Client) SubscribeSingle(ctx context.Context, topic string, qos byte) error {
	return c.Subscribe(ctx, map[string]byte{topic: qos})
}

// SubscribeWithOptions subscribes with advanced MQTT 5.0 options.
// For MQTT 3.1.1 connections, advanced options are ignored.

func (c *Client) SubscribeWithOptions(ctx context.Context, opts ...*SubscribeOption) error {
	if err := c.subscribeWithOptions(ctx, opts); err != nil {
		return err
	}
	for _, opt := range opts {
		c.subscriptions.setOption(opt)
	}
	return nil
}

// subscribeWithOptions performs a protocol subscribe exchange without mutating stored subscription state.

func (c *Client) subscribeWithOptions(ctx context.Context, opts []*SubscribeOption) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(opts) == 0 {
		return ErrInvalidTopic
	}
	for _, opt := range opts {
		if opt == nil {
			return ErrInvalidSubscribeOpt
		}
		if opt.Topic == "" {
			return ErrInvalidTopic
		}
		if opt.QoS > 2 {
			return ErrInvalidQoS
		}
		if opt.RetainHandling > 2 {
			return ErrInvalidSubscribeOpt
		}
	}

	return c.sendAndAwaitPending(ctx, pendingSubscribe, nil, func(packetID uint16) error {
		return c.sendSubscribeWithOptions(ctx, packetID, opts)
	})
}

func (c *Client) sendSubscribe(ctx context.Context, packetID uint16, topics map[string]byte) error {
	deadline := c.writeDeadline(ctx)

	if c.isMQTTv5() {
		opts := make([]v5.SubOption, 0, len(topics))
		for topic, qos := range topics {
			opts = append(opts, v5.SubOption{Topic: topic, MaxQoS: qos})
		}
		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
			ID:          packetID,
			Opts:        opts,
		}
		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
	}

	ts := make([]v3.Topic, 0, len(topics))
	for topic, qos := range topics {
		ts = append(ts, v3.Topic{Name: topic, QoS: qos})
	}
	pkt := &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      ts,
	}
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

func (c *Client) sendSubscribeWithOptions(ctx context.Context, packetID uint16, opts []*SubscribeOption) error {
	deadline := c.writeDeadline(ctx)

	if c.isMQTTv5() {
		v5Opts := make([]v5.SubOption, len(opts))
		for i, opt := range opts {
			v5Opts[i] = v5.SubOption{
				Topic:  opt.Topic,
				MaxQoS: opt.QoS,
			}

			if opt.NoLocal {
				noLocal := true
				v5Opts[i].NoLocal = &noLocal
			}

			if opt.RetainAsPublished {
				rap := true
				v5Opts[i].RetainAsPublished = &rap
			}

			if opt.RetainHandling > 0 {
				v5Opts[i].RetainHandling = &opt.RetainHandling
			}
		}

		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
			ID:          packetID,
			Opts:        v5Opts,
		}

		if len(opts) > 0 && opts[0].SubscriptionID > 0 {
			subID := int(opts[0].SubscriptionID)
			pkt.Properties = &v5.SubscribeProperties{
				SubscriptionIdentifier: &subID,
			}
		}

		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
	}

	ts := make([]v3.Topic, len(opts))
	for i, opt := range opts {
		ts[i] = v3.Topic{Name: opt.Topic, QoS: opt.QoS}
	}
	pkt := &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      ts,
	}
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

// Unsubscribe unsubscribes from one or more topics.

func (c *Client) Unsubscribe(ctx context.Context, topics ...string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(topics) == 0 {
		return ErrInvalidTopic
	}

	if err := c.sendAndAwaitPending(ctx, pendingUnsubscribe, nil, func(packetID uint16) error {
		return c.sendUnsubscribe(ctx, packetID, topics)
	}); err != nil {
		return err
	}

	c.subscriptions.remove(topics...)

	return nil
}

func (c *Client) sendUnsubscribe(ctx context.Context, packetID uint16, topics []string) error {
	deadline := c.writeDeadline(ctx)

	if c.isMQTTv5() {
		pkt := &v5.Unsubscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
			ID:          packetID,
			Topics:      topics,
		}
		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
	}

	pkt := &v3.Unsubscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      topics,
	}
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

// SubscribeAsync subscribes in a background goroutine and returns a completion token.

func (c *Client) SubscribeAsync(ctx context.Context, topics map[string]byte) *SubscribeToken {
	tok := &SubscribeToken{token: newToken()}
	go func() {
		tok.complete(c.Subscribe(ctx, topics))
	}()
	return tok
}

// SubscribeWithOptionsAsync subscribes with MQTT 5 options in a background goroutine.

func (c *Client) SubscribeWithOptionsAsync(ctx context.Context, opts ...*SubscribeOption) *SubscribeToken {
	tok := &SubscribeToken{token: newToken()}
	go func() {
		tok.complete(c.SubscribeWithOptions(ctx, opts...))
	}()
	return tok
}

// UnsubscribeAsync unsubscribes in a background goroutine and returns a completion token.

func (c *Client) UnsubscribeAsync(ctx context.Context, topics ...string) *UnsubscribeToken {
	tok := &UnsubscribeToken{token: newToken()}
	go func() {
		tok.complete(c.Unsubscribe(ctx, topics...))
	}()
	return tok
}
