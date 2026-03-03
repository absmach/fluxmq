// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"time"
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

func (c *Client) stageQoSPublish(msg *Message) (uint16, *Message, *pendingOp, error) {
	if msg == nil {
		return 0, nil, nil, ErrInvalidMessage
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return 0, nil, nil, ErrMaxInflight
	}

	staged := msg.Copy()
	if staged == nil {
		return 0, nil, nil, ErrInvalidMessage
	}
	staged.PacketID = packetID

	if err := c.store.StoreOutbound(packetID, staged); err != nil {
		return 0, nil, nil, err
	}

	op, err := c.pending.add(packetID, pendingPublish, staged)
	if err != nil {
		_ = c.store.DeleteOutbound(packetID)
		return 0, nil, nil, err
	}

	return packetID, staged, op, nil
}

func (c *Client) rollbackStagedPublish(packetID uint16) {
	c.pending.remove(packetID)
	c.store.DeleteOutbound(packetID)
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	msg := NewMessage(topic, payload, qos, retain)
	_, _, err := c.PublishMessage(ctx, msg, true)
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

// PublishMessage sends a message with optional MQTT 5.0 publish properties.
// For MQTT 3.1.1, publish properties are ignored.
func (c *Client) PublishMessage(ctx context.Context, msg *Message, waitAck bool) (*pendingOp, uint16, error) {
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

	packetID, publishMsg, op, err := c.stageQoSPublish(msg)
	if err != nil {
		return nil, 0, err
	}

	if err := c.sendPublish(ctx, publishMsg, packetID); err != nil {
		c.rollbackStagedPublish(packetID)
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
	op, packetID, err := c.PublishMessage(ctx, msg, false)
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
	op, packetID, err := c.PublishMessage(ctx, msg, false)
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
		c.updateActivity()
		return c.queueWrite(c.encodeSubscribePacket(packetID, opts), c.writeDeadline(ctx))
	})
}

func (c *Client) sendSubscribe(ctx context.Context, packetID uint16, topics map[string]byte) error {
	opts := make([]*SubscribeOption, 0, len(topics))
	for topic, qos := range topics {
		opts = append(opts, &SubscribeOption{Topic: topic, QoS: qos})
	}
	c.updateActivity()
	return c.queueWrite(c.encodeSubscribePacket(packetID, opts), c.writeDeadline(ctx))
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
		c.updateActivity()
		return c.queueWrite(c.encodeUnsubscribePacket(packetID, topics), c.writeDeadline(ctx))
	}); err != nil {
		return err
	}

	c.subscriptions.remove(topics...)

	return nil
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
