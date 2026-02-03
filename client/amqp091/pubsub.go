// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"strconv"
	"strings"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// PublishOptions configures topic message publishing.
type PublishOptions struct {
	Topic      string            // Topic name
	Exchange   string            // Optional exchange override
	RoutingKey string            // Optional routing key override
	Payload    []byte            // Message payload
	Properties map[string]string // Optional message properties
	Mandatory  bool
	Immediate  bool
}

// MessageHandler is called when a pub/sub message is received.
type MessageHandler func(msg *Message)

// Message represents a pub/sub message received via basic.consume.
type Message struct {
	amqp091.Delivery
	Topic  string
	client *Client
}

// Ack acknowledges successful processing of a message.
func (m *Message) Ack() error {
	return m.withChannelLock(func() error {
		return m.Delivery.Ack(false)
	})
}

// Nack negatively acknowledges the message, triggering a retry.
func (m *Message) Nack() error {
	return m.withChannelLock(func() error {
		return m.Delivery.Nack(false, true)
	})
}

// Reject rejects the message.
func (m *Message) Reject() error {
	return m.withChannelLock(func() error {
		return m.Delivery.Reject(false)
	})
}

func (m *Message) withChannelLock(fn func() error) error {
	if m.client == nil {
		return fn()
	}
	m.client.chMu.Lock()
	defer m.client.chMu.Unlock()
	return fn()
}

// SubscribeOptions configures topic subscriptions.
type SubscribeOptions struct {
	Topic       string
	AutoAck     bool
	Exclusive   bool
	NoLocal     bool
	NoWait      bool
	ConsumerTag string
	Arguments   amqp091.Table
}

type topicSubscription struct {
	topicFilter string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	consumerTag string
	arguments   amqp091.Table
	handler     MessageHandler
	done        chan struct{}
}

func (s *topicSubscription) close() {
	select {
	case <-s.done:
		return
	default:
		close(s.done)
	}
}

// Publish sends a message to a topic using the default exchange.
func (c *Client) Publish(topic string, payload []byte) error {
	return c.PublishWithOptions(&PublishOptions{
		Topic:   topic,
		Payload: payload,
	})
}

// PublishWithOptions publishes a message with optional properties and routing control.
func (c *Client) PublishWithOptions(opts *PublishOptions) error {
	if opts == nil {
		return ErrInvalidTopic
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}

	exchange := opts.Exchange
	routingKey := opts.RoutingKey
	if routingKey == "" {
		routingKey = opts.Topic
	}
	if routingKey == "" {
		return ErrInvalidTopic
	}

	publishing := amqp091.Publishing{
		Timestamp: time.Now(),
		Body:      opts.Payload,
	}

	applyProperties(&publishing, opts.Properties)
	return c.publish(exchange, routingKey, publishing, opts.Mandatory, opts.Immediate)
}

// Subscribe registers a pub/sub consumer for a topic filter with auto-ack enabled.
func (c *Client) Subscribe(topic string, handler MessageHandler) error {
	return c.SubscribeWithOptions(&SubscribeOptions{
		Topic:   topic,
		AutoAck: true,
	}, handler)
}

// SubscribeWithOptions registers a pub/sub consumer for a topic filter.
func (c *Client) SubscribeWithOptions(opts *SubscribeOptions, handler MessageHandler) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}
	if opts == nil || opts.Topic == "" {
		return ErrInvalidTopic
	}
	if handler == nil {
		return ErrNilHandler
	}

	topicFilter := opts.Topic
	consumerTag := opts.ConsumerTag
	if consumerTag == "" {
		consumerTag = "ctag-" + strings.ReplaceAll(topicFilter, "/", "-") + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	c.subsMu.Lock()
	if _, exists := c.topicSubs[topicFilter]; exists {
		c.subsMu.Unlock()
		return ErrAlreadySubscribed
	}

	sub := &topicSubscription{
		topicFilter: topicFilter,
		autoAck:     opts.AutoAck,
		exclusive:   opts.Exclusive,
		noLocal:     opts.NoLocal,
		noWait:      opts.NoWait,
		consumerTag: consumerTag,
		arguments:   opts.Arguments,
		handler:     handler,
		done:        make(chan struct{}),
	}
	c.topicSubs[topicFilter] = sub
	c.subsMu.Unlock()

	if err := c.subscribeTopic(sub); err != nil {
		c.subsMu.Lock()
		delete(c.topicSubs, topicFilter)
		c.subsMu.Unlock()
		return err
	}

	return nil
}

// Unsubscribe removes a pub/sub subscription for a topic filter.
func (c *Client) Unsubscribe(topic string) error {
	c.subsMu.Lock()
	sub, ok := c.topicSubs[topic]
	if ok {
		delete(c.topicSubs, topic)
	}
	c.subsMu.Unlock()

	if !ok {
		return nil
	}

	sub.close()

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.Cancel(sub.consumerTag, false)
}

func (c *Client) subscribeTopic(sub *topicSubscription) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	deliveries, err := ch.Consume(
		sub.topicFilter,
		sub.consumerTag,
		sub.autoAck,
		sub.exclusive,
		sub.noLocal,
		sub.noWait,
		sub.arguments,
	)
	c.chMu.Unlock()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-sub.done:
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				topic := d.RoutingKey
				if d.Exchange != "" {
					topic = d.Exchange + "/" + d.RoutingKey
				}
				sub.handler(&Message{
					Delivery: d,
					Topic:    topic,
					client:   c,
				})
			}
		}
	}()

	return nil
}
