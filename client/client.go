// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/client/amqp"
	"github.com/absmach/fluxmq/client/mqtt"
	"github.com/absmach/fluxmq/topics"
)

// Client provides a unified messaging API over MQTT or AMQP transports.
type Client struct {
	defaultProtocol Protocol

	mqtt *mqtt.Client
	amqp *amqp.Client

	mu    sync.RWMutex
	subs  map[string]MessageHandler
	qsubs map[string]MessageHandler
}

// New creates a unified client configured with one or both transports.
func New(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = NewConfig()
	}
	if cfg.MQTT == nil && cfg.AMQP == nil {
		return nil, ErrNoTransport
	}

	c := &Client{
		defaultProtocol: cfg.DefaultProtocol,
		subs:            make(map[string]MessageHandler),
		qsubs:           make(map[string]MessageHandler),
	}

	if cfg.MQTT != nil {
		mqttClient, err := c.newMQTTClient(cfg.MQTT)
		if err != nil {
			return nil, err
		}
		c.mqtt = mqttClient
	}
	if cfg.AMQP != nil {
		amqpClient, err := amqp.New(cfg.AMQP)
		if err != nil {
			return nil, err
		}
		c.amqp = amqpClient
	}

	return c, nil
}

// NewMQTT creates a unified client backed by the MQTT client.
func NewMQTT(opts *mqtt.Options) (*Client, error) {
	if opts == nil {
		opts = mqtt.NewOptions()
	}
	return New(NewConfig().SetMQTT(opts).SetDefaultProtocol(ProtocolMQTT))
}

// NewAMQP creates a unified client backed by the AMQP client.
func NewAMQP(opts *amqp.Options) (*Client, error) {
	if opts == nil {
		opts = amqp.NewOptions()
	}
	return New(NewConfig().SetAMQP(opts).SetDefaultProtocol(ProtocolAMQP))
}

// MQTT returns the underlying MQTT client, if configured.
func (c *Client) MQTT() *mqtt.Client { return c.mqtt }

// AMQP returns the underlying AMQP client, if configured.
func (c *Client) AMQP() *amqp.Client { return c.amqp }

// Connect establishes a connection to the broker.
func (c *Client) Connect(ctx context.Context) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	return doWithContext(ctx, func() error {
		if c.mqtt != nil {
			if err := c.mqtt.Connect(); err != nil {
				return fmt.Errorf("%w: %v", ErrConnectFailed, err)
			}
		}
		if c.amqp != nil {
			if err := c.amqp.Connect(); err != nil {
				if c.mqtt != nil {
					_ = c.mqtt.Close()
				}
				return fmt.Errorf("%w: %v", ErrConnectFailed, err)
			}
		}
		return nil
	})
}

// Close terminates the connection.
func (c *Client) Close(ctx context.Context) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	return doWithContext(ctx, func() error {
		var errs []error
		if c.mqtt != nil {
			if err := c.mqtt.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%w: %v", ErrCloseFailed, err))
			}
		}
		if c.amqp != nil {
			if err := c.amqp.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%w: %v", ErrCloseFailed, err))
			}
		}
		return errors.Join(errs...)
	})
}

// IsConnected reports whether the client is connected.
func (c *Client) IsConnected() bool {
	switch {
	case c.mqtt != nil && c.amqp != nil:
		return c.mqtt.IsConnected() && c.amqp.IsConnected()
	case c.mqtt != nil:
		return c.mqtt.IsConnected()
	case c.amqp != nil:
		return c.amqp.IsConnected()
	default:
		return false
	}
}

// Publish publishes a message to a topic.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, opts ...Option) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	po := buildPublishOptions(opts)
	protocol, err := c.resolveProtocol(po.Protocol)
	if err != nil {
		return err
	}

	return doWithContext(ctx, func() error {
		if protocol == ProtocolMQTT {
			qos := byte(0)
			if po.QoS != nil {
				qos = *po.QoS
			}
			retain := false
			if po.Retain != nil {
				retain = *po.Retain
			}
			msg := mqtt.NewMessage(topic, payload, qos, retain)
			msg.UserProperties = mqttUserProps(po.Properties)
			if err := c.mqtt.PublishMessage(ctx, msg); err != nil {
				return fmt.Errorf("%w: %v", ErrPublishFailed, err)
			}
			return nil
		}

		amqpOpts := &amqp.PublishOptions{
			Exchange:   po.Exchange,
			RoutingKey: po.RoutingKey,
			Topic:      topic,
			Payload:    payload,
			Properties: amqpHeaders(po.Properties),
		}
		if po.Mandatory != nil {
			amqpOpts.Mandatory = *po.Mandatory
		}
		if po.Immediate != nil {
			amqpOpts.Immediate = *po.Immediate
		}
		if err := c.amqp.PublishWithOptionsContext(ctx, amqpOpts); err != nil {
			return fmt.Errorf("%w: %v", ErrPublishFailed, err)
		}
		return nil
	})
}

// Subscribe subscribes to a topic and routes matching messages to handler.
func (c *Client) Subscribe(ctx context.Context, topic string, handler MessageHandler, opts ...Option) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	if handler == nil {
		return fmt.Errorf("%w: handler cannot be nil", ErrSubscribeFailed)
	}

	so := buildSubscribeOptions(opts)
	protocol, err := c.resolveProtocol(so.Protocol)
	if err != nil {
		return err
	}
	return doWithContext(ctx, func() error {
		if protocol == ProtocolMQTT {
			qos := byte(0)
			if so.QoS != nil {
				qos = *so.QoS
			}
			if err := c.mqtt.SubscribeSingle(ctx, topic, qos); err != nil {
				return fmt.Errorf("%w: %v", ErrSubscribeFailed, err)
			}
			c.mu.Lock()
			c.subs[topic] = handler
			c.mu.Unlock()
			return nil
		}

		autoAck := true
		if so.AutoAck != nil {
			autoAck = *so.AutoAck
		}
		subOpts := &amqp.SubscribeOptions{Topic: topic, AutoAck: autoAck}
		if err := c.amqp.SubscribeWithOptions(subOpts, func(msg *amqp.Message) {
			handler(amqpToMessage(msg, ""))
		}); err != nil {
			return fmt.Errorf("%w: %v", ErrSubscribeFailed, err)
		}
		return nil
	})
}

// Unsubscribe removes a topic subscription.
func (c *Client) Unsubscribe(ctx context.Context, topic string, opts ...Option) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	so := buildSubscribeOptions(opts)
	protocol, err := c.resolveProtocol(so.Protocol)
	if err != nil {
		return err
	}

	return doWithContext(ctx, func() error {
		if protocol == ProtocolMQTT {
			if err := c.mqtt.Unsubscribe(ctx, topic); err != nil {
				return fmt.Errorf("%w: %v", ErrUnsubFailed, err)
			}
			c.mu.Lock()
			delete(c.subs, topic)
			c.mu.Unlock()
			return nil
		}

		if err := c.amqp.Unsubscribe(topic); err != nil {
			return fmt.Errorf("%w: %v", ErrUnsubFailed, err)
		}
		return nil
	})
}

// PublishToQueue publishes a message to a durable queue.
func (c *Client) PublishToQueue(ctx context.Context, queue string, payload []byte, opts ...Option) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	po := buildPublishOptions(opts)
	protocol, err := c.resolveProtocol(po.Protocol)
	if err != nil {
		return err
	}

	return doWithContext(ctx, func() error {
		if protocol == ProtocolMQTT {
			qos := byte(1)
			if po.QoS != nil {
				qos = *po.QoS
			}
			qopts := &mqtt.QueuePublishOptions{
				QueueName:  queue,
				Payload:    payload,
				Properties: mqttUserProps(po.Properties),
				QoS:        qos,
			}
			if err := c.mqtt.PublishToQueueWithOptions(ctx, qopts); err != nil {
				return fmt.Errorf("%w: %v", ErrQueuePublishFailed, err)
			}
			return nil
		}

		qopts := &amqp.QueuePublishOptions{
			QueueName:  queue,
			Payload:    payload,
			Properties: amqpHeaders(po.Properties),
		}
		if err := c.amqp.PublishToQueueWithOptionsContext(ctx, qopts); err != nil {
			return fmt.Errorf("%w: %v", ErrQueuePublishFailed, err)
		}
		return nil
	})
}

// SubscribeToQueue subscribes to a queue with a consumer group.
func (c *Client) SubscribeToQueue(ctx context.Context, queue, group string, handler MessageHandler, opts ...Option) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	if handler == nil {
		return fmt.Errorf("%w: handler cannot be nil", ErrQueueSubscribeFailed)
	}
	so := buildSubscribeOptions(opts)
	protocol, err := c.resolveProtocol(so.Protocol)
	if err != nil {
		return err
	}

	return doWithContext(ctx, func() error {
		if protocol == ProtocolMQTT {
			if err := c.mqtt.SubscribeToQueue(ctx, queue, group, func(msg *mqtt.QueueMessage) {
				handler(mqttQueueToMessage(msg, queue))
			}); err != nil {
				return fmt.Errorf("%w: %v", ErrQueueSubscribeFailed, err)
			}
			c.mu.Lock()
			c.qsubs[queue] = handler
			c.mu.Unlock()
			return nil
		}

		if err := c.amqp.SubscribeToQueue(queue, group, func(msg *amqp.QueueMessage) {
			handler(amqpQueueToMessage(msg, queue))
		}); err != nil {
			return fmt.Errorf("%w: %v", ErrQueueSubscribeFailed, err)
		}
		return nil
	})
}

// UnsubscribeFromQueue removes a queue subscription.
func (c *Client) UnsubscribeFromQueue(ctx context.Context, queue string, opts ...Option) error {
	if c.mqtt == nil && c.amqp == nil {
		return ErrNoTransport
	}
	so := buildSubscribeOptions(opts)
	protocol, err := c.resolveProtocol(so.Protocol)
	if err != nil {
		return err
	}

	return doWithContext(ctx, func() error {
		if protocol == ProtocolMQTT {
			if err := c.mqtt.UnsubscribeFromQueue(ctx, queue); err != nil {
				return fmt.Errorf("%w: %v", ErrQueueUnsubFailed, err)
			}
			c.mu.Lock()
			delete(c.qsubs, queue)
			c.mu.Unlock()
			return nil
		}

		if err := c.amqp.UnsubscribeFromQueue(queue); err != nil {
			return fmt.Errorf("%w: %v", ErrQueueUnsubFailed, err)
		}
		return nil
	})
}

func (c *Client) newMQTTClient(opts *mqtt.Options) (*mqtt.Client, error) {
	if opts == nil {
		opts = mqtt.NewOptions()
	}

	userOnMessageV2 := opts.OnMessageV2
	userOnMessage := opts.OnMessage

	cloned := *opts
	cloned.OnMessageV2 = func(msg *mqtt.Message) {
		c.dispatchMQTT(msg)
		if userOnMessageV2 != nil {
			userOnMessageV2(msg)
			return
		}
		if userOnMessage != nil {
			userOnMessage(msg.Topic, msg.Payload, msg.QoS)
		}
	}
	cloned.OnMessage = nil

	return mqtt.New(&cloned)
}

func (c *Client) resolveProtocol(requested Protocol) (Protocol, error) {
	switch requested {
	case "":
		return c.defaultProtocolOrFallback()
	case ProtocolMQTT:
		if c.mqtt == nil {
			return "", fmt.Errorf("%w: %s", ErrNoRouteProtocol, ProtocolMQTT)
		}
		return ProtocolMQTT, nil
	case ProtocolAMQP:
		if c.amqp == nil {
			return "", fmt.Errorf("%w: %s", ErrNoRouteProtocol, ProtocolAMQP)
		}
		return ProtocolAMQP, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidProtocol, requested)
	}
}

func (c *Client) defaultProtocolOrFallback() (Protocol, error) {
	if c.defaultProtocol == ProtocolMQTT && c.mqtt != nil {
		return ProtocolMQTT, nil
	}
	if c.defaultProtocol == ProtocolAMQP && c.amqp != nil {
		return ProtocolAMQP, nil
	}
	if c.mqtt != nil {
		return ProtocolMQTT, nil
	}
	if c.amqp != nil {
		return ProtocolAMQP, nil
	}
	return "", ErrNoTransport
}

func (c *Client) dispatchMQTT(msg *mqtt.Message) {
	if msg == nil {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for filter, handler := range c.subs {
		if topics.TopicMatch(filter, msg.Topic) {
			handler(mqttToMessage(msg))
		}
	}
}

func mqttToMessage(msg *mqtt.Message) *Message {
	if msg == nil {
		return nil
	}

	props := make(map[string]string)
	props[propMQTTQoS] = strconv.Itoa(int(msg.QoS))
	props[propMQTTRetain] = strconv.FormatBool(msg.Retain)
	props[propMQTTDup] = strconv.FormatBool(msg.Dup)
	for k, v := range msg.UserProperties {
		props[propMQTTUserPref+k] = v
	}

	return &Message{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		Properties: props,
		Timestamp:  msg.Timestamp,
	}
}

func mqttQueueToMessage(msg *mqtt.QueueMessage, queue string) *Message {
	if msg == nil {
		return nil
	}

	m := mqttToMessage(msg.Message)
	if m == nil {
		return nil
	}
	m.Queue = queue
	m.Offset = msg.Offset
	m.ackFn = func() error { return msg.Ack(context.Background()) }
	m.nackFn = func() error { return msg.Nack(context.Background()) }
	m.rejectFn = func() error { return msg.Reject(context.Background()) }
	return m
}

func amqpToMessage(msg *amqp.Message, queue string) *Message {
	if msg == nil {
		return nil
	}

	props := make(map[string]string)
	for k, v := range msg.Headers {
		props[propAMQPHeadersPref+k] = fmt.Sprint(v)
	}

	return &Message{
		Topic:      msg.Topic,
		Payload:    msg.Body,
		Properties: props,
		Timestamp:  msg.Timestamp,
		Queue:      queue,
		ackFn:      msg.Ack,
		nackFn:     msg.Nack,
		rejectFn:   msg.Reject,
	}
}

func amqpQueueToMessage(msg *amqp.QueueMessage, queue string) *Message {
	if msg == nil {
		return nil
	}

	props := make(map[string]string)
	for k, v := range msg.Headers {
		props[propAMQPHeadersPref+k] = fmt.Sprint(v)
	}

	m := &Message{
		Topic:      msg.RoutingKey,
		Payload:    msg.Body,
		Properties: props,
		Timestamp:  msg.Timestamp,
		Queue:      queue,
		ackFn:      msg.Ack,
		nackFn:     msg.Nack,
		rejectFn:   msg.Reject,
	}

	if offset, ok := msg.StreamOffset(); ok {
		m.Offset = offset
	}
	return m
}

func mqttUserProps(props map[string]string) map[string]string {
	if len(props) == 0 {
		return nil
	}

	out := make(map[string]string)
	for k, v := range props {
		switch {
		case strings.HasPrefix(k, propMQTTUserPref):
			out[strings.TrimPrefix(k, propMQTTUserPref)] = v
		case strings.HasPrefix(k, "mqtt."):
			continue
		case strings.HasPrefix(k, "amqp."):
			continue
		default:
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func amqpHeaders(props map[string]string) map[string]string {
	if len(props) == 0 {
		return nil
	}

	out := make(map[string]string)
	for k, v := range props {
		switch {
		case strings.HasPrefix(k, propAMQPHeadersPref):
			out[strings.TrimPrefix(k, propAMQPHeadersPref)] = v
		case strings.HasPrefix(k, "amqp."):
			continue
		case strings.HasPrefix(k, "mqtt."):
			continue
		default:
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func doWithContext(ctx context.Context, fn func() error) error {
	if ctx == nil {
		return fn()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}
