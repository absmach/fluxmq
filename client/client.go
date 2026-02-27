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

	amqpclient "github.com/absmach/fluxmq/client/amqp"
	mqttclient "github.com/absmach/fluxmq/client/mqtt"
	"github.com/absmach/fluxmq/topics"
)

// Client provides a unified messaging API over MQTT or AMQP transports.
type Client struct {
	defaultProtocol Protocol

	mqtt *mqttclient.Client
	amqp *amqpclient.Client

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
		amqpClient, err := amqpclient.New(cfg.AMQP)
		if err != nil {
			return nil, err
		}
		c.amqp = amqpClient
	}

	return c, nil
}

// NewMQTT creates a unified client backed by the MQTT client.
func NewMQTT(opts *mqttclient.Options) (*Client, error) {
	if opts == nil {
		opts = mqttclient.NewOptions()
	}
	return New(NewConfig().SetMQTT(opts).SetDefaultProtocol(ProtocolMQTT))
}

// NewAMQP creates a unified client backed by the AMQP client.
func NewAMQP(opts *amqpclient.Options) (*Client, error) {
	if opts == nil {
		opts = amqpclient.NewOptions()
	}
	return New(NewConfig().SetAMQP(opts).SetDefaultProtocol(ProtocolAMQP))
}

// MQTT returns the underlying MQTT client, if configured.
func (c *Client) MQTT() *mqttclient.Client { return c.mqtt }

// AMQP returns the underlying AMQP client, if configured.
func (c *Client) AMQP() *amqpclient.Client { return c.amqp }

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
			msg := mqttclient.NewMessage(topic, payload, qos, retain)
			msg.UserProperties = mqttUserProps(po.Properties)
			if err := c.mqtt.PublishMessageContext(ctx, msg); err != nil {
				return fmt.Errorf("%w: %v", ErrPublishFailed, err)
			}
			return nil
		}

		amqpOpts := &amqpclient.PublishOptions{
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
			if err := c.mqtt.SubscribeSingle(topic, qos); err != nil {
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
		subOpts := &amqpclient.SubscribeOptions{Topic: topic, AutoAck: autoAck}
		if err := c.amqp.SubscribeWithOptions(subOpts, func(msg *amqpclient.Message) {
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
			if err := c.mqtt.Unsubscribe(topic); err != nil {
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
			qopts := &mqttclient.QueuePublishOptions{
				QueueName:  queue,
				Payload:    payload,
				Properties: mqttUserProps(po.Properties),
				QoS:        qos,
			}
			if err := c.mqtt.PublishToQueueWithOptionsContext(ctx, qopts); err != nil {
				return fmt.Errorf("%w: %v", ErrQueuePublishFailed, err)
			}
			return nil
		}

		qopts := &amqpclient.QueuePublishOptions{
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
			if err := c.mqtt.SubscribeToQueue(queue, group, func(msg *mqttclient.QueueMessage) {
				handler(mqttQueueToMessage(msg, queue))
			}); err != nil {
				return fmt.Errorf("%w: %v", ErrQueueSubscribeFailed, err)
			}
			c.mu.Lock()
			c.qsubs[queue] = handler
			c.mu.Unlock()
			return nil
		}

		if err := c.amqp.SubscribeToQueue(queue, group, func(msg *amqpclient.QueueMessage) {
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
			if err := c.mqtt.UnsubscribeFromQueue(queue); err != nil {
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

func (c *Client) newMQTTClient(opts *mqttclient.Options) (*mqttclient.Client, error) {
	if opts == nil {
		opts = mqttclient.NewOptions()
	}

	userOnMessageV2 := opts.OnMessageV2
	userOnMessage := opts.OnMessage

	cloned := *opts
	cloned.OnMessageV2 = func(msg *mqttclient.Message) {
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

	return mqttclient.New(&cloned)
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

func (c *Client) dispatchMQTT(msg *mqttclient.Message) {
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

func mqttToMessage(msg *mqttclient.Message) *Message {
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

func mqttQueueToMessage(msg *mqttclient.QueueMessage, queue string) *Message {
	if msg == nil {
		return nil
	}

	m := mqttToMessage(msg.Message)
	if m == nil {
		return nil
	}
	m.Queue = queue
	m.Offset = msg.Offset
	m.ackFn = msg.Ack
	m.nackFn = msg.Nack
	m.rejectFn = msg.Reject
	return m
}

func amqpToMessage(msg *amqpclient.Message, queue string) *Message {
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

func amqpQueueToMessage(msg *amqpclient.QueueMessage, queue string) *Message {
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
