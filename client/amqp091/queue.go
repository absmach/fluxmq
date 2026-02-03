// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"strconv"
	"strings"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// QueuePublishOptions configures queue message publishing.
type QueuePublishOptions struct {
	QueueName  string            // Queue name (without $queue/ prefix)
	Payload    []byte            // Message payload
	Properties map[string]string // Optional message properties
}

// QueueMessageHandler is called when a queue message is received.
type QueueMessageHandler func(msg *QueueMessage)

// QueueMessage represents a message received from a durable queue.
type QueueMessage struct {
	amqp091.Delivery
	queueName string
	client    *Client
}

// Ack acknowledges successful processing of a message.
func (qm *QueueMessage) Ack() error {
	return qm.withChannelLock(func() error {
		return qm.Delivery.Ack(false)
	})
}

// Nack negatively acknowledges the message, triggering a retry.
func (qm *QueueMessage) Nack() error {
	return qm.withChannelLock(func() error {
		return qm.Delivery.Nack(false, true)
	})
}

// Reject rejects the message, sending it to the dead-letter queue.
func (qm *QueueMessage) Reject() error {
	return qm.withChannelLock(func() error {
		return qm.Delivery.Reject(false)
	})
}

func (qm *QueueMessage) withChannelLock(fn func() error) error {
	if qm.client == nil {
		return fn()
	}
	qm.client.chMu.Lock()
	defer qm.client.chMu.Unlock()
	return fn()
}

type subscription struct {
	queueTopic  string
	consumerTag string
	done        chan struct{}
}

func (s *subscription) close() {
	select {
	case <-s.done:
		return
	default:
		close(s.done)
	}
}

// PublishToQueue publishes a message to a durable queue.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) PublishToQueue(queueName string, payload []byte) error {
	return c.PublishToQueueWithOptions(&QueuePublishOptions{
		QueueName: queueName,
		Payload:   payload,
	})
}

// PublishToQueueWithOptions publishes a message to a durable queue with full control.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) PublishToQueueWithOptions(opts *QueuePublishOptions) error {
	if opts == nil {
		return ErrInvalidQueueName
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}
	if opts.QueueName == "" {
		return ErrInvalidQueueName
	}

	queueTopic := normalizeQueueTopic(opts.QueueName)

	publishing := amqp091.Publishing{
		Timestamp: time.Now(),
		Body:      opts.Payload,
	}

	applyProperties(&publishing, opts.Properties)

	// Queue publishes always use default exchange with routing key as the full queue topic.
	return c.publish("", queueTopic, publishing, false, false)
}

// SubscribeToQueue subscribes to a durable queue with a consumer group.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
// The handler will be called for each message received from the queue.
func (c *Client) SubscribeToQueue(queueName, consumerGroup string, handler QueueMessageHandler) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}
	if queueName == "" {
		return ErrInvalidQueueName
	}
	if handler == nil {
		return ErrNilHandler
	}

	queueTopic := normalizeQueueTopic(queueName)
	consumerTag := "ctag-" + strings.ReplaceAll(queueTopic, "/", "-") + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	args := amqp091.Table{}
	if consumerGroup != "" {
		args["x-consumer-group"] = consumerGroup
	}

	c.subsMu.Lock()
	if _, exists := c.subs[queueTopic]; exists {
		c.subsMu.Unlock()
		return ErrAlreadySubscribed
	}
	c.subsMu.Unlock()

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	deliveries, err := ch.Consume(
		queueTopic,
		consumerTag,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		args,
	)
	c.chMu.Unlock()
	if err != nil {
		return err
	}

	sub := &subscription{
		queueTopic:  queueTopic,
		consumerTag: consumerTag,
		done:        make(chan struct{}),
	}

	c.subsMu.Lock()
	c.subs[queueTopic] = sub
	c.subsMu.Unlock()

	go func() {
		for {
			select {
			case <-sub.done:
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				handler(&QueueMessage{
					Delivery:  d,
					queueName: queueName,
					client:    c,
				})
			}
		}
	}()

	return nil
}

// UnsubscribeFromQueue unsubscribes from a durable queue.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) UnsubscribeFromQueue(queueName string) error {
	queueTopic := normalizeQueueTopic(queueName)

	c.subsMu.Lock()
	sub, ok := c.subs[queueTopic]
	if ok {
		delete(c.subs, queueTopic)
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

func normalizeQueueTopic(queueName string) string {
	if strings.HasPrefix(queueName, "$queue/") {
		return queueName
	}
	return "$queue/" + queueName
}

func applyProperties(p *amqp091.Publishing, props map[string]string) {
	if len(props) == 0 {
		return
	}

	for key, value := range props {
		switch strings.ToLower(key) {
		case "content-type":
			p.ContentType = value
		case "content-encoding":
			p.ContentEncoding = value
		case "correlation-id":
			p.CorrelationId = value
		case "reply-to":
			p.ReplyTo = value
		case "message-id":
			p.MessageId = value
		case "type":
			p.Type = value
		case "app-id":
			p.AppId = value
		case "expiration":
			p.Expiration = value
		default:
			if p.Headers == nil {
				p.Headers = amqp091.Table{}
			}
			p.Headers[key] = value
		}
	}
}
