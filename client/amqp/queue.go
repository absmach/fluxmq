// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"math"
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

// StreamQueueOptions configures a stream queue declaration.
type StreamQueueOptions struct {
	Name              string
	Durable           bool
	AutoDelete        bool
	Exclusive         bool
	NoWait            bool
	MaxAge            string // e.g. "7D", "1h"
	MaxLengthBytes    int64
	MaxLengthMessages int64
}

// StreamConsumeOptions configures a stream queue subscription.
type StreamConsumeOptions struct {
	QueueName     string
	ConsumerGroup string
	Offset        string // "first", "last", "next", "offset=123", "timestamp=..."
	AutoAck       bool
	AutoCommit    *bool // nil = default (true), false = manual commit required
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	ConsumerTag   string
	Arguments     amqp091.Table
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

// StreamOffset returns the stream offset if present.
func (qm *QueueMessage) StreamOffset() (uint64, bool) {
	return headerUint64(qm.Headers, "x-stream-offset")
}

// StreamTimestamp returns the stream timestamp (unix millis) if present.
func (qm *QueueMessage) StreamTimestamp() (int64, bool) {
	if v, ok := headerInt64(qm.Headers, "x-stream-timestamp"); ok {
		return v, true
	}
	return 0, false
}

// WorkAcked reports whether the primary work group has acknowledged this offset.
func (qm *QueueMessage) WorkAcked() (bool, bool) {
	return headerBool(qm.Headers, "x-work-acked")
}

// WorkCommittedOffset returns the primary group's committed offset if present.
func (qm *QueueMessage) WorkCommittedOffset() (uint64, bool) {
	return headerUint64(qm.Headers, "x-work-committed-offset")
}

// WorkGroup returns the primary work group name if present.
func (qm *QueueMessage) WorkGroup() (string, bool) {
	return headerString(qm.Headers, "x-work-group")
}

func (qm *QueueMessage) withChannelLock(fn func() error) error {
	if qm.client == nil {
		return fn()
	}
	qm.client.chMu.Lock()
	defer qm.client.chMu.Unlock()
	return fn()
}

type queueSubscription struct {
	queueName     string
	queueTopic    string
	consumerGroup string
	consumerTag   string
	handler       QueueMessageHandler
	done          chan struct{}
}

func (s *queueSubscription) close() {
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
		return ErrNilOptions
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

// PublishToStream publishes a message to a stream queue (RabbitMQ-style queue name).
func (c *Client) PublishToStream(queueName string, payload []byte, props map[string]string) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}
	if queueName == "" {
		return ErrInvalidQueueName
	}

	publishing := amqp091.Publishing{
		Timestamp: time.Now(),
		Body:      payload,
	}
	applyProperties(&publishing, props)
	return c.publish("", queueName, publishing, false, false)
}

// DeclareStreamQueue declares a stream queue with retention settings.
func (c *Client) DeclareStreamQueue(opts *StreamQueueOptions) (string, error) {
	if !c.connected.Load() {
		return "", ErrNotConnected
	}
	if opts == nil {
		return "", ErrNilOptions
	}

	args := amqp091.Table{
		"x-queue-type": "stream",
	}
	if opts.MaxAge != "" {
		args["x-max-age"] = opts.MaxAge
	}
	if opts.MaxLengthBytes > 0 {
		args["x-max-length-bytes"] = opts.MaxLengthBytes
	}
	if opts.MaxLengthMessages > 0 {
		args["x-max-length"] = opts.MaxLengthMessages
	}

	ch, err := c.channel()
	if err != nil {
		return "", err
	}

	c.chMu.Lock()
	q, err := ch.QueueDeclare(
		opts.Name,
		opts.Durable,
		opts.AutoDelete,
		opts.Exclusive,
		opts.NoWait,
		args,
	)
	c.chMu.Unlock()
	if err != nil {
		return "", err
	}
	return q.Name, nil
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

	c.subsMu.Lock()
	if _, exists := c.queueSubs[queueTopic]; exists {
		c.subsMu.Unlock()
		return ErrAlreadySubscribed
	}

	sub := &queueSubscription{
		queueName:     queueName,
		queueTopic:    queueTopic,
		consumerGroup: consumerGroup,
		consumerTag:   "ctag-" + strings.ReplaceAll(queueTopic, "/", "-") + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		handler:       handler,
		done:          make(chan struct{}),
	}
	c.queueSubs[queueTopic] = sub
	c.subsMu.Unlock()

	if err := c.subscribeQueue(sub); err != nil {
		c.subsMu.Lock()
		delete(c.queueSubs, queueTopic)
		c.subsMu.Unlock()
		return err
	}

	return nil
}

// SubscribeToStream subscribes to a stream queue with cursor control.
func (c *Client) SubscribeToStream(opts *StreamConsumeOptions, handler QueueMessageHandler) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}
	if opts == nil {
		return ErrNilOptions
	}
	if opts.QueueName == "" {
		return ErrInvalidQueueName
	}
	if handler == nil {
		return ErrNilHandler
	}

	queueName := opts.QueueName
	c.subsMu.Lock()
	if _, exists := c.queueSubs[queueName]; exists {
		c.subsMu.Unlock()
		return ErrAlreadySubscribed
	}

	consumerTag := opts.ConsumerTag
	if consumerTag == "" {
		consumerTag = "ctag-" + strings.ReplaceAll(queueName, "/", "-") + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	sub := &queueSubscription{
		queueName:     queueName,
		queueTopic:    queueName,
		consumerGroup: opts.ConsumerGroup,
		consumerTag:   consumerTag,
		handler:       handler,
		done:          make(chan struct{}),
	}
	c.queueSubs[queueName] = sub
	c.subsMu.Unlock()

	if err := c.subscribeStream(sub, opts); err != nil {
		c.subsMu.Lock()
		delete(c.queueSubs, queueName)
		c.subsMu.Unlock()
		return err
	}

	return nil
}

// UnsubscribeFromQueue unsubscribes from a durable queue.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) UnsubscribeFromQueue(queueName string) error {
	queueTopic := normalizeQueueTopic(queueName)

	c.subsMu.Lock()
	sub, ok := c.queueSubs[queueTopic]
	if ok {
		delete(c.queueSubs, queueTopic)
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

// UnsubscribeFromStream unsubscribes from a stream queue.
func (c *Client) UnsubscribeFromStream(queueName string) error {
	c.subsMu.Lock()
	sub, ok := c.queueSubs[queueName]
	if ok {
		delete(c.queueSubs, queueName)
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

// CommitOffset explicitly commits an offset for a stream consumer group.
// Use when AutoCommit is false for manual commit control.
func (c *Client) CommitOffset(queueName, groupID string, offset uint64) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()

	// Send commit via publish to a special commit topic
	return ch.Publish("", "$queue/"+queueName+"/$commit", false, false, amqp091.Publishing{
		Headers: amqp091.Table{
			"x-group-id": groupID,
			"x-offset":   int64(offset),
		},
	})
}

func (c *Client) subscribeQueue(sub *queueSubscription) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	args := amqp091.Table{}
	if sub.consumerGroup != "" {
		args["x-consumer-group"] = sub.consumerGroup
	}

	c.chMu.Lock()
	deliveries, err := ch.Consume(
		sub.queueTopic,
		sub.consumerTag,
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

	go func() {
		for {
			select {
			case <-sub.done:
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				sub.handler(&QueueMessage{
					Delivery:  d,
					queueName: sub.queueName,
					client:    c,
				})
			}
		}
	}()

	return nil
}

func (c *Client) subscribeStream(sub *queueSubscription, opts *StreamConsumeOptions) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	args := amqp091.Table{}
	if opts != nil {
		if opts.ConsumerGroup != "" {
			args["x-consumer-group"] = opts.ConsumerGroup
		}
		if opts.Offset != "" {
			args["x-stream-offset"] = opts.Offset
		}
		if opts.AutoCommit != nil && !*opts.AutoCommit {
			args["x-auto-commit"] = false
		}
		for k, v := range opts.Arguments {
			args[k] = v
		}
	}

	autoAck := false
	exclusive := false
	noLocal := false
	noWait := false
	if opts != nil {
		autoAck = opts.AutoAck
		exclusive = opts.Exclusive
		noLocal = opts.NoLocal
		noWait = opts.NoWait
	}

	c.chMu.Lock()
	deliveries, err := ch.Consume(
		sub.queueTopic,
		sub.consumerTag,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
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
				sub.handler(&QueueMessage{
					Delivery:  d,
					queueName: sub.queueName,
					client:    c,
				})
			}
		}
	}()

	return nil
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

func headerUint64(headers amqp091.Table, key string) (uint64, bool) {
	if headers == nil {
		return 0, false
	}
	val, ok := headers[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case string:
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

func headerInt64(headers amqp091.Table, key string) (int64, bool) {
	if headers == nil {
		return 0, false
	}
	val, ok := headers[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case uint64:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

func headerBool(headers amqp091.Table, key string) (bool, bool) {
	if headers == nil {
		return false, false
	}
	val, ok := headers[key]
	if !ok {
		return false, false
	}
	switch v := val.(type) {
	case bool:
		return v, true
	case string:
		if v == "true" {
			return true, true
		}
		if v == "false" {
			return false, true
		}
	}
	return false, false
}

func headerString(headers amqp091.Table, key string) (string, bool) {
	if headers == nil {
		return "", false
	}
	val, ok := headers[key]
	if !ok {
		return "", false
	}
	switch v := val.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	}
	return "", false
}
