// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// SetReturnHandler sets a callback for unroutable mandatory publishes.
func (c *Client) SetReturnHandler(handler func(amqp091.Return)) {
	c.notifyMu.Lock()
	c.onReturn = handler
	c.notifyMu.Unlock()

	if handler != nil {
		if ch, err := c.pubChannel(); err == nil {
			c.startNotificationListeners(ch)
		}
	}
}

// SetPublishConfirmHandler sets a callback for publisher confirms.
func (c *Client) SetPublishConfirmHandler(handler func(amqp091.Confirmation)) {
	c.notifyMu.Lock()
	c.onPublishConfirmation = handler
	c.notifyMu.Unlock()

	if handler != nil {
		if ch, err := c.pubChannel(); err == nil {
			c.startNotificationListeners(ch)
		}
	}
}

// EnablePublisherConfirms enables confirm mode on the current channel.
func (c *Client) EnablePublisherConfirms() error {
	c.publisherConfirms.Store(true)

	ch, err := c.pubChannel()
	if err != nil {
		return err
	}

	c.pubChMu.Lock()
	defer c.pubChMu.Unlock()
	if err := ch.Confirm(false); err != nil {
		return err
	}

	c.startNotificationListeners(ch)
	return nil
}

// PublishWithConfirm publishes and waits for a broker confirm up to timeout.
func (c *Client) PublishWithConfirm(opts *PublishOptions, timeout time.Duration) error {
	if opts == nil {
		return ErrNilOptions
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}

	if err := c.EnablePublisherConfirms(); err != nil {
		return err
	}

	ch, err := c.pubChannel()
	if err != nil {
		return err
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

	c.pubChMu.Lock()
	confirmCh := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))
	if err := ch.Publish(exchange, routingKey, opts.Mandatory, opts.Immediate, publishing); err != nil {
		c.pubChMu.Unlock()
		return err
	}
	c.pubChMu.Unlock()

	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case confirm, ok := <-confirmCh:
		if !ok || !confirm.Ack {
			return ErrPublisherConfirm
		}
		return nil
	case <-timer.C:
		return ErrTimeout
	}
}
