// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import amqp091 "github.com/rabbitmq/amqp091-go"

// ExchangeDeclareOptions configures exchange declaration.
type ExchangeDeclareOptions struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  amqp091.Table
}

// QueueDeclareOptions configures queue declaration.
type QueueDeclareOptions struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  amqp091.Table
}

// DeclareExchange declares an exchange.
func (c *Client) DeclareExchange(opts *ExchangeDeclareOptions) error {
	if opts == nil || opts.Name == "" {
		return ErrInvalidTopic
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.ExchangeDeclare(
		opts.Name,
		opts.Kind,
		opts.Durable,
		opts.AutoDelete,
		opts.Internal,
		opts.NoWait,
		opts.Arguments,
	)
}

// DeleteExchange deletes an exchange.
func (c *Client) DeleteExchange(name string, ifUnused, noWait bool) error {
	if name == "" {
		return ErrInvalidTopic
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.ExchangeDelete(name, ifUnused, noWait)
}

// BindExchange binds a destination exchange to a source exchange.
func (c *Client) BindExchange(destination, key, source string, noWait bool, args amqp091.Table) error {
	if destination == "" || source == "" {
		return ErrInvalidTopic
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.ExchangeBind(destination, key, source, noWait, args)
}

// UnbindExchange removes an exchange binding.
func (c *Client) UnbindExchange(destination, key, source string, noWait bool, args amqp091.Table) error {
	if destination == "" || source == "" {
		return ErrInvalidTopic
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.ExchangeUnbind(destination, key, source, noWait, args)
}

// DeclareQueue declares a queue.
func (c *Client) DeclareQueue(opts *QueueDeclareOptions) (string, error) {
	if opts == nil {
		return "", ErrNilOptions
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
		opts.Arguments,
	)
	c.chMu.Unlock()
	if err != nil {
		return "", err
	}

	return q.Name, nil
}

// DeleteQueue deletes a queue and returns purged message count.
func (c *Client) DeleteQueue(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	if name == "" {
		return 0, ErrInvalidQueueName
	}

	ch, err := c.channel()
	if err != nil {
		return 0, err
	}

	c.chMu.Lock()
	count, err := ch.QueueDelete(name, ifUnused, ifEmpty, noWait)
	c.chMu.Unlock()
	return count, err
}

// PurgeQueue purges all ready messages in a queue.
func (c *Client) PurgeQueue(name string, noWait bool) (int, error) {
	if name == "" {
		return 0, ErrInvalidQueueName
	}

	ch, err := c.channel()
	if err != nil {
		return 0, err
	}

	c.chMu.Lock()
	count, err := ch.QueuePurge(name, noWait)
	c.chMu.Unlock()
	return count, err
}

// BindQueue binds a queue to an exchange using a routing key.
func (c *Client) BindQueue(queue, key, exchange string, noWait bool, args amqp091.Table) error {
	if queue == "" {
		return ErrInvalidQueueName
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.QueueBind(queue, key, exchange, noWait, args)
}

// UnbindQueue removes a queue binding.
func (c *Client) UnbindQueue(queue, key, exchange string, args amqp091.Table) error {
	if queue == "" {
		return ErrInvalidQueueName
	}

	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.QueueUnbind(queue, key, exchange, args)
}

// SetQoS updates channel QoS (prefetch count/size).
func (c *Client) SetQoS(count, size int, global bool) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()
	return ch.Qos(count, size, global)
}
