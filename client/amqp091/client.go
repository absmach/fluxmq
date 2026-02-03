// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"net"
	"sync"
	"sync/atomic"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Client is a minimal AMQP 0.9.1 client with queue support.
type Client struct {
	opts *Options

	conn *amqp091.Connection
	ch   *amqp091.Channel

	chMu sync.Mutex

	subsMu sync.Mutex
	subs   map[string]*subscription

	connected atomic.Bool
}

// New creates a new AMQP 0.9.1 client with the given options.
func New(opts *Options) (*Client, error) {
	if opts == nil {
		opts = NewOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &Client{
		opts: opts,
		subs: make(map[string]*subscription),
	}, nil
}

// Connect establishes a connection to the broker.
func (c *Client) Connect() error {
	if c.connected.Load() {
		return ErrAlreadyConnected
	}

	url, err := c.opts.dialURL()
	if err != nil {
		return err
	}

	dialer := &net.Dialer{Timeout: c.opts.DialTimeout}
	cfg := amqp091.Config{
		TLSClientConfig: c.opts.TLSConfig,
		Heartbeat:       c.opts.Heartbeat,
		Dial:            dialer.Dial,
	}

	conn, err := amqp091.DialConfig(url, cfg)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}

	if c.opts.PrefetchCount > 0 || c.opts.PrefetchSize > 0 {
		if err := ch.Qos(c.opts.PrefetchCount, c.opts.PrefetchSize, false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return err
		}
	}

	c.conn = conn
	c.ch = ch
	c.connected.Store(true)
	return nil
}

// Close closes the client and all active subscriptions.
func (c *Client) Close() error {
	if !c.connected.Load() {
		return nil
	}

	c.subsMu.Lock()
	for _, sub := range c.subs {
		sub.close()
	}
	c.subs = make(map[string]*subscription)
	c.subsMu.Unlock()

	c.chMu.Lock()
	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.chMu.Unlock()

	c.connected.Store(false)
	return nil
}

// IsConnected reports whether the client is connected.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

func (c *Client) channel() (*amqp091.Channel, error) {
	if !c.connected.Load() || c.ch == nil {
		return nil, ErrNotConnected
	}
	return c.ch, nil
}

func (c *Client) publish(exchange, routingKey string, msg amqp091.Publishing, mandatory, immediate bool) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	c.chMu.Lock()
	defer c.chMu.Unlock()

	return ch.Publish(exchange, routingKey, mandatory, immediate, msg)
}
