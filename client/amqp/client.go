// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Client is a minimal AMQP 0.9.1 client with queue and pub/sub support.
type Client struct {
	opts *Options

	conn   *amqp091.Connection
	ch     *amqp091.Channel
	connMu sync.RWMutex

	chMu sync.Mutex

	subsMu    sync.Mutex
	queueSubs map[string]*queueSubscription
	topicSubs map[string]*topicSubscription

	connected    atomic.Bool
	closing      atomic.Bool
	reconnecting atomic.Bool
	stopCh       chan struct{}
	closeOnce    sync.Once
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
		opts:      opts,
		queueSubs: make(map[string]*queueSubscription),
		topicSubs: make(map[string]*topicSubscription),
		stopCh:    make(chan struct{}),
	}, nil
}

// Connect establishes a connection to the broker.
func (c *Client) Connect() error {
	if c.connected.Load() {
		return ErrAlreadyConnected
	}

	if err := c.connectOnce(); err != nil {
		return err
	}

	if err := c.resubscribeAll(); err != nil {
		c.cleanupConn()
		return err
	}

	return nil
}

// Close closes the client and all active subscriptions.
func (c *Client) Close() error {
	if c.closing.Swap(true) {
		return nil
	}

	c.closeOnce.Do(func() {
		close(c.stopCh)
	})

	c.subsMu.Lock()
	for _, sub := range c.queueSubs {
		sub.close()
	}
	for _, sub := range c.topicSubs {
		sub.close()
	}
	c.queueSubs = make(map[string]*queueSubscription)
	c.topicSubs = make(map[string]*topicSubscription)
	c.subsMu.Unlock()

	c.cleanupConn()
	c.connected.Store(false)
	return nil
}

// IsConnected reports whether the client is connected.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

func (c *Client) channel() (*amqp091.Channel, error) {
	if !c.connected.Load() {
		return nil, ErrNotConnected
	}
	c.connMu.RLock()
	ch := c.ch
	c.connMu.RUnlock()
	if ch == nil {
		return nil, ErrNotConnected
	}
	return ch, nil
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

func (c *Client) connectOnce() error {
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

	c.connMu.Lock()
	c.conn = conn
	c.ch = ch
	c.connMu.Unlock()

	c.connected.Store(true)
	c.watchClose(conn, ch)

	if c.opts.OnConnect != nil {
		go c.opts.OnConnect()
	}

	return nil
}

func (c *Client) watchClose(conn *amqp091.Connection, ch *amqp091.Channel) {
	connClose := conn.NotifyClose(make(chan *amqp091.Error, 1))
	chClose := ch.NotifyClose(make(chan *amqp091.Error, 1))

	go func() {
		select {
		case err := <-connClose:
			c.handleDisconnect(err)
		case err := <-chClose:
			c.handleDisconnect(err)
		case <-c.stopCh:
			return
		}
	}()
}

func (c *Client) handleDisconnect(err error) {
	if c.closing.Load() {
		return
	}

	if !c.connected.Swap(false) {
		return
	}
	c.cleanupConn()

	if c.opts.OnConnectionLost != nil {
		go c.opts.OnConnectionLost(err)
	}

	if c.opts.AutoReconnect {
		c.startReconnect()
	}
}

func (c *Client) startReconnect() {
	if !c.reconnecting.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer c.reconnecting.Store(false)

		attempt := 1
		delay := c.opts.ReconnectBackoff
		if delay <= 0 {
			delay = DefaultReconnectBackoff
		}

		maxDelay := c.opts.MaxReconnectWait
		if maxDelay <= 0 {
			maxDelay = DefaultMaxReconnectWait
		}

		for {
			select {
			case <-c.stopCh:
				return
			default:
			}

			if c.opts.OnReconnecting != nil {
				c.opts.OnReconnecting(attempt)
			}

			if err := c.connectOnce(); err == nil {
				if err := c.resubscribeAll(); err == nil {
					return
				}
				c.cleanupConn()
			}

			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-c.stopCh:
				timer.Stop()
				return
			}

			attempt++
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}()
}

func (c *Client) resubscribeAll() error {
	c.subsMu.Lock()
	queueSubs := make([]*queueSubscription, 0, len(c.queueSubs))
	for _, sub := range c.queueSubs {
		queueSubs = append(queueSubs, sub)
	}
	topicSubs := make([]*topicSubscription, 0, len(c.topicSubs))
	for _, sub := range c.topicSubs {
		topicSubs = append(topicSubs, sub)
	}
	c.subsMu.Unlock()

	for _, sub := range queueSubs {
		if err := c.subscribeQueue(sub); err != nil {
			return err
		}
	}

	for _, sub := range topicSubs {
		if err := c.subscribeTopic(sub); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) cleanupConn() {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}
