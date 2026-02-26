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
// It uses separate channels for publishing and consuming so that a
// channel-level exception from a publish does not kill active consumers.
type Client struct {
	opts *Options

	conn   *amqp091.Connection
	connMu sync.RWMutex

	// Publishing channel — isolated from consumer channel
	pubCh   *amqp091.Channel
	pubChMu sync.Mutex

	// Consuming / admin channel
	subCh   *amqp091.Channel
	subChMu sync.Mutex

	notifyMu              sync.RWMutex
	onReturn              func(amqp091.Return)
	onPublishConfirmation func(amqp091.Confirmation)
	publisherConfirms     atomic.Bool
	returnListenerStarted  atomic.Bool
	confirmListenerStarted atomic.Bool

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

// pubChannel returns the publishing channel.
func (c *Client) pubChannel() (*amqp091.Channel, error) {
	if !c.connected.Load() {
		return nil, ErrNotConnected
	}
	c.connMu.RLock()
	ch := c.pubCh
	c.connMu.RUnlock()
	if ch == nil {
		return nil, ErrNotConnected
	}
	return ch, nil
}

// subChannel returns the consuming/admin channel.
func (c *Client) subChannel() (*amqp091.Channel, error) {
	if !c.connected.Load() {
		return nil, ErrNotConnected
	}
	c.connMu.RLock()
	ch := c.subCh
	c.connMu.RUnlock()
	if ch == nil {
		return nil, ErrNotConnected
	}
	return ch, nil
}

// channel returns the consuming/admin channel (backward-compatible alias).
func (c *Client) channel() (*amqp091.Channel, error) {
	return c.subChannel()
}

func (c *Client) publish(exchange, routingKey string, msg amqp091.Publishing, mandatory, immediate bool) error {
	ch, err := c.pubChannel()
	if err != nil {
		return err
	}

	c.pubChMu.Lock()
	defer c.pubChMu.Unlock()

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

	// Create publishing channel
	pubCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}

	// Create consuming/admin channel
	subCh, err := conn.Channel()
	if err != nil {
		_ = pubCh.Close()
		_ = conn.Close()
		return err
	}

	if c.opts.PrefetchCount > 0 || c.opts.PrefetchSize > 0 {
		if err := subCh.Qos(c.opts.PrefetchCount, c.opts.PrefetchSize, false); err != nil {
			_ = pubCh.Close()
			_ = subCh.Close()
			_ = conn.Close()
			return err
		}
	}

	if c.publisherConfirms.Load() {
		if err := pubCh.Confirm(false); err != nil {
			_ = pubCh.Close()
			_ = subCh.Close()
			_ = conn.Close()
			return err
		}
	}

	c.connMu.Lock()
	c.conn = conn
	c.pubCh = pubCh
	c.subCh = subCh
	c.connMu.Unlock()

	c.connected.Store(true)
	c.startNotificationListeners(pubCh)
	c.watchClose(conn, pubCh, subCh)

	if c.opts.OnConnect != nil {
		go c.opts.OnConnect()
	}

	return nil
}

func (c *Client) startNotificationListeners(pubCh *amqp091.Channel) {
	c.notifyMu.RLock()
	returnHandler := c.onReturn
	confirmHandler := c.onPublishConfirmation
	c.notifyMu.RUnlock()

	if returnHandler != nil && c.returnListenerStarted.CompareAndSwap(false, true) {
		returns := pubCh.NotifyReturn(make(chan amqp091.Return, 32))
		go func() {
			for {
				select {
				case ret, ok := <-returns:
					if !ok {
						return
					}
					returnHandler(ret)
				case <-c.stopCh:
					return
				}
			}
		}()
	}

	if confirmHandler != nil && c.publisherConfirms.Load() && c.confirmListenerStarted.CompareAndSwap(false, true) {
		confirms := pubCh.NotifyPublish(make(chan amqp091.Confirmation, 64))
		go func() {
			for {
				select {
				case confirm, ok := <-confirms:
					if !ok {
						return
					}
					confirmHandler(confirm)
				case <-c.stopCh:
					return
				}
			}
		}()
	}
}

func (c *Client) watchClose(conn *amqp091.Connection, pubCh, subCh *amqp091.Channel) {
	connClose := conn.NotifyClose(make(chan *amqp091.Error, 1))
	pubChClose := pubCh.NotifyClose(make(chan *amqp091.Error, 1))
	subChClose := subCh.NotifyClose(make(chan *amqp091.Error, 1))

	go func() {
		select {
		case err := <-connClose:
			c.handleDisconnect(err)
		case err := <-pubChClose:
			c.handleDisconnect(err)
		case err := <-subChClose:
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
	c.returnListenerStarted.Store(false)
	c.confirmListenerStarted.Store(false)

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.pubCh != nil {
		_ = c.pubCh.Close()
		c.pubCh = nil
	}
	if c.subCh != nil {
		_ = c.subCh.Close()
		c.subCh = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}
