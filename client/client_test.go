// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"testing"
	"time"

	amqpclient "github.com/absmach/fluxmq/client/amqp"
	mqttclient "github.com/absmach/fluxmq/client/mqtt"
)

func TestNewNoTransport(t *testing.T) {
	_, err := New(nil)
	if !errors.Is(err, ErrNoTransport) {
		t.Fatalf("New(nil) error = %v, want %v", err, ErrNoTransport)
	}

	_, err = New(NewConfig())
	if !errors.Is(err, ErrNoTransport) {
		t.Fatalf("New(NewConfig()) error = %v, want %v", err, ErrNoTransport)
	}
}

func TestNewWithBothTransports(t *testing.T) {
	cfg := NewConfig().
		SetMQTT(testMQTTOptions()).
		SetAMQP(testAMQPOptions()).
		SetDefaultProtocol(ProtocolAMQP)

	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if c.MQTT() == nil {
		t.Fatal("MQTT transport should be configured")
	}
	if c.AMQP() == nil {
		t.Fatal("AMQP transport should be configured")
	}
}

func TestPublishNoTransport(t *testing.T) {
	c := &Client{}
	err := c.Publish(context.Background(), "topic/a", []byte("x"))
	if !errors.Is(err, ErrNoTransport) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrNoTransport)
	}
}

func TestPublishNoRouteProtocol(t *testing.T) {
	c, err := NewMQTT(testMQTTOptions())
	if err != nil {
		t.Fatalf("NewMQTT() error = %v", err)
	}

	err = c.Publish(context.Background(), "topic/a", []byte("x"), WithProtocol(ProtocolAMQP))
	if !errors.Is(err, ErrNoRouteProtocol) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrNoRouteProtocol)
	}
}

func TestSubscribeNilHandler(t *testing.T) {
	c, err := NewMQTT(testMQTTOptions())
	if err != nil {
		t.Fatalf("NewMQTT() error = %v", err)
	}

	err = c.Subscribe(context.Background(), "topic/a", nil)
	if !errors.Is(err, ErrSubscribeFailed) {
		t.Fatalf("Subscribe() error = %v, want %v", err, ErrSubscribeFailed)
	}

	err = c.SubscribeToQueue(context.Background(), "queue/a", "group-a", nil)
	if !errors.Is(err, ErrQueueSubscribeFailed) {
		t.Fatalf("SubscribeToQueue() error = %v, want %v", err, ErrQueueSubscribeFailed)
	}
}

func TestUnsubscribeProtocolRouting(t *testing.T) {
	c, err := New(NewConfig().SetMQTT(testMQTTOptions()).SetAMQP(testAMQPOptions()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// AMQP unsubscribe returns nil when no subscription exists.
	if err := c.Unsubscribe(context.Background(), "topic/a", WithProtocol(ProtocolAMQP)); err != nil {
		t.Fatalf("Unsubscribe(AMQP) unexpected error: %v", err)
	}

	// MQTT unsubscribe attempts network state checks and fails while disconnected.
	err = c.Unsubscribe(context.Background(), "topic/a", WithProtocol(ProtocolMQTT))
	if !errors.Is(err, ErrUnsubFailed) {
		t.Fatalf("Unsubscribe(MQTT) error = %v, want %v", err, ErrUnsubFailed)
	}
}

func TestUnsubscribeFromQueueProtocolRouting(t *testing.T) {
	c, err := New(NewConfig().SetMQTT(testMQTTOptions()).SetAMQP(testAMQPOptions()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// AMQP queue unsubscribe returns nil when no subscription exists.
	if err := c.UnsubscribeFromQueue(context.Background(), "queue/a", WithProtocol(ProtocolAMQP)); err != nil {
		t.Fatalf("UnsubscribeFromQueue(AMQP) unexpected error: %v", err)
	}

	// MQTT queue unsubscribe attempts network state checks and fails while disconnected.
	err = c.UnsubscribeFromQueue(context.Background(), "queue/a", WithProtocol(ProtocolMQTT))
	if !errors.Is(err, ErrQueueUnsubFailed) {
		t.Fatalf("UnsubscribeFromQueue(MQTT) error = %v, want %v", err, ErrQueueUnsubFailed)
	}
}

func TestConnectWrapsErrors(t *testing.T) {
	t.Run("mqtt", func(t *testing.T) {
		c, err := NewMQTT(testMQTTOptions())
		if err != nil {
			t.Fatalf("NewMQTT() error = %v", err)
		}

		err = c.Connect(context.Background())
		if !errors.Is(err, ErrConnectFailed) {
			t.Fatalf("Connect() error = %v, want %v", err, ErrConnectFailed)
		}
	})

	t.Run("amqp", func(t *testing.T) {
		c, err := NewAMQP(testAMQPOptions())
		if err != nil {
			t.Fatalf("NewAMQP() error = %v", err)
		}

		err = c.Connect(context.Background())
		if !errors.Is(err, ErrConnectFailed) {
			t.Fatalf("Connect() error = %v, want %v", err, ErrConnectFailed)
		}
	})
}

func TestDispatchMQTTDoesNotHoldLockWhileCallingHandlers(t *testing.T) {
	c := &Client{
		subsExact: make(map[string]MessageHandler),
		subsWild:  make(map[string]MessageHandler),
		qsubs:     make(map[string]MessageHandler),
	}

	done := make(chan struct{})
	c.subsWild["events/#"] = func(_ *Message) {
		c.mu.Lock()
		c.mu.Unlock()
		close(done)
	}

	go c.dispatchMQTT(&mqttclient.Message{Topic: "events/test", Payload: []byte("payload")})

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("dispatchMQTT appears to hold c.mu while invoking handlers")
	}
}

func TestDispatchMQTTMatchesExactAndWildcard(t *testing.T) {
	c := &Client{
		subsExact: make(map[string]MessageHandler),
		subsWild:  make(map[string]MessageHandler),
		qsubs:     make(map[string]MessageHandler),
	}

	got := make(chan string, 4)
	c.subsExact["events/a"] = func(_ *Message) { got <- "exact" }
	c.subsWild["events/+"] = func(_ *Message) { got <- "wild-plus" }
	c.subsWild["events/#"] = func(_ *Message) { got <- "wild-hash" }

	c.dispatchMQTT(&mqttclient.Message{Topic: "events/a", Payload: []byte("payload")})

	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case name := <-got:
			seen[name] = true
		case <-time.After(250 * time.Millisecond):
			t.Fatal("timed out waiting for dispatch handlers")
		}
	}

	if !seen["exact"] || !seen["wild-plus"] || !seen["wild-hash"] {
		t.Fatalf("expected exact and wildcard handlers, got %#v", seen)
	}
}

func testMQTTOptions() *mqttclient.Options {
	return mqttclient.NewOptions().
		SetServers("127.0.0.1:1").
		SetClientID("test-client").
		SetConnectTimeout(20 * time.Millisecond)
}

func testAMQPOptions() *amqpclient.Options {
	return amqpclient.NewOptions().
		SetAddress("127.0.0.1:1").
		SetDialTimeout(20 * time.Millisecond)
}
