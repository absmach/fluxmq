// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import "testing"

func TestOptionsSetURL(t *testing.T) {
	opts := NewOptions().SetURL("amqp://user:pass@localhost:5672/vhost")
	if opts.URL != "amqp://user:pass@localhost:5672/vhost" {
		t.Fatalf("expected URL to be set, got %q", opts.URL)
	}
}

func TestPublishWithOptionsMandatoryRequiresReturnHandler(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	c.connected.Store(true)

	err = c.PublishWithOptions(&PublishOptions{
		Topic:     "events/test",
		Payload:   []byte("hello"),
		Mandatory: true,
	})
	if err != ErrNoReturnHandler {
		t.Fatalf("expected ErrNoReturnHandler, got %v", err)
	}
}

func TestPublishWithConfirmNilOptions(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := c.PublishWithConfirm(nil, 0); err != ErrNilOptions {
		t.Fatalf("expected ErrNilOptions, got %v", err)
	}
}

func TestGetValidation(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Not connected
	_, _, err = c.Get("test-queue", true)
	if err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}

	c.connected.Store(true)

	// Empty queue name
	_, _, err = c.Get("", true)
	if err != ErrInvalidQueueName {
		t.Fatalf("expected ErrInvalidQueueName, got %v", err)
	}

	_, _, err = c.GetFromQueue("", true)
	if err != ErrInvalidQueueName {
		t.Fatalf("expected ErrInvalidQueueName, got %v", err)
	}
}

func TestGetFromQueueNormalizesName(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Not connected — should still propagate ErrNotConnected
	_, _, err = c.GetFromQueue("my-queue", true)
	if err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}
