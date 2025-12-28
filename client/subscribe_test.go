// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"
)

func TestSubscribeOption_Basic(t *testing.T) {
	opt := NewSubscribeOption("sensors/temperature", 1)

	if opt.Topic != "sensors/temperature" {
		t.Errorf("Expected topic 'sensors/temperature', got '%s'", opt.Topic)
	}
	if opt.QoS != 1 {
		t.Errorf("Expected QoS 1, got %d", opt.QoS)
	}
	if opt.NoLocal {
		t.Error("Expected NoLocal false by default")
	}
	if opt.RetainAsPublished {
		t.Error("Expected RetainAsPublished false by default")
	}
	if opt.RetainHandling != 0 {
		t.Errorf("Expected RetainHandling 0 by default, got %d", opt.RetainHandling)
	}
}

func TestSubscribeOption_NoLocal(t *testing.T) {
	opt := NewSubscribeOption("test/topic", 2).SetNoLocal(true)

	if !opt.NoLocal {
		t.Error("Expected NoLocal true")
	}
}

func TestSubscribeOption_RetainAsPublished(t *testing.T) {
	opt := NewSubscribeOption("test/topic", 2).SetRetainAsPublished(true)

	if !opt.RetainAsPublished {
		t.Error("Expected RetainAsPublished true")
	}
}

func TestSubscribeOption_RetainHandling(t *testing.T) {
	testCases := []struct {
		name     string
		handling byte
	}{
		{"send at subscribe", 0},
		{"send if new subscription", 1},
		{"don't send", 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opt := NewSubscribeOption("test/topic", 2).SetRetainHandling(tc.handling)

			if opt.RetainHandling != tc.handling {
				t.Errorf("Expected RetainHandling %d, got %d", tc.handling, opt.RetainHandling)
			}
		})
	}
}

func TestSubscribeOption_Chaining(t *testing.T) {
	opt := NewSubscribeOption("sensors/#", 1).
		SetNoLocal(true).
		SetRetainAsPublished(true).
		SetRetainHandling(1)

	if opt.Topic != "sensors/#" {
		t.Errorf("Expected topic 'sensors/#', got '%s'", opt.Topic)
	}
	if opt.QoS != 1 {
		t.Errorf("Expected QoS 1, got %d", opt.QoS)
	}
	if !opt.NoLocal {
		t.Error("Expected NoLocal true")
	}
	if !opt.RetainAsPublished {
		t.Error("Expected RetainAsPublished true")
	}
	if opt.RetainHandling != 1 {
		t.Errorf("Expected RetainHandling 1, got %d", opt.RetainHandling)
	}
}

func TestSubscribeOption_MultipleTopics(t *testing.T) {
	opts := []*SubscribeOption{
		NewSubscribeOption("sensors/temperature", 1).SetNoLocal(true),
		NewSubscribeOption("sensors/humidity", 2).SetRetainAsPublished(true),
		NewSubscribeOption("alerts/#", 0).SetRetainHandling(2),
	}

	if len(opts) != 3 {
		t.Fatalf("Expected 3 options, got %d", len(opts))
	}

	// First option
	if opts[0].Topic != "sensors/temperature" || opts[0].QoS != 1 || !opts[0].NoLocal {
		t.Error("First option configured incorrectly")
	}

	// Second option
	if opts[1].Topic != "sensors/humidity" || opts[1].QoS != 2 || !opts[1].RetainAsPublished {
		t.Error("Second option configured incorrectly")
	}

	// Third option
	if opts[2].Topic != "alerts/#" || opts[2].QoS != 0 || opts[2].RetainHandling != 2 {
		t.Error("Third option configured incorrectly")
	}
}

func TestSubscribeOption_AllOptions(t *testing.T) {
	opt := NewSubscribeOption("devices/+/status", 2).
		SetNoLocal(true).
		SetRetainAsPublished(true).
		SetRetainHandling(1)

	if opt.Topic != "devices/+/status" {
		t.Errorf("Expected topic 'devices/+/status', got '%s'", opt.Topic)
	}
	if opt.QoS != 2 {
		t.Errorf("Expected QoS 2, got %d", opt.QoS)
	}
	if !opt.NoLocal {
		t.Error("Expected NoLocal true")
	}
	if !opt.RetainAsPublished {
		t.Error("Expected RetainAsPublished true")
	}
	if opt.RetainHandling != 1 {
		t.Errorf("Expected RetainHandling 1, got %d", opt.RetainHandling)
	}
}
