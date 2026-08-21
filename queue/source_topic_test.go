// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"

	"github.com/absmach/fluxmq/queue/types"
)

const (
	testSourceTopic   = "acme/temp"
	testQueuedAddress = "$queue/m/acme/temp"
)

// The delivery address is not injective: several distinct origins map onto the
// same $queue/... path. These are the collisions, pinned so the contract cannot
// drift silently, and so the fix below is measured against something real.
func TestQueueDeliveryTopicCollides(t *testing.T) {
	tests := []struct {
		name   string
		queue  string
		stored string
		want   string
	}{
		{name: "capture keeping the queue-named level", queue: "m", stored: "m/acme/temp", want: testQueuedAddress},
		{name: "explicit queue publish", queue: "m", stored: testQueuedAddress, want: testQueuedAddress},
		{name: "capture below the queue root", queue: "m", stored: testSourceTopic, want: testQueuedAddress},
		{name: "capture of the queue name alone", queue: "m", stored: "m", want: "$queue/m"},
		{name: "capture of a single level", queue: "q", stored: "y", want: "$queue/q/y"},
		{name: "capture prefixed by the queue name", queue: "q", stored: "q/y", want: "$queue/q/y"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := queueDeliveryTopic(test.queue, test.stored); got != test.want {
				t.Fatalf("queueDeliveryTopic(%q, %q) = %q, want %q", test.queue, test.stored, got, test.want)
			}
		})
	}
}

// Whatever the address collapses, the source topic must survive intact, because
// it is the only thing a consumer can use to tell those origins apart.
func TestDeliveryCarriesTheSourceTopic(t *testing.T) {
	for _, stored := range []string{"m/acme/temp", testQueuedAddress, testSourceTopic, "m"} {
		t.Run(stored, func(t *testing.T) {
			msg := &types.Message{Topic: stored, Sequence: 7}
			delivery := createDeliveryMessage(msg, "group", "m")

			if got := delivery.Properties[types.PropSourceTopic]; got != stored {
				t.Fatalf("%s = %q, want the unmodified source topic %q", types.PropSourceTopic, got, stored)
			}
			// The address stays as it is: this change adds information rather
			// than altering the wire format consumers already subscribe to.
			if got := delivery.Topic; got != queueDeliveryTopic("m", stored) {
				t.Fatalf("delivery address changed to %q", got)
			}
		})
	}
}

// The property is broker-owned. A publisher that sets it on its own message
// must not be able to misrepresent where that message came from.
func TestPublisherCannotForgeTheSourceTopic(t *testing.T) {
	msg := &types.Message{
		Topic:      testSourceTopic,
		Sequence:   1,
		Properties: map[string]string{types.PropSourceTopic: "somewhere/else", "user": "kept"},
	}

	delivery := createDeliveryMessage(msg, "group", "m")

	if got := delivery.Properties[types.PropSourceTopic]; got != testSourceTopic {
		t.Fatalf("%s = %q, want the broker's value to win", types.PropSourceTopic, got)
	}
	if got := delivery.Properties["user"]; got != "kept" {
		t.Fatalf("an ordinary publisher property was dropped: %q", got)
	}
}

// A reserved property is queue-owned metadata, so it must be recognized as such
// wherever reserved keys are filtered out of user properties.
func TestSourceTopicIsAReservedProperty(t *testing.T) {
	if !types.IsReservedQueueDeliveryProperty(types.PropSourceTopic) {
		t.Fatalf("%s must be reserved so it is not mistaken for a user property", types.PropSourceTopic)
	}
}

// The remote path builds its own envelope, so the source topic has to be
// carried explicitly rather than re-derived from the delivery address, which by
// then has already been converted and cannot be reversed.
func TestRoutedQueueMessageCarriesTheSourceTopic(t *testing.T) {
	msg := &types.Message{Topic: testSourceTopic, Sequence: 3}

	routed := createRoutedQueueMessage(msg, "group", "m", false, 0, false, "")

	if routed.SourceTopic != testSourceTopic {
		t.Fatalf("SourceTopic = %q, want the unmodified source topic", routed.SourceTopic)
	}
	if routed.Topic != testQueuedAddress {
		t.Fatalf("Topic = %q, want the canonical queue address", routed.Topic)
	}
}
