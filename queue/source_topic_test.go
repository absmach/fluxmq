// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
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
			msg := message.New(stored, nil)
			msg.Broker.Queue.Offset = 7
			delivery := createDeliveryMessage(msg, "group", "m")
			defer message.Release(delivery)

			if got := delivery.Broker.Source.Topic; got != stored {
				t.Fatalf("source topic = %q, want the unmodified source topic %q", got, stored)
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
	msg := message.New(testSourceTopic, nil)
	msg.Broker.Queue.Offset = 1
	msg.User.Properties = message.FilterUserProperties(map[string]string{
		message.PropertySourceTopic: "somewhere/else",
		"user":                      "kept",
	})

	delivery := createDeliveryMessage(msg, "group", "m")
	defer message.Release(delivery)

	if got := delivery.Broker.Source.Topic; got != testSourceTopic {
		t.Fatalf("source topic = %q, want the broker's value to win", got)
	}
	if got := delivery.User.Properties["user"]; got != "kept" {
		t.Fatalf("an ordinary publisher property was dropped: %q", got)
	}
}

// A reserved property is queue-owned metadata, so it must be recognized as such
// wherever reserved keys are filtered out of user properties.
func TestSourceTopicIsAReservedProperty(t *testing.T) {
	if !message.IsReservedProperty(message.PropertySourceTopic) {
		t.Fatalf("%s must be reserved so it is not mistaken for a user property", message.PropertySourceTopic)
	}
}

// The remote path builds its own envelope, so the source topic has to be
// carried explicitly rather than re-derived from the delivery address, which by
// then has already been converted and cannot be reversed.
func TestRoutedQueueMessageCarriesTheSourceTopic(t *testing.T) {
	msg := message.New(testSourceTopic, nil)
	msg.Broker.Queue.Offset = 3

	routed := createRoutedQueueMessage(msg, "group", "m", false, 0, false, "")
	defer message.Release(routed)

	if routed.Broker.Source.Topic != testSourceTopic {
		t.Fatalf("source topic = %q, want the unmodified source topic", routed.Broker.Source.Topic)
	}
	if routed.Topic != testQueuedAddress {
		t.Fatalf("Topic = %q, want the canonical queue address", routed.Topic)
	}
}

func TestQueueRoundTripPreservesProtocolMetadataAndExpiry(t *testing.T) {
	payloadFormat := byte(1)
	messageExpiry := uint32(90)
	publishedAt := time.Now().Add(-time.Second)
	expiresAt := publishedAt.Add(time.Duration(messageExpiry) * time.Second)
	correlationData := []byte{0x00, 0x01, 0xfe, 0xff}

	config := types.DefaultQueueConfig("m", "m/#")
	config.MessageTTL = 5 * time.Minute
	queued := newQueuedMessage(types.PublishRequest{
		Topic:           testSourceTopic,
		Payload:         []byte("payload"),
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		ResponseTopic:   "responses/42",
		CorrelationData: correlationData,
		PayloadFormat:   &payloadFormat,
		MessageExpiry:   &messageExpiry,
		PublishedAt:     publishedAt,
		ExpiresAt:       expiresAt,
	}, &config)
	defer message.Release(queued)

	correlationData[0] = 0xff
	payloadFormat = 0
	messageExpiry = 1

	if queued.User.ContentType != "application/json" || queued.User.ContentEncoding != "gzip" {
		t.Fatalf("content metadata was not preserved: %+v", queued.User)
	}
	if queued.User.ResponseTopic != "responses/42" {
		t.Fatalf("response topic = %q, want responses/42", queued.User.ResponseTopic)
	}
	if !bytes.Equal(queued.User.CorrelationData, []byte{0x00, 0x01, 0xfe, 0xff}) {
		t.Fatalf("correlation data was aliased: %v", queued.User.CorrelationData)
	}
	if queued.User.PayloadFormat == nil || *queued.User.PayloadFormat != 1 {
		t.Fatalf("payload format was not copied: %v", queued.User.PayloadFormat)
	}
	if queued.User.MessageExpiry == nil || *queued.User.MessageExpiry != 90 {
		t.Fatalf("message expiry was not copied: %v", queued.User.MessageExpiry)
	}
	if !queued.Broker.Delivery.PublishedAt.Equal(publishedAt) || !queued.Broker.Delivery.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("delivery timing was not preserved: %+v", queued.Broker.Delivery)
	}
	if !queued.Broker.Queue.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("queue expiry = %v, want earlier protocol expiry %v", queued.Broker.Queue.ExpiresAt, expiresAt)
	}

	delivery := createDeliveryMessage(queued, "group", "m")
	defer message.Release(delivery)
	if !delivery.Broker.Delivery.PublishedAt.Equal(publishedAt) || !delivery.Broker.Delivery.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("queue delivery discarded protocol expiry: %+v", delivery.Broker.Delivery)
	}
}
