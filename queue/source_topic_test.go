// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
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
			msg.BrokerMeta.Queue.Offset = 7
			delivery := createDeliveryMessage(msg, "group", "m")
			defer message.Release(delivery)

			if got := delivery.BrokerMeta.Source.Topic; got != stored {
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
	msg.BrokerMeta.Queue.Offset = 1
	msg.PublisherMeta.Properties = message.FilterUserProperties(map[string]string{
		message.PropertySourceTopic: "somewhere/else",
		"user":                      "kept",
	})

	delivery := createDeliveryMessage(msg, "group", "m")
	defer message.Release(delivery)

	if got := delivery.BrokerMeta.Source.Topic; got != testSourceTopic {
		t.Fatalf("source topic = %q, want the broker's value to win", got)
	}
	if got, _ := delivery.PublisherMeta.Properties.Get("user"); got != "kept" {
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
	msg.BrokerMeta.Queue.Offset = 3

	routed := createRoutedQueueMessage(msg, "group", "m", false, 0, false, "")
	defer message.Release(routed)

	if routed.BrokerMeta.Source.Topic != testSourceTopic {
		t.Fatalf("source topic = %q, want the unmodified source topic", routed.BrokerMeta.Source.Topic)
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
	published := publishEnvelope(t, testSourceTopic, []byte("payload"))
	published.PublisherMeta.ContentType = "application/json"
	published.PublisherMeta.ContentEncoding = "gzip"
	published.PublisherMeta.ResponseTopic = "responses/42"
	published.PublisherMeta.CorrelationData = message.NewBinary(correlationData)
	published.PublisherMeta.PayloadFormat = message.Some(payloadFormat)
	published.PublisherMeta.MessageExpiry = message.Some(messageExpiry)
	published.BrokerMeta.Delivery.PublishedAt = publishedAt
	published.BrokerMeta.Delivery.ExpiresAt = expiresAt

	queued := newQueuedRecord(published, "m", &config)
	defer message.Release(queued)

	correlationData[0] = 0xff

	if queued.PublisherMeta.ContentType != "application/json" || queued.PublisherMeta.ContentEncoding != "gzip" {
		t.Fatalf("content metadata was not preserved: %+v", queued.PublisherMeta)
	}
	if queued.PublisherMeta.ResponseTopic != "responses/42" {
		t.Fatalf("response topic = %q, want responses/42", queued.PublisherMeta.ResponseTopic)
	}
	if !queued.PublisherMeta.CorrelationData.Equal([]byte{0x00, 0x01, 0xfe, 0xff}) {
		t.Fatalf("correlation data was aliased: %v", queued.PublisherMeta.CorrelationData)
	}
	if value, ok := queued.PublisherMeta.PayloadFormat.Value(); !ok || value != 1 {
		t.Fatalf("payload format was not copied: %v", queued.PublisherMeta.PayloadFormat)
	}
	if value, ok := queued.PublisherMeta.MessageExpiry.Value(); !ok || value != 90 {
		t.Fatalf("message expiry was not copied: %v", queued.PublisherMeta.MessageExpiry)
	}
	if !queued.BrokerMeta.Delivery.PublishedAt.Equal(publishedAt) || !queued.BrokerMeta.Delivery.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("delivery timing was not preserved: %+v", queued.BrokerMeta.Delivery)
	}
	if !queued.BrokerMeta.Queue.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("queue expiry = %v, want earlier protocol expiry %v", queued.BrokerMeta.Queue.ExpiresAt, expiresAt)
	}

	delivery := createDeliveryMessage(queued, "group", "m")
	defer message.Release(delivery)
	if !delivery.BrokerMeta.Delivery.PublishedAt.Equal(publishedAt) || !delivery.BrokerMeta.Delivery.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("queue delivery discarded protocol expiry: %+v", delivery.BrokerMeta.Delivery)
	}
}
