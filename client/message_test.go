// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"
	"time"
)

func TestNewMessage(t *testing.T) {
	msg := NewMessage("test/topic", []byte("payload"), 1, true)

	if msg.Topic != "test/topic" {
		t.Errorf("expected topic 'test/topic', got %s", msg.Topic)
	}
	if string(msg.Payload) != "payload" {
		t.Errorf("expected payload 'payload', got %s", string(msg.Payload))
	}
	if msg.QoS != 1 {
		t.Errorf("expected QoS 1, got %d", msg.QoS)
	}
	if !msg.Retain {
		t.Error("expected Retain to be true")
	}
	if msg.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}
}

func TestMessageCopy(t *testing.T) {
	pf := byte(1)
	me := uint32(3600)

	original := &Message{
		Topic:           "test/topic",
		Payload:         []byte("payload"),
		QoS:             2,
		Retain:          true,
		Dup:             true,
		PacketID:        123,
		Timestamp:       time.Now(),
		PayloadFormat:   &pf,
		MessageExpiry:   &me,
		ContentType:     "application/json",
		ResponseTopic:   "response/topic",
		CorrelationData: []byte("correlation"),
		UserProperties:  map[string]string{"key": "value"},
		SubscriptionIDs: []uint32{1, 2, 3},
	}

	copied := original.Copy()

	// Verify values are copied
	if copied.Topic != original.Topic {
		t.Error("Topic not copied")
	}
	if string(copied.Payload) != string(original.Payload) {
		t.Error("Payload not copied")
	}
	if copied.QoS != original.QoS {
		t.Error("QoS not copied")
	}
	if copied.Retain != original.Retain {
		t.Error("Retain not copied")
	}
	if copied.Dup != original.Dup {
		t.Error("Dup not copied")
	}
	if copied.PacketID != original.PacketID {
		t.Error("PacketID not copied")
	}
	if *copied.PayloadFormat != *original.PayloadFormat {
		t.Error("PayloadFormat not copied")
	}
	if *copied.MessageExpiry != *original.MessageExpiry {
		t.Error("MessageExpiry not copied")
	}
	if copied.ContentType != original.ContentType {
		t.Error("ContentType not copied")
	}
	if copied.ResponseTopic != original.ResponseTopic {
		t.Error("ResponseTopic not copied")
	}
	if string(copied.CorrelationData) != string(original.CorrelationData) {
		t.Error("CorrelationData not copied")
	}
	if copied.UserProperties["key"] != original.UserProperties["key"] {
		t.Error("UserProperties not copied")
	}
	if len(copied.SubscriptionIDs) != len(original.SubscriptionIDs) {
		t.Error("SubscriptionIDs not copied")
	}

	// Verify deep copy (modifications don't affect original)
	copied.Payload[0] = 'X'
	if original.Payload[0] == 'X' {
		t.Error("Payload should be deep copied")
	}

	*copied.PayloadFormat = 99
	if *original.PayloadFormat == 99 {
		t.Error("PayloadFormat should be deep copied")
	}

	copied.UserProperties["key"] = "modified"
	if original.UserProperties["key"] == "modified" {
		t.Error("UserProperties should be deep copied")
	}

	copied.CorrelationData[0] = 'X'
	if original.CorrelationData[0] == 'X' {
		t.Error("CorrelationData should be deep copied")
	}

	copied.SubscriptionIDs[0] = 999
	if original.SubscriptionIDs[0] == 999 {
		t.Error("SubscriptionIDs should be deep copied")
	}
}

func TestMessageCopyNil(t *testing.T) {
	var msg *Message = nil
	copied := msg.Copy()
	if copied != nil {
		t.Error("Copy of nil should return nil")
	}
}

func TestMessageCopyWithNilFields(t *testing.T) {
	msg := &Message{
		Topic:   "topic",
		Payload: nil,
	}

	copied := msg.Copy()
	if copied.Payload != nil {
		t.Error("nil Payload should remain nil")
	}
}

func TestToken(t *testing.T) {
	tok := newToken()

	// Should not be done initially
	select {
	case <-tok.Done():
		t.Error("token should not be done initially")
	default:
	}

	// Complete the token
	tok.complete(nil)

	// Should be done now
	select {
	case <-tok.Done():
	default:
		t.Error("token should be done after complete")
	}

	if tok.Error() != nil {
		t.Error("Error should be nil")
	}
}

func TestTokenWithError(t *testing.T) {
	tok := newToken()
	tok.complete(ErrTimeout)

	if tok.Error() != ErrTimeout {
		t.Errorf("expected ErrTimeout, got %v", tok.Error())
	}
}

func TestTokenWait(t *testing.T) {
	tok := newToken()

	go func() {
		time.Sleep(10 * time.Millisecond)
		tok.complete(nil)
	}()

	err := tok.Wait()
	if err != nil {
		t.Errorf("Wait should succeed, got: %v", err)
	}
}

func TestTokenWaitTimeout(t *testing.T) {
	tok := newToken()

	err := tok.WaitTimeout(10 * time.Millisecond)
	if err != ErrTimeout {
		t.Errorf("expected ErrTimeout, got: %v", err)
	}
}

func TestTokenWaitTimeoutSuccess(t *testing.T) {
	tok := newToken()

	go func() {
		time.Sleep(5 * time.Millisecond)
		tok.complete(nil)
	}()

	err := tok.WaitTimeout(1 * time.Second)
	if err != nil {
		t.Errorf("WaitTimeout should succeed, got: %v", err)
	}
}
