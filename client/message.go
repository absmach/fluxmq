// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "time"

// Message represents an MQTT message.
type Message struct {
	Topic     string
	Payload   []byte
	QoS       byte
	Retain    bool
	Dup       bool
	PacketID  uint16
	Timestamp time.Time

	// MQTT 5.0 properties
	PayloadFormat   *byte
	MessageExpiry   *uint32
	ContentType     string
	ResponseTopic   string
	CorrelationData []byte
	UserProperties  map[string]string
	SubscriptionIDs []uint32
}

// NewMessage creates a new message with the given parameters.
func NewMessage(topic string, payload []byte, qos byte, retain bool) *Message {
	return &Message{
		Topic:     topic,
		Payload:   payload,
		QoS:       qos,
		Retain:    retain,
		Timestamp: time.Now(),
	}
}

// Copy creates a deep copy of the message.
func (m *Message) Copy() *Message {
	if m == nil {
		return nil
	}

	msg := &Message{
		Topic:     m.Topic,
		QoS:       m.QoS,
		Retain:    m.Retain,
		Dup:       m.Dup,
		PacketID:  m.PacketID,
		Timestamp: m.Timestamp,
	}

	if m.Payload != nil {
		msg.Payload = make([]byte, len(m.Payload))
		copy(msg.Payload, m.Payload)
	}

	if m.PayloadFormat != nil {
		pf := *m.PayloadFormat
		msg.PayloadFormat = &pf
	}

	if m.MessageExpiry != nil {
		me := *m.MessageExpiry
		msg.MessageExpiry = &me
	}

	msg.ContentType = m.ContentType
	msg.ResponseTopic = m.ResponseTopic

	if m.CorrelationData != nil {
		msg.CorrelationData = make([]byte, len(m.CorrelationData))
		copy(msg.CorrelationData, m.CorrelationData)
	}

	if m.UserProperties != nil {
		msg.UserProperties = make(map[string]string, len(m.UserProperties))
		for k, v := range m.UserProperties {
			msg.UserProperties[k] = v
		}
	}

	if m.SubscriptionIDs != nil {
		msg.SubscriptionIDs = make([]uint32, len(m.SubscriptionIDs))
		copy(msg.SubscriptionIDs, m.SubscriptionIDs)
	}

	return msg
}

// Token represents an asynchronous operation result.
type Token interface {
	Wait() error
	WaitTimeout(time.Duration) error
	Done() <-chan struct{}
	Error() error
}

// token is the default Token implementation.
type token struct {
	done chan struct{}
	err  error
}

// newToken creates a new token.
func newToken() *token {
	return &token{
		done: make(chan struct{}),
	}
}

// complete signals the token as done.
func (t *token) complete(err error) {
	t.err = err
	close(t.done)
}

// Wait blocks until the operation completes.
func (t *token) Wait() error {
	<-t.done
	return t.err
}

// WaitTimeout blocks until the operation completes or times out.
func (t *token) WaitTimeout(timeout time.Duration) error {
	select {
	case <-t.done:
		return t.err
	case <-time.After(timeout):
		return ErrTimeout
	}
}

// Done returns a channel that closes when the operation completes.
func (t *token) Done() <-chan struct{} {
	return t.done
}

// Error returns the operation error (may be nil).
func (t *token) Error() error {
	return t.err
}

// PublishToken is returned by Publish operations.
type PublishToken struct {
	*token
	MessageID uint16
}

// SubscribeToken is returned by Subscribe operations.
type SubscribeToken struct {
	*token
	ReturnCodes []byte
}

// UnsubscribeToken is returned by Unsubscribe operations.
type UnsubscribeToken struct {
	*token
}
