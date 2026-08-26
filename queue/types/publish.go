// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"time"

	"github.com/absmach/fluxmq/message"
)

// PublishRequest encapsulates publish data for queue routing.
type PublishRequest struct {
	Source              message.SourceMetadata
	Trace               message.TraceMetadata
	Topic               string
	Payload             []byte
	Key                 []byte
	Headers             map[string][]byte
	Properties          map[string]string
	ContentType         string
	ContentEncoding     string
	ResponseTopic       string
	CorrelationData     []byte
	PayloadFormat       *byte
	MessageExpiry       *uint32
	PublishedAt         time.Time
	ExpiresAt           time.Time
	ForwardTargetQueues []string
}

// Envelope renders the request for the cluster wire. The caller owns the result
// and must release it; the envelope borrows the request's slices and maps, so
// they must stay alive until then.
//
// It replaces a map[string]string projection that could only carry the source,
// trace and user properties. The key, headers, content type, content encoding,
// response topic, correlation data, payload format, message expiry and both
// timestamps had no representation in that map and were dropped on every
// cluster hop.
func (p PublishRequest) Envelope() *message.Envelope {
	msg := message.New(p.Topic, p.Payload)
	msg.User.Key = p.Key
	msg.User.Headers = p.Headers
	msg.User.Properties = message.FilterUserProperties(p.Properties)
	msg.User.ContentType = p.ContentType
	msg.User.ContentEncoding = p.ContentEncoding
	msg.User.ResponseTopic = p.ResponseTopic
	msg.User.CorrelationData = p.CorrelationData
	msg.User.PayloadFormat = p.PayloadFormat
	msg.User.MessageExpiry = p.MessageExpiry
	msg.Broker.Source = p.Source
	msg.Broker.Trace = p.Trace
	msg.Broker.Delivery.PublishedAt = p.PublishedAt
	msg.Broker.Delivery.ExpiresAt = p.ExpiresAt
	return msg
}

// PublishFromEnvelope is the inverse of PublishRequest.Envelope. The result
// borrows msg's payload and metadata and is only valid while msg is.
// ForwardTargetQueues is a routing control rather than message content, so it
// travels beside the envelope and is not restored here.
func PublishFromEnvelope(msg *message.Envelope) PublishRequest {
	if msg == nil {
		return PublishRequest{}
	}
	return PublishRequest{
		Source:          msg.Broker.Source,
		Trace:           msg.Broker.Trace,
		Topic:           msg.Topic,
		Payload:         msg.PayloadBytes(),
		Key:             msg.User.Key,
		Headers:         msg.User.Headers,
		Properties:      msg.User.Properties,
		ContentType:     msg.User.ContentType,
		ContentEncoding: msg.User.ContentEncoding,
		ResponseTopic:   msg.User.ResponseTopic,
		CorrelationData: msg.User.CorrelationData,
		PayloadFormat:   msg.User.PayloadFormat,
		MessageExpiry:   msg.User.MessageExpiry,
		PublishedAt:     msg.Broker.Delivery.PublishedAt,
		ExpiresAt:       msg.Broker.Delivery.ExpiresAt,
	}
}

// PublishMode controls how the queue manager should handle a publish.
type PublishMode int

const (
	PublishNormal PublishMode = iota
	PublishLocal
	PublishForwarded
)
