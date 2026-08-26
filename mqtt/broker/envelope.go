// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"time"

	"github.com/absmach/fluxmq/message"
)

func newMQTTEnvelope(topic string, data []byte, clientID, externalID string, qos byte, retain bool, properties map[string]string) *message.Envelope {
	envelope := message.New(topic, data)
	envelope.User.Properties = message.FilterUserProperties(properties)
	envelope.Broker.Source = message.SourceMetadata{
		ClientID:   clientID,
		ExternalID: externalID,
		Protocol:   message.ProtocolMQTT,
	}
	envelope.Broker.Trace = message.TraceFromProperties(properties)
	envelope.Broker.Delivery.QoS = qos
	envelope.Broker.Delivery.Retain = retain
	return envelope
}

func setMQTT5Metadata(envelope *message.Envelope, expiry *uint32, expiresAt, publishedAt time.Time, payloadFormat *byte, contentType, responseTopic string, correlationData []byte) {
	if expiry != nil {
		value := *expiry
		envelope.User.MessageExpiry = &value
	}
	if payloadFormat != nil {
		value := *payloadFormat
		envelope.User.PayloadFormat = &value
	}
	envelope.User.ContentType = contentType
	envelope.User.ResponseTopic = responseTopic
	envelope.User.CorrelationData = bytes.Clone(correlationData)
	envelope.Broker.Delivery.ExpiresAt = expiresAt
	envelope.Broker.Delivery.PublishedAt = publishedAt
}
