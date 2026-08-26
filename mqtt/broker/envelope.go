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
	envelope.PublisherMeta.Properties = message.FilterUserProperties(properties)
	envelope.BrokerMeta.Source = message.SourceMetadata{
		ClientID:   clientID,
		ExternalID: externalID,
		Protocol:   message.ProtocolMQTT,
	}
	envelope.BrokerMeta.Trace = message.TraceFromProperties(properties)
	envelope.BrokerMeta.Delivery.QoS = qos
	envelope.BrokerMeta.Delivery.Retain = retain
	return envelope
}

func setMQTT5Metadata(envelope *message.Envelope, expiry *uint32, expiresAt, publishedAt time.Time, payloadFormat *byte, contentType, responseTopic string, correlationData []byte) {
	if expiry != nil {
		value := *expiry
		envelope.PublisherMeta.MessageExpiry = &value
	}
	if payloadFormat != nil {
		value := *payloadFormat
		envelope.PublisherMeta.PayloadFormat = &value
	}
	envelope.PublisherMeta.ContentType = contentType
	envelope.PublisherMeta.ResponseTopic = responseTopic
	envelope.PublisherMeta.CorrelationData = bytes.Clone(correlationData)
	envelope.BrokerMeta.Delivery.ExpiresAt = expiresAt
	envelope.BrokerMeta.Delivery.PublishedAt = publishedAt
}
