// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"strconv"
	"strings"
)

const (
	PropertyClientID   = "client_id"
	PropertyExternalID = "external_id"
	PropertyProtocol   = "protocol"

	PropertyMessageID   = "message-id"
	PropertyGroupID     = "group-id"
	PropertyQueueName   = "queue"
	PropertyOffset      = "offset"
	PropertySourceTopic = "x-source-topic"

	PropertyStreamOffset    = "x-stream-offset"
	PropertyStreamTimestamp = "x-stream-timestamp"
	PropertyWorkCommitted   = "x-work-committed-offset"
	PropertyWorkAcked       = "x-work-acked"
	PropertyWorkGroup       = "x-work-group"

	PropertyTransferID          = "x-dlq-transfer-id"
	PropertyDLQReason           = "x-dlq-reason"
	PropertyForwardTargetQueues = "x-queue-forward-targets"

	PropertyTraceParent = "_flux.traceparent"
	PropertyTraceState  = "_flux.tracestate"
	PropertyTraceID     = "_flux.trace_id"

	ReservedPropertyPrefix = "_flux."
)

// Projection controls which broker-owned namespaces cross a protocol
// boundary. Queue and transfer metadata are consumer-facing delivery
// semantics; source and trace context are restricted to trusted services.
type Projection struct {
	Queue    bool
	Transfer bool
	Source   bool
	Trace    bool
}

// PublicProjection exposes delivery semantics without broker identity or
// tracing internals.
var PublicProjection = Projection{Queue: true, Transfer: true}

// TrustedServiceProjection exposes all typed broker metadata.
var TrustedServiceProjection = Projection{Queue: true, Transfer: true, Source: true, Trace: true}

// IsReservedProperty reports whether key belongs to any broker-owned
// namespace and therefore cannot be accepted as a user property.
func IsReservedProperty(key string) bool {
	if strings.HasPrefix(key, ReservedPropertyPrefix) {
		return true
	}
	switch key {
	case PropertyClientID, PropertyExternalID, PropertyProtocol,
		PropertyMessageID, PropertyGroupID, PropertyQueueName, PropertyOffset,
		PropertySourceTopic, PropertyStreamOffset, PropertyStreamTimestamp,
		PropertyWorkCommitted, PropertyWorkAcked, PropertyWorkGroup,
		PropertyTransferID, PropertyDLQReason, PropertyForwardTargetQueues:
		return true
	default:
		return false
	}
}

// FilterUserProperties copies only publisher-owned properties.
func FilterUserProperties(properties map[string]string) map[string]string {
	if len(properties) == 0 {
		return nil
	}
	filtered := make(map[string]string, len(properties))
	for key, value := range properties {
		if !IsReservedProperty(key) {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// SourceFromProperties decodes authenticated broker-boundary origin fields.
func SourceFromProperties(properties map[string]string) SourceMetadata {
	return SourceMetadata{
		ClientID:   properties[PropertyClientID],
		ExternalID: properties[PropertyExternalID],
		Protocol:   Protocol(properties[PropertyProtocol]),
	}
}

// TraceFromProperties decodes broker-owned trace context at a trusted
// boundary. Public protocol ingress must not call this on untrusted fields.
func TraceFromProperties(properties map[string]string) TraceMetadata {
	return TraceMetadata{
		TraceParent: properties[PropertyTraceParent],
		TraceState:  properties[PropertyTraceState],
		TraceID:     properties[PropertyTraceID],
	}
}

// ApplyTrustedProperties decodes the cluster protobuf property projection into
// typed namespaces. It is only for authenticated
// broker-to-broker and trusted-service boundaries; public ingress must call
// FilterUserProperties and set SourceMetadata from its authenticated session.
func ApplyTrustedProperties(envelope *Envelope, properties map[string]string) {
	if envelope == nil {
		return
	}
	envelope.User.Properties = FilterUserProperties(properties)
	envelope.Broker.Source = SourceFromProperties(properties)
	envelope.Broker.Source.Topic = properties[PropertySourceTopic]

	queue := &envelope.Broker.Queue
	queue.MessageID = properties[PropertyMessageID]
	queue.Name = properties[PropertyQueueName]
	queue.GroupID = properties[PropertyGroupID]
	queue.Offset, _ = strconv.ParseUint(properties[PropertyOffset], 10, 64)
	if rawOffset, ok := properties[PropertyStreamOffset]; ok {
		stream := &StreamMetadata{}
		stream.Offset, _ = strconv.ParseUint(rawOffset, 10, 64)
		stream.Timestamp, _ = strconv.ParseInt(properties[PropertyStreamTimestamp], 10, 64)
		if rawCommitted, exists := properties[PropertyWorkCommitted]; exists {
			stream.HasCommittedOffset = true
			stream.CommittedOffset, _ = strconv.ParseUint(rawCommitted, 10, 64)
		}
		stream.WorkAcknowledged, _ = strconv.ParseBool(properties[PropertyWorkAcked])
		stream.WorkGroup = properties[PropertyWorkGroup]
		queue.Stream = stream
	}

	envelope.Broker.Transfer.ID = properties[PropertyTransferID]
	envelope.Broker.Transfer.FailureReason = properties[PropertyDLQReason]
	envelope.Broker.Trace = TraceFromProperties(properties)
}

// ProjectProperties returns a fresh wire property map. Broker-owned values are
// always stamped after user properties, so user input cannot forge them.
func ProjectProperties(envelope *Envelope, projection Projection) map[string]string {
	if envelope == nil {
		return nil
	}
	properties := FilterUserProperties(envelope.User.Properties)

	if projection.Queue && hasQueueProjection(envelope.Broker.Queue) {
		properties = ensureProperties(properties)
		projectQueueProperties(properties, envelope.Broker.Source, envelope.Broker.Queue)
	}
	if projection.Transfer {
		if transfer := envelope.Broker.Transfer; transfer.ID != "" || transfer.FailureReason != "" {
			properties = ensureProperties(properties)
			if transfer.ID != "" {
				properties[PropertyTransferID] = transfer.ID
			}
			if transfer.FailureReason != "" {
				properties[PropertyDLQReason] = transfer.FailureReason
			}
		}
	}
	if projection.Source {
		if source := envelope.Broker.Source; source.ClientID != "" || source.ExternalID != "" || source.Protocol != "" {
			properties = ensureProperties(properties)
			if source.ClientID != "" {
				properties[PropertyClientID] = source.ClientID
			}
			if source.ExternalID != "" {
				properties[PropertyExternalID] = source.ExternalID
			}
			if source.Protocol != "" {
				properties[PropertyProtocol] = string(source.Protocol)
			}
		}
	}
	if projection.Trace {
		if trace := envelope.Broker.Trace; trace.TraceParent != "" || trace.TraceState != "" || trace.TraceID != "" {
			properties = ensureProperties(properties)
			if trace.TraceParent != "" {
				properties[PropertyTraceParent] = trace.TraceParent
			}
			if trace.TraceState != "" {
				properties[PropertyTraceState] = trace.TraceState
			}
			if trace.TraceID != "" {
				properties[PropertyTraceID] = trace.TraceID
			}
		}
	}

	if len(properties) == 0 {
		return nil
	}
	return properties
}

func ensureProperties(properties map[string]string) map[string]string {
	if properties == nil {
		return make(map[string]string)
	}
	return properties
}

func hasQueueProjection(queue QueueMetadata) bool {
	return queue.MessageID != "" || queue.Name != "" || queue.GroupID != "" || queue.Stream != nil
}

func projectQueueProperties(properties map[string]string, source SourceMetadata, queue QueueMetadata) {
	if queue.MessageID != "" {
		properties[PropertyMessageID] = queue.MessageID
	}
	if queue.GroupID != "" {
		properties[PropertyGroupID] = queue.GroupID
	}
	if queue.Name != "" {
		properties[PropertyQueueName] = queue.Name
	}
	properties[PropertyOffset] = strconv.FormatUint(queue.Offset, 10)
	properties[PropertySourceTopic] = source.Topic

	if queue.Stream == nil {
		return
	}
	stream := queue.Stream
	properties[PropertyStreamOffset] = strconv.FormatUint(stream.Offset, 10)
	if stream.Timestamp != 0 {
		properties[PropertyStreamTimestamp] = strconv.FormatInt(stream.Timestamp, 10)
	}
	if stream.HasCommittedOffset {
		properties[PropertyWorkCommitted] = strconv.FormatUint(stream.CommittedOffset, 10)
		properties[PropertyWorkAcked] = strconv.FormatBool(stream.WorkAcknowledged)
		if stream.WorkGroup != "" {
			properties[PropertyWorkGroup] = stream.WorkGroup
		}
	}
}

// QueueOffsetFromProperties recovers the queue offset a delivery carries.
//
// Protocol adapters that receive a delivery as a property map rather than an
// envelope use this at the delivery boundary, once, to resolve the offset they
// will later settle on. Settlement itself takes a uint64: a textual identifier
// is never parsed back into an offset.
//
// ok reports whether the delivery came from a queue at all. A malformed value
// is reported as absent rather than silently settling offset 0.
func QueueOffsetFromProperties(properties map[string]string) (offset uint64, ok bool) {
	raw, present := properties[PropertyOffset]
	if !present {
		return 0, false
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}
