// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"bytes"
	"fmt"

	"github.com/absmach/fluxmq/amqp1/types"
)

// Message section descriptors.
const (
	DescriptorHeader                uint64 = 0x70
	DescriptorDeliveryAnnotations   uint64 = 0x71
	DescriptorMessageAnnotations    uint64 = 0x72
	DescriptorProperties            uint64 = 0x73
	DescriptorApplicationProperties uint64 = 0x74
	DescriptorData                  uint64 = 0x75
	DescriptorAMQPSequence          uint64 = 0x76
	DescriptorAMQPValue             uint64 = 0x77
	DescriptorFooter                uint64 = 0x78
)

// Header section.
type Header struct {
	Durable       bool
	Priority      uint8
	TTL           uint32 // milliseconds, 0 = no TTL
	FirstAcquirer bool
	DeliveryCount uint32
}

// Properties section.
type Properties struct {
	MessageID          any // string, uint64, UUID, or binary
	UserID             []byte
	To                 string
	Subject            string
	ReplyTo            string
	CorrelationID      any
	ContentType        types.Symbol
	ContentEncoding    types.Symbol
	AbsoluteExpiryTime types.Timestamp
	CreationTime       types.Timestamp
	GroupID            string
	GroupSequence      uint32
	ReplyToGroupID     string
}

// Message represents an AMQP 1.0 message with optional sections.
type Message struct {
	Header                *Header
	DeliveryAnnotations   map[types.Symbol]any
	MessageAnnotations    map[types.Symbol]any
	Properties            *Properties
	ApplicationProperties map[string]any
	Data                  [][]byte // one or more data sections
	Value                 any      // amqp-value section (mutually exclusive with Data)
}

// Encode serializes the message into wire format (concatenated described sections).
func (m *Message) Encode() ([]byte, error) {
	var buf bytes.Buffer

	if m.Header != nil {
		if err := encodeHeader(&buf, m.Header); err != nil {
			return nil, err
		}
	}

	if len(m.MessageAnnotations) > 0 {
		if err := encodeAnnotations(&buf, DescriptorMessageAnnotations, m.MessageAnnotations); err != nil {
			return nil, err
		}
	}

	if m.Properties != nil {
		if err := encodeProperties(&buf, m.Properties); err != nil {
			return nil, err
		}
	}

	if len(m.ApplicationProperties) > 0 {
		if err := encodeAppProperties(&buf, m.ApplicationProperties); err != nil {
			return nil, err
		}
	}

	for _, data := range m.Data {
		if err := types.WriteDescriptor(&buf, DescriptorData); err != nil {
			return nil, err
		}
		if err := types.WriteBinary(&buf, data); err != nil {
			return nil, err
		}
	}

	if m.Value != nil {
		if err := types.WriteDescriptor(&buf, DescriptorAMQPValue); err != nil {
			return nil, err
		}
		if err := types.WriteAny(&buf, m.Value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Decode parses message sections from the wire format payload.
func Decode(payload []byte) (*Message, error) {
	m := &Message{}
	r := bytes.NewReader(payload)

	for r.Len() > 0 {
		val, err := types.ReadType(r)
		if err != nil {
			return m, err
		}

		desc, ok := val.(*types.Described)
		if !ok {
			return m, fmt.Errorf("expected described type, got %T", val)
		}

		switch desc.Descriptor {
		case DescriptorHeader:
			m.Header = decodeHeader(desc.Value)
		case DescriptorDeliveryAnnotations:
			m.DeliveryAnnotations = decodeSymbolAnyMap(desc.Value)
		case DescriptorMessageAnnotations:
			m.MessageAnnotations = decodeSymbolAnyMap(desc.Value)
		case DescriptorProperties:
			m.Properties = decodeProperties(desc.Value)
		case DescriptorApplicationProperties:
			m.ApplicationProperties = decodeStringAnyMap(desc.Value)
		case DescriptorData:
			if data, ok := desc.Value.([]byte); ok {
				m.Data = append(m.Data, data)
			}
		case DescriptorAMQPValue:
			m.Value = desc.Value
		}
	}

	return m, nil
}

func encodeHeader(buf *bytes.Buffer, h *Header) error {
	var fields bytes.Buffer
	count := 0

	if err := types.WriteBool(&fields, h.Durable); err != nil {
		return err
	}
	count++
	if err := types.WriteUbyte(&fields, h.Priority); err != nil {
		return err
	}
	count++
	if err := types.WriteUint(&fields, h.TTL); err != nil {
		return err
	}
	count++
	if err := types.WriteBool(&fields, h.FirstAcquirer); err != nil {
		return err
	}
	count++
	if err := types.WriteUint(&fields, h.DeliveryCount); err != nil {
		return err
	}
	count++

	if err := types.WriteDescriptor(buf, DescriptorHeader); err != nil {
		return err
	}
	return types.WriteList(buf, fields.Bytes(), count)
}

func encodeProperties(buf *bytes.Buffer, p *Properties) error {
	var fields bytes.Buffer
	count := 0

	// message-id (0)
	if p.MessageID != nil {
		if err := types.WriteAny(&fields, p.MessageID); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// user-id (1)
	if p.UserID != nil {
		if err := types.WriteBinary(&fields, p.UserID); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// to (2)
	if p.To != "" {
		if err := types.WriteString(&fields, p.To); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// subject (3)
	if p.Subject != "" {
		if err := types.WriteString(&fields, p.Subject); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// reply-to (4)
	if p.ReplyTo != "" {
		if err := types.WriteString(&fields, p.ReplyTo); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// correlation-id (5)
	if p.CorrelationID != nil {
		if err := types.WriteAny(&fields, p.CorrelationID); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// content-type (6)
	if p.ContentType != "" {
		if err := types.WriteSymbol(&fields, p.ContentType); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	// content-encoding (7)
	if p.ContentEncoding != "" {
		if err := types.WriteSymbol(&fields, p.ContentEncoding); err != nil {
			return err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return err
		}
	}
	count++

	if err := types.WriteDescriptor(buf, DescriptorProperties); err != nil {
		return err
	}
	return types.WriteList(buf, fields.Bytes(), count)
}

func encodeAnnotations(buf *bytes.Buffer, descriptor uint64, ann map[types.Symbol]any) error {
	var pairs bytes.Buffer
	for k, v := range ann {
		if err := types.WriteSymbol(&pairs, k); err != nil {
			return err
		}
		if err := types.WriteAny(&pairs, v); err != nil {
			return err
		}
	}
	if err := types.WriteDescriptor(buf, descriptor); err != nil {
		return err
	}
	return types.WriteMap(buf, pairs.Bytes(), len(ann))
}

func encodeAppProperties(buf *bytes.Buffer, props map[string]any) error {
	var pairs bytes.Buffer
	for k, v := range props {
		if err := types.WriteString(&pairs, k); err != nil {
			return err
		}
		if err := types.WriteAny(&pairs, v); err != nil {
			return err
		}
	}
	if err := types.WriteDescriptor(buf, DescriptorApplicationProperties); err != nil {
		return err
	}
	return types.WriteMap(buf, pairs.Bytes(), len(props))
}

func decodeHeader(v any) *Header {
	fields, ok := v.([]any)
	if !ok {
		return &Header{}
	}
	h := &Header{}
	if len(fields) > 0 && fields[0] != nil {
		h.Durable, _ = fields[0].(bool)
	}
	if len(fields) > 1 && fields[1] != nil {
		h.Priority, _ = fields[1].(uint8)
	}
	if len(fields) > 2 && fields[2] != nil {
		h.TTL = anyToUint32(fields[2])
	}
	if len(fields) > 3 && fields[3] != nil {
		h.FirstAcquirer, _ = fields[3].(bool)
	}
	if len(fields) > 4 && fields[4] != nil {
		h.DeliveryCount = anyToUint32(fields[4])
	}
	return h
}

func decodeProperties(v any) *Properties {
	fields, ok := v.([]any)
	if !ok {
		return &Properties{}
	}
	p := &Properties{}
	if len(fields) > 0 && fields[0] != nil {
		p.MessageID = fields[0]
	}
	if len(fields) > 1 && fields[1] != nil {
		p.UserID, _ = fields[1].([]byte)
	}
	if len(fields) > 2 && fields[2] != nil {
		p.To, _ = fields[2].(string)
	}
	if len(fields) > 3 && fields[3] != nil {
		p.Subject, _ = fields[3].(string)
	}
	if len(fields) > 4 && fields[4] != nil {
		p.ReplyTo, _ = fields[4].(string)
	}
	if len(fields) > 5 && fields[5] != nil {
		p.CorrelationID = fields[5]
	}
	if len(fields) > 6 && fields[6] != nil {
		p.ContentType, _ = fields[6].(types.Symbol)
	}
	if len(fields) > 7 && fields[7] != nil {
		p.ContentEncoding, _ = fields[7].(types.Symbol)
	}
	return p
}

func decodeSymbolAnyMap(v any) map[types.Symbol]any {
	m, ok := v.(map[any]any)
	if !ok {
		return nil
	}
	result := make(map[types.Symbol]any, len(m))
	for k, val := range m {
		if sym, ok := k.(types.Symbol); ok {
			result[sym] = val
		}
	}
	return result
}

func decodeStringAnyMap(v any) map[string]any {
	m, ok := v.(map[any]any)
	if !ok {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, val := range m {
		if s, ok := k.(string); ok {
			result[s] = val
		}
	}
	return result
}

func anyToUint32(v any) uint32 {
	switch val := v.(type) {
	case uint32:
		return val
	case uint64:
		return uint32(val)
	case uint8:
		return uint32(val)
	default:
		return 0
	}
}
