// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"

	"github.com/absmach/fluxmq/amqp1/types"
)

// Descriptors for Source and Target.
const (
	DescriptorSource uint64 = 0x28
	DescriptorTarget uint64 = 0x29
)

// CapQueue is the capability symbol indicating a queue node.
const CapQueue types.Symbol = "queue"

// Source represents an AMQP source.
type Source struct {
	Address          string
	Durable          uint32
	ExpiryPolicy     types.Symbol
	Timeout          uint32
	Dynamic          bool
	DistributionMode types.Symbol
	Capabilities     []types.Symbol
}

// Encode serializes the Source as a described list.
func (s *Source) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	// Address (index 0)
	if s.Address != "" {
		if err := types.WriteString(&fields, s.Address); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	// Durable (index 1)
	if err := types.WriteUint(&fields, s.Durable); err != nil {
		return nil, err
	}
	count++

	// ExpiryPolicy (index 2)
	if s.ExpiryPolicy != "" {
		if err := types.WriteSymbol(&fields, s.ExpiryPolicy); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteSymbol(&fields, "session-end"); err != nil {
			return nil, err
		}
	}
	count++

	// Timeout (index 3)
	if err := types.WriteUint(&fields, s.Timeout); err != nil {
		return nil, err
	}
	count++

	// Dynamic (index 4)
	if err := types.WriteBool(&fields, s.Dynamic); err != nil {
		return nil, err
	}
	count++

	if len(s.Capabilities) > 0 {
		// dynamic-node-properties (5) - null
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
		count++

		// distribution-mode (6) - null
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
		count++

		// filter (7) - null
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
		count++

		// default-outcome (8) - null
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
		count++

		// outcomes (9) - null
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
		count++

		// capabilities (10)
		if err := types.WriteSymbolArray(&fields, s.Capabilities); err != nil {
			return nil, err
		}
		count++
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorSource); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), count); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeSource decodes a Source from list fields.
func DecodeSource(fields []any) *Source {
	s := &Source{}
	if len(fields) > 0 && fields[0] != nil {
		s.Address, _ = fields[0].(string)
	}
	if len(fields) > 1 && fields[1] != nil {
		s.Durable = toUint32(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		s.ExpiryPolicy, _ = fields[2].(types.Symbol)
	}
	if len(fields) > 3 && fields[3] != nil {
		s.Timeout = toUint32(fields[3])
	}
	if len(fields) > 4 && fields[4] != nil {
		s.Dynamic, _ = fields[4].(bool)
	}
	if len(fields) > 6 && fields[6] != nil {
		s.DistributionMode, _ = fields[6].(types.Symbol)
	}
	if len(fields) > 10 && fields[10] != nil {
		s.Capabilities = decodeCapabilities(fields[10])
	}
	return s
}

// Target represents an AMQP target.
type Target struct {
	Address      string
	Durable      uint32
	ExpiryPolicy types.Symbol
	Timeout      uint32
	Dynamic      bool
	Capabilities []types.Symbol
}

// Encode serializes the Target as a described list.
func (t *Target) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if t.Address != "" {
		if err := types.WriteString(&fields, t.Address); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if err := types.WriteUint(&fields, t.Durable); err != nil {
		return nil, err
	}
	count++

	if t.ExpiryPolicy != "" {
		if err := types.WriteSymbol(&fields, t.ExpiryPolicy); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteSymbol(&fields, "session-end"); err != nil {
			return nil, err
		}
	}
	count++

	if err := types.WriteUint(&fields, t.Timeout); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteBool(&fields, t.Dynamic); err != nil {
		return nil, err
	}
	count++

	if len(t.Capabilities) > 0 {
		// dynamic-node-properties (5) - null
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
		count++

		// capabilities (6)
		if err := types.WriteSymbolArray(&fields, t.Capabilities); err != nil {
			return nil, err
		}
		count++
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorTarget); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), count); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeTarget decodes a Target from list fields.
func DecodeTarget(fields []any) *Target {
	t := &Target{}
	if len(fields) > 0 && fields[0] != nil {
		t.Address, _ = fields[0].(string)
	}
	if len(fields) > 1 && fields[1] != nil {
		t.Durable = toUint32(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		t.ExpiryPolicy, _ = fields[2].(types.Symbol)
	}
	if len(fields) > 3 && fields[3] != nil {
		t.Timeout = toUint32(fields[3])
	}
	if len(fields) > 4 && fields[4] != nil {
		t.Dynamic, _ = fields[4].(bool)
	}
	if len(fields) > 6 && fields[6] != nil {
		t.Capabilities = decodeCapabilities(fields[6])
	}
	return t
}

// decodeCapabilities decodes a "multiple symbol" field — either a single symbol or an array of symbols.
func decodeCapabilities(field any) []types.Symbol {
	switch v := field.(type) {
	case types.Symbol:
		return []types.Symbol{v}
	case []any:
		caps := make([]types.Symbol, 0, len(v))
		for _, item := range v {
			if s, ok := item.(types.Symbol); ok {
				caps = append(caps, s)
			}
		}
		return caps
	default:
		return nil
	}
}

// HasCapability checks whether a list of capabilities contains the given symbol.
func HasCapability(caps []types.Symbol, cap types.Symbol) bool {
	for _, c := range caps {
		if c == cap {
			return true
		}
	}
	return false
}

func toUint32(v any) uint32 {
	switch val := v.(type) {
	case uint32:
		return val
	case uint64:
		return uint32(val)
	case uint8:
		return uint32(val)
	case uint16:
		return uint16ToUint32(val)
	default:
		return 0
	}
}

func uint16ToUint32(v uint16) uint32 {
	return uint32(v)
}

func toUint64(v any) uint64 {
	switch val := v.(type) {
	case uint64:
		return val
	case uint32:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	default:
		return 0
	}
}

func toBool(v any) bool {
	b, _ := v.(bool)
	return b
}
