// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package sasl

import (
	"bytes"
	"fmt"

	"github.com/absmach/fluxmq/amqp1/types"
)

// SASL frame descriptors.
const (
	DescriptorMechanisms uint64 = 0x40
	DescriptorInit       uint64 = 0x41
	DescriptorChallenge  uint64 = 0x42
	DescriptorResponse   uint64 = 0x43
	DescriptorOutcome    uint64 = 0x44
)

// SASL outcome codes.
const (
	CodeOK      uint8 = 0
	CodeAuth    uint8 = 1 // authentication failed
	CodeSys     uint8 = 2 // system error
	CodeSysTemp uint8 = 4 // temporary system error
)

// Mechanism names.
var (
	MechPLAIN     = types.Symbol("PLAIN")
	MechANONYMOUS = types.Symbol("ANONYMOUS")
)

// Mechanisms (0x40) - server sends available mechanisms.
type Mechanisms struct {
	Mechanisms []types.Symbol
}

func (m *Mechanisms) Encode() ([]byte, error) {
	var fields bytes.Buffer

	// Encode mechanisms as an array of symbols
	var arrayData bytes.Buffer
	for _, mech := range m.Mechanisms {
		b := []byte(mech)
		if len(b) <= 255 {
			arrayData.WriteByte(byte(len(b)))
			arrayData.Write(b)
		}
	}
	if err := types.WriteArray(&fields, types.TypeSymbolShort, arrayData.Bytes(), len(m.Mechanisms)); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorMechanisms); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), 1); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Init (0x41) - client selects mechanism and provides initial response.
type Init struct {
	Mechanism       types.Symbol
	InitialResponse []byte
	Hostname        string
}

func (i *Init) Encode() ([]byte, error) {
	var fields bytes.Buffer

	if err := types.WriteSymbol(&fields, i.Mechanism); err != nil {
		return nil, err
	}
	if i.InitialResponse != nil {
		if err := types.WriteBinary(&fields, i.InitialResponse); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	if i.Hostname != "" {
		if err := types.WriteString(&fields, i.Hostname); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorInit); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), 3); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Outcome (0x44) - server sends authentication result.
type Outcome struct {
	Code           uint8
	AdditionalData []byte
}

func (o *Outcome) Encode() ([]byte, error) {
	var fields bytes.Buffer

	if err := types.WriteUbyte(&fields, o.Code); err != nil {
		return nil, err
	}
	if o.AdditionalData != nil {
		if err := types.WriteBinary(&fields, o.AdditionalData); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorOutcome); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), 2); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeSASL decodes a SASL frame body into the appropriate type.
func DecodeSASL(body []byte) (uint64, any, error) {
	r := bytes.NewReader(body)
	descriptor, fields, err := types.ReadListFields(r)
	if err != nil {
		return 0, nil, err
	}

	switch descriptor {
	case DescriptorMechanisms:
		m := &Mechanisms{}
		if len(fields) > 0 && fields[0] != nil {
			if arr, ok := fields[0].([]any); ok {
				for _, v := range arr {
					if sym, ok := v.(types.Symbol); ok {
						m.Mechanisms = append(m.Mechanisms, sym)
					}
				}
			}
		}
		return descriptor, m, nil

	case DescriptorInit:
		i := &Init{}
		if len(fields) > 0 && fields[0] != nil {
			i.Mechanism, _ = fields[0].(types.Symbol)
		}
		if len(fields) > 1 && fields[1] != nil {
			i.InitialResponse, _ = fields[1].([]byte)
		}
		if len(fields) > 2 && fields[2] != nil {
			i.Hostname, _ = fields[2].(string)
		}
		return descriptor, i, nil

	case DescriptorOutcome:
		o := &Outcome{}
		if len(fields) > 0 && fields[0] != nil {
			o.Code = uint8(fields[0].(uint8))
		}
		if len(fields) > 1 && fields[1] != nil {
			o.AdditionalData, _ = fields[1].([]byte)
		}
		return descriptor, o, nil

	default:
		return descriptor, nil, fmt.Errorf("unknown SASL descriptor: 0x%02x", descriptor)
	}
}

// ParsePLAIN parses a SASL PLAIN initial response.
// Format: \0<authzid>\0<authcid>\0<password> or \0<authcid>\0<password>
func ParsePLAIN(response []byte) (authzID, username, password string, err error) {
	if len(response) == 0 {
		return "", "", "", fmt.Errorf("empty PLAIN response")
	}

	parts := bytes.SplitN(response, []byte{0}, -1)
	switch len(parts) {
	case 3:
		// \0authcid\0password (authzid is empty)
		return string(parts[0]), string(parts[1]), string(parts[2]), nil
	default:
		return "", "", "", fmt.Errorf("invalid PLAIN response format: expected 3 parts, got %d", len(parts))
	}
}
