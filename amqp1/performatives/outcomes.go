// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"

	"github.com/absmach/fluxmq/amqp1/types"
)

// Outcome descriptors.
const (
	DescriptorAccepted uint64 = 0x24
	DescriptorRejected uint64 = 0x25
	DescriptorReleased uint64 = 0x26
	DescriptorModified uint64 = 0x27
)

// Accepted outcome.
type Accepted struct{}

func (a *Accepted) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorAccepted); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, nil, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Rejected outcome with optional error.
type Rejected struct {
	Error *Error
}

func (r *Rejected) Encode() ([]byte, error) {
	var fields bytes.Buffer
	if r.Error != nil {
		errBytes, err := r.Error.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(errBytes)
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorRejected); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), 1); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Released outcome.
type Released struct{}

func (r *Released) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorReleased); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, nil, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Modified outcome.
type Modified struct {
	DeliveryFailed    bool
	UndeliverableHere bool
}

func (m *Modified) Encode() ([]byte, error) {
	var fields bytes.Buffer
	if err := types.WriteBool(&fields, m.DeliveryFailed); err != nil {
		return nil, err
	}
	if err := types.WriteBool(&fields, m.UndeliverableHere); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorModified); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), 2); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeOutcome decodes a disposition state from a described type.
func DecodeOutcome(desc *types.Described) any {
	switch desc.Descriptor {
	case DescriptorAccepted:
		return &Accepted{}
	case DescriptorRejected:
		r := &Rejected{}
		if fields, ok := desc.Value.([]any); ok && len(fields) > 0 {
			if errDesc, ok := fields[0].(*types.Described); ok && errDesc.Descriptor == DescriptorError {
				if errFields, ok := errDesc.Value.([]any); ok {
					r.Error = DecodeError(errFields)
				}
			}
		}
		return r
	case DescriptorReleased:
		return &Released{}
	case DescriptorModified:
		m := &Modified{}
		if fields, ok := desc.Value.([]any); ok {
			if len(fields) > 0 {
				if v, ok := fields[0].(bool); ok {
					m.DeliveryFailed = v
				}
			}
			if len(fields) > 1 {
				if v, ok := fields[1].(bool); ok {
					m.UndeliverableHere = v
				}
			}
		}
		return m
	default:
		return nil
	}
}
