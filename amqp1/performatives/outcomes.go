// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"fmt"

	"github.com/absmach/fluxmq/amqp1/types"
	"github.com/absmach/fluxmq/internal/bufpool"
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
	buf := bufpool.Get()
	defer bufpool.Put(buf)
	if err := types.WriteDescriptor(buf, DescriptorAccepted); err != nil {
		return nil, err
	}
	if err := types.WriteList(buf, nil, 0); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// Rejected outcome with optional error.
type Rejected struct {
	Error *Error
}

func (r *Rejected) Encode() ([]byte, error) {
	fields := bufpool.Get()
	defer bufpool.Put(fields)
	if r.Error != nil {
		errBytes, err := r.Error.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(errBytes)
	} else {
		if err := types.WriteNull(fields); err != nil {
			return nil, err
		}
	}

	buf := bufpool.Get()
	defer bufpool.Put(buf)
	if err := types.WriteDescriptor(buf, DescriptorRejected); err != nil {
		return nil, err
	}
	if err := types.WriteList(buf, fields.Bytes(), 1); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// Released outcome.
type Released struct{}

func (r *Released) Encode() ([]byte, error) {
	buf := bufpool.Get()
	defer bufpool.Put(buf)
	if err := types.WriteDescriptor(buf, DescriptorReleased); err != nil {
		return nil, err
	}
	if err := types.WriteList(buf, nil, 0); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// Modified outcome.
type Modified struct {
	DeliveryFailed    bool
	UndeliverableHere bool
}

func (m *Modified) Encode() ([]byte, error) {
	fields := bufpool.Get()
	defer bufpool.Put(fields)
	if err := types.WriteBool(fields, m.DeliveryFailed); err != nil {
		return nil, err
	}
	if err := types.WriteBool(fields, m.UndeliverableHere); err != nil {
		return nil, err
	}

	buf := bufpool.Get()
	defer bufpool.Put(buf)
	if err := types.WriteDescriptor(buf, DescriptorModified); err != nil {
		return nil, err
	}
	if err := types.WriteList(buf, fields.Bytes(), 2); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// DecodeOutcome decodes a disposition state from a described type. A malformed
// error inside a rejected outcome is reported rather than partially decoded.
func DecodeOutcome(desc *types.Described) (any, error) {
	switch desc.Descriptor {
	case DescriptorAccepted:
		return &Accepted{}, nil
	case DescriptorRejected:
		r := &Rejected{}
		if desc.Value != nil {
			fields, ok := desc.Value.([]any)
			if !ok {
				return nil, fmt.Errorf("%w: rejected body is %T, want a list", ErrMalformedError, desc.Value)
			}
			if len(fields) > 0 {
				decoded, err := decodeErrorField(fields[0])
				if err != nil {
					return nil, err
				}
				r.Error = decoded
			}
		}
		return r, nil
	case DescriptorReleased:
		return &Released{}, nil
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
		return m, nil
	default:
		return nil, nil
	}
}
