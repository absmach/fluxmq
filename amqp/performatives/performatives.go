// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"
	"fmt"

	"github.com/absmach/fluxmq/amqp/types"
)

// Performative descriptors.
const (
	DescriptorOpen        uint64 = 0x10
	DescriptorBegin       uint64 = 0x11
	DescriptorAttach      uint64 = 0x12
	DescriptorFlow        uint64 = 0x13
	DescriptorTransfer    uint64 = 0x14
	DescriptorDisposition uint64 = 0x15
	DescriptorDetach      uint64 = 0x16
	DescriptorEnd         uint64 = 0x17
	DescriptorClose       uint64 = 0x18
)

// Role constants.
const (
	RoleSender   = false
	RoleReceiver = true
)

// Open performative (0x10).
type Open struct {
	ContainerID  string
	Hostname     string
	MaxFrameSize uint32
	ChannelMax   uint16
	IdleTimeOut  uint32 // milliseconds, 0 = no timeout
	Properties   map[types.Symbol]any
}

func (o *Open) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if err := types.WriteString(&fields, o.ContainerID); err != nil {
		return nil, err
	}
	count++

	if o.Hostname != "" {
		if err := types.WriteString(&fields, o.Hostname); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	maxFrame := o.MaxFrameSize
	if maxFrame == 0 {
		maxFrame = uint32(DefaultMaxFrameSize)
	}
	if err := types.WriteUint(&fields, maxFrame); err != nil {
		return nil, err
	}
	count++

	channelMax := o.ChannelMax
	if channelMax == 0 {
		channelMax = 65535
	}
	if err := types.WriteUshort(&fields, channelMax); err != nil {
		return nil, err
	}
	count++

	if o.IdleTimeOut > 0 {
		if err := types.WriteUint(&fields, o.IdleTimeOut); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if len(o.Properties) > 0 {
		if err := writeSymbolAnyMap(&fields, o.Properties); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	return encodePerformative(DescriptorOpen, fields.Bytes(), count)
}

func DecodeOpen(fields []any) *Open {
	o := &Open{}
	if len(fields) > 0 && fields[0] != nil {
		o.ContainerID, _ = fields[0].(string)
	}
	if len(fields) > 1 && fields[1] != nil {
		o.Hostname, _ = fields[1].(string)
	}
	if len(fields) > 2 && fields[2] != nil {
		o.MaxFrameSize = toUint32(fields[2])
	}
	if len(fields) > 3 && fields[3] != nil {
		o.ChannelMax = uint16(toUint32(fields[3]))
	}
	if len(fields) > 4 && fields[4] != nil {
		o.IdleTimeOut = toUint32(fields[4])
	}
	return o
}

// DefaultMaxFrameSize used when encoding Open with zero MaxFrameSize.
const DefaultMaxFrameSize = 65536

// Begin performative (0x11).
type Begin struct {
	RemoteChannel  *uint16
	NextOutgoingID uint32
	IncomingWindow uint32
	OutgoingWindow uint32
	HandleMax      uint32
}

func (b *Begin) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if b.RemoteChannel != nil {
		if err := types.WriteUshort(&fields, *b.RemoteChannel); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if err := types.WriteUint(&fields, b.NextOutgoingID); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteUint(&fields, b.IncomingWindow); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteUint(&fields, b.OutgoingWindow); err != nil {
		return nil, err
	}
	count++

	handleMax := b.HandleMax
	if handleMax == 0 {
		handleMax = 4294967295
	}
	if err := types.WriteUint(&fields, handleMax); err != nil {
		return nil, err
	}
	count++

	return encodePerformative(DescriptorBegin, fields.Bytes(), count)
}

func DecodeBegin(fields []any) *Begin {
	b := &Begin{}
	if len(fields) > 0 && fields[0] != nil {
		v := uint16(toUint32(fields[0]))
		b.RemoteChannel = &v
	}
	if len(fields) > 1 && fields[1] != nil {
		b.NextOutgoingID = toUint32(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		b.IncomingWindow = toUint32(fields[2])
	}
	if len(fields) > 3 && fields[3] != nil {
		b.OutgoingWindow = toUint32(fields[3])
	}
	if len(fields) > 4 && fields[4] != nil {
		b.HandleMax = toUint32(fields[4])
	}
	return b
}

// Attach performative (0x12).
type Attach struct {
	Name                 string
	Handle               uint32
	Role                 bool // false=sender, true=receiver
	SndSettleMode        *uint8
	RcvSettleMode        *uint8
	Source               *Source
	Target               *Target
	InitialDeliveryCount uint32
	MaxMessageSize       uint64
}

func (a *Attach) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	// Name (0)
	if err := types.WriteString(&fields, a.Name); err != nil {
		return nil, err
	}
	count++

	// Handle (1)
	if err := types.WriteUint(&fields, a.Handle); err != nil {
		return nil, err
	}
	count++

	// Role (2)
	if err := types.WriteBool(&fields, a.Role); err != nil {
		return nil, err
	}
	count++

	// SndSettleMode (3)
	if a.SndSettleMode != nil {
		if err := types.WriteUbyte(&fields, *a.SndSettleMode); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	// RcvSettleMode (4)
	if a.RcvSettleMode != nil {
		if err := types.WriteUbyte(&fields, *a.RcvSettleMode); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	// Source (5)
	if a.Source != nil {
		src, err := a.Source.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(src)
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	// Target (6)
	if a.Target != nil {
		tgt, err := a.Target.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(tgt)
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	// Unsettled (7) - null
	if err := types.WriteNull(&fields); err != nil {
		return nil, err
	}
	count++

	// IncompleteUnsettled (8) - null
	if err := types.WriteNull(&fields); err != nil {
		return nil, err
	}
	count++

	// InitialDeliveryCount (9) - required for sender role
	if !a.Role { // sender
		if err := types.WriteUint(&fields, a.InitialDeliveryCount); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	// MaxMessageSize (10)
	if a.MaxMessageSize > 0 {
		if err := types.WriteUlong(&fields, a.MaxMessageSize); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	return encodePerformative(DescriptorAttach, fields.Bytes(), count)
}

func DecodeAttach(fields []any) *Attach {
	a := &Attach{}
	if len(fields) > 0 && fields[0] != nil {
		a.Name, _ = fields[0].(string)
	}
	if len(fields) > 1 && fields[1] != nil {
		a.Handle = toUint32(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		a.Role = toBool(fields[2])
	}
	if len(fields) > 3 && fields[3] != nil {
		v := uint8(toUint32(fields[3]))
		a.SndSettleMode = &v
	}
	if len(fields) > 4 && fields[4] != nil {
		v := uint8(toUint32(fields[4]))
		a.RcvSettleMode = &v
	}
	if len(fields) > 5 && fields[5] != nil {
		if desc, ok := fields[5].(*types.Described); ok && desc.Descriptor == DescriptorSource {
			if srcFields, ok := desc.Value.([]any); ok {
				a.Source = DecodeSource(srcFields)
			}
		}
	}
	if len(fields) > 6 && fields[6] != nil {
		if desc, ok := fields[6].(*types.Described); ok && desc.Descriptor == DescriptorTarget {
			if tgtFields, ok := desc.Value.([]any); ok {
				a.Target = DecodeTarget(tgtFields)
			}
		}
	}
	if len(fields) > 9 && fields[9] != nil {
		a.InitialDeliveryCount = toUint32(fields[9])
	}
	if len(fields) > 10 && fields[10] != nil {
		a.MaxMessageSize = toUint64(fields[10])
	}
	return a
}

// Flow performative (0x13).
type Flow struct {
	NextIncomingID *uint32
	IncomingWindow uint32
	NextOutgoingID uint32
	OutgoingWindow uint32
	Handle         *uint32
	DeliveryCount  *uint32
	LinkCredit     *uint32
	Available      *uint32
	Drain          bool
	Echo           bool
}

func (f *Flow) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	writeOptUint32 := func(v *uint32) error {
		if v != nil {
			return types.WriteUint(&fields, *v)
		}
		return types.WriteNull(&fields)
	}

	if err := writeOptUint32(f.NextIncomingID); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteUint(&fields, f.IncomingWindow); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteUint(&fields, f.NextOutgoingID); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteUint(&fields, f.OutgoingWindow); err != nil {
		return nil, err
	}
	count++

	if err := writeOptUint32(f.Handle); err != nil {
		return nil, err
	}
	count++

	if err := writeOptUint32(f.DeliveryCount); err != nil {
		return nil, err
	}
	count++

	if err := writeOptUint32(f.LinkCredit); err != nil {
		return nil, err
	}
	count++

	if err := writeOptUint32(f.Available); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteBool(&fields, f.Drain); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteBool(&fields, f.Echo); err != nil {
		return nil, err
	}
	count++

	return encodePerformative(DescriptorFlow, fields.Bytes(), count)
}

func DecodeFlow(fields []any) *Flow {
	f := &Flow{}
	if len(fields) > 0 && fields[0] != nil {
		v := toUint32(fields[0])
		f.NextIncomingID = &v
	}
	if len(fields) > 1 && fields[1] != nil {
		f.IncomingWindow = toUint32(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		f.NextOutgoingID = toUint32(fields[2])
	}
	if len(fields) > 3 && fields[3] != nil {
		f.OutgoingWindow = toUint32(fields[3])
	}
	if len(fields) > 4 && fields[4] != nil {
		v := toUint32(fields[4])
		f.Handle = &v
	}
	if len(fields) > 5 && fields[5] != nil {
		v := toUint32(fields[5])
		f.DeliveryCount = &v
	}
	if len(fields) > 6 && fields[6] != nil {
		v := toUint32(fields[6])
		f.LinkCredit = &v
	}
	if len(fields) > 7 && fields[7] != nil {
		v := toUint32(fields[7])
		f.Available = &v
	}
	if len(fields) > 8 && fields[8] != nil {
		f.Drain = toBool(fields[8])
	}
	if len(fields) > 9 && fields[9] != nil {
		f.Echo = toBool(fields[9])
	}
	return f
}

// Transfer performative (0x14).
type Transfer struct {
	Handle        uint32
	DeliveryID    *uint32
	DeliveryTag   []byte
	MessageFormat *uint32
	Settled       bool
	More          bool
	RcvSettleMode *uint8
	State         any // outcome type
	Payload       []byte
}

func (t *Transfer) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if err := types.WriteUint(&fields, t.Handle); err != nil {
		return nil, err
	}
	count++

	if t.DeliveryID != nil {
		if err := types.WriteUint(&fields, *t.DeliveryID); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if t.DeliveryTag != nil {
		if err := types.WriteBinary(&fields, t.DeliveryTag); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if t.MessageFormat != nil {
		if err := types.WriteUint(&fields, *t.MessageFormat); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if err := types.WriteBool(&fields, t.Settled); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteBool(&fields, t.More); err != nil {
		return nil, err
	}
	count++

	return encodePerformative(DescriptorTransfer, fields.Bytes(), count)
}

func DecodeTransfer(fields []any) *Transfer {
	t := &Transfer{}
	if len(fields) > 0 && fields[0] != nil {
		t.Handle = toUint32(fields[0])
	}
	if len(fields) > 1 && fields[1] != nil {
		v := toUint32(fields[1])
		t.DeliveryID = &v
	}
	if len(fields) > 2 && fields[2] != nil {
		t.DeliveryTag, _ = fields[2].([]byte)
	}
	if len(fields) > 3 && fields[3] != nil {
		v := toUint32(fields[3])
		t.MessageFormat = &v
	}
	if len(fields) > 4 && fields[4] != nil {
		t.Settled = toBool(fields[4])
	}
	if len(fields) > 5 && fields[5] != nil {
		t.More = toBool(fields[5])
	}
	if len(fields) > 6 && fields[6] != nil {
		v := uint8(toUint32(fields[6]))
		t.RcvSettleMode = &v
	}
	return t
}

// Disposition performative (0x15).
type Disposition struct {
	Role      bool // true=receiver, false=sender
	First     uint32
	Last      *uint32
	Settled   bool
	State     any // outcome
	Batchable bool
}

func (d *Disposition) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if err := types.WriteBool(&fields, d.Role); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteUint(&fields, d.First); err != nil {
		return nil, err
	}
	count++

	if d.Last != nil {
		if err := types.WriteUint(&fields, *d.Last); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if err := types.WriteBool(&fields, d.Settled); err != nil {
		return nil, err
	}
	count++

	if d.State != nil {
		stateBytes, err := encodeOutcome(d.State)
		if err != nil {
			return nil, err
		}
		fields.Write(stateBytes)
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	if err := types.WriteBool(&fields, d.Batchable); err != nil {
		return nil, err
	}
	count++

	return encodePerformative(DescriptorDisposition, fields.Bytes(), count)
}

func DecodeDisposition(fields []any) *Disposition {
	d := &Disposition{}
	if len(fields) > 0 && fields[0] != nil {
		d.Role = toBool(fields[0])
	}
	if len(fields) > 1 && fields[1] != nil {
		d.First = toUint32(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		v := toUint32(fields[2])
		d.Last = &v
	}
	if len(fields) > 3 && fields[3] != nil {
		d.Settled = toBool(fields[3])
	}
	if len(fields) > 4 && fields[4] != nil {
		if desc, ok := fields[4].(*types.Described); ok {
			d.State = DecodeOutcome(desc)
		}
	}
	if len(fields) > 5 && fields[5] != nil {
		d.Batchable = toBool(fields[5])
	}
	return d
}

// Detach performative (0x16).
type Detach struct {
	Handle uint32
	Closed bool
	Error  *Error
}

func (d *Detach) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if err := types.WriteUint(&fields, d.Handle); err != nil {
		return nil, err
	}
	count++

	if err := types.WriteBool(&fields, d.Closed); err != nil {
		return nil, err
	}
	count++

	if d.Error != nil {
		errBytes, err := d.Error.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(errBytes)
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	count++

	return encodePerformative(DescriptorDetach, fields.Bytes(), count)
}

func DecodeDetach(fields []any) *Detach {
	d := &Detach{}
	if len(fields) > 0 && fields[0] != nil {
		d.Handle = toUint32(fields[0])
	}
	if len(fields) > 1 && fields[1] != nil {
		d.Closed = toBool(fields[1])
	}
	if len(fields) > 2 && fields[2] != nil {
		if desc, ok := fields[2].(*types.Described); ok && desc.Descriptor == DescriptorError {
			if errFields, ok := desc.Value.([]any); ok {
				d.Error = DecodeError(errFields)
			}
		}
	}
	return d
}

// End performative (0x17).
type End struct {
	Error *Error
}

func (e *End) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if e.Error != nil {
		errBytes, err := e.Error.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(errBytes)
		count++
	}

	return encodePerformative(DescriptorEnd, fields.Bytes(), count)
}

func DecodeEnd(fields []any) *End {
	e := &End{}
	if len(fields) > 0 && fields[0] != nil {
		if desc, ok := fields[0].(*types.Described); ok && desc.Descriptor == DescriptorError {
			if errFields, ok := desc.Value.([]any); ok {
				e.Error = DecodeError(errFields)
			}
		}
	}
	return e
}

// Close performative (0x18).
type Close struct {
	Error *Error
}

func (c *Close) Encode() ([]byte, error) {
	var fields bytes.Buffer
	count := 0

	if c.Error != nil {
		errBytes, err := c.Error.Encode()
		if err != nil {
			return nil, err
		}
		fields.Write(errBytes)
		count++
	}

	return encodePerformative(DescriptorClose, fields.Bytes(), count)
}

func DecodeClose(fields []any) *Close {
	c := &Close{}
	if len(fields) > 0 && fields[0] != nil {
		if desc, ok := fields[0].(*types.Described); ok && desc.Descriptor == DescriptorError {
			if errFields, ok := desc.Value.([]any); ok {
				c.Error = DecodeError(errFields)
			}
		}
	}
	return c
}

// DecodePerformative decodes a performative from a frame body.
func DecodePerformative(body []byte) (uint64, any, error) {
	r := bytes.NewReader(body)
	descriptor, fields, err := types.ReadListFields(r)
	if err != nil {
		return 0, nil, err
	}

	switch descriptor {
	case DescriptorOpen:
		return descriptor, DecodeOpen(fields), nil
	case DescriptorBegin:
		return descriptor, DecodeBegin(fields), nil
	case DescriptorAttach:
		return descriptor, DecodeAttach(fields), nil
	case DescriptorFlow:
		return descriptor, DecodeFlow(fields), nil
	case DescriptorTransfer:
		return descriptor, DecodeTransfer(fields), nil
	case DescriptorDisposition:
		return descriptor, DecodeDisposition(fields), nil
	case DescriptorDetach:
		return descriptor, DecodeDetach(fields), nil
	case DescriptorEnd:
		return descriptor, DecodeEnd(fields), nil
	case DescriptorClose:
		return descriptor, DecodeClose(fields), nil
	default:
		return descriptor, nil, fmt.Errorf("unknown performative descriptor: 0x%02x", descriptor)
	}
}

func encodePerformative(descriptor uint64, fields []byte, count int) ([]byte, error) {
	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, descriptor); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields, count); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeOutcome(state any) ([]byte, error) {
	switch s := state.(type) {
	case *Accepted:
		return s.Encode()
	case *Rejected:
		return s.Encode()
	case *Released:
		return s.Encode()
	case *Modified:
		return s.Encode()
	default:
		return nil, fmt.Errorf("unknown outcome type: %T", state)
	}
}
