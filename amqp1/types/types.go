// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"time"
)

// AMQP 1.0 type constructor codes per OASIS spec.
const (
	// Fixed-width primitives
	TypeNull       byte = 0x40
	TypeBoolTrue   byte = 0x41
	TypeBoolFalse  byte = 0x42
	TypeBool       byte = 0x56
	TypeUbyte      byte = 0x50
	TypeUshort     byte = 0x60
	TypeUint       byte = 0x70
	TypeUintSmall  byte = 0x52
	TypeUint0      byte = 0x43
	TypeUlong      byte = 0x80
	TypeUlongSmall byte = 0x53
	TypeUlong0     byte = 0x44
	TypeByte       byte = 0x51
	TypeShort      byte = 0x61
	TypeInt        byte = 0x71
	TypeIntSmall   byte = 0x54
	TypeLong       byte = 0x81
	TypeLongSmall  byte = 0x55
	TypeFloat      byte = 0x72
	TypeDouble     byte = 0x82
	TypeTimestamp  byte = 0x83
	TypeUUID       byte = 0x98

	// Variable-width primitives
	TypeBinaryShort byte = 0xa0
	TypeBinaryLong  byte = 0xb0
	TypeStringShort byte = 0xa1
	TypeStringLong  byte = 0xb1
	TypeSymbolShort byte = 0xa3
	TypeSymbolLong  byte = 0xb3

	// Compound types
	TypeList0     byte = 0x45
	TypeList8     byte = 0xc0
	TypeList32    byte = 0xd0
	TypeMap8      byte = 0xc1
	TypeMap32     byte = 0xd1
	TypeArray8    byte = 0xe0
	TypeArray32   byte = 0xf0

	// Described type constructor
	TypeDescriptor byte = 0x00
)

// Symbol is an AMQP symbolic value (ASCII subset of string).
type Symbol string

// UUID is a 128-bit universally unique identifier.
type UUID [16]byte

// Timestamp wraps time.Time for AMQP timestamp encoding (milliseconds since Unix epoch).
type Timestamp time.Time

// Described wraps a described type with its numeric descriptor and value.
type Described struct {
	Descriptor uint64
	Value      any
}

// Milliseconds returns the Unix timestamp in milliseconds.
func (t Timestamp) Milliseconds() int64 {
	return time.Time(t).UnixMilli()
}

// TimestampFromMillis creates a Timestamp from milliseconds since Unix epoch.
func TimestampFromMillis(ms int64) Timestamp {
	return Timestamp(time.UnixMilli(ms))
}
