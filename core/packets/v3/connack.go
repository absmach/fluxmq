// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"fmt"
	"io"

	"github.com/absmach/mqtt/core/codec"
	"github.com/absmach/mqtt/core/packets"
)

// ConnAck represents the MQTT V3.1.1 CONNACK packet.
type ConnAck struct {
	packets.FixedHeader
	SessionPresent bool
	ReturnCode     byte
}

func (c *ConnAck) String() string {
	return fmt.Sprintf("%s\nSessionPresent: %t\nReturnCode: %d\n", c.FixedHeader, c.SessionPresent, c.ReturnCode)
}

func (c *ConnAck) Type() byte {
	return packets.ConnAckType
}

func (c *ConnAck) Encode() []byte {
	var body []byte
	var flags byte
	if c.SessionPresent {
		flags |= 0x01
	}
	body = append(body, flags)
	body = append(body, c.ReturnCode)

	c.FixedHeader.RemainingLength = len(body)
	return append(c.FixedHeader.Encode(), body...)
}

func (c *ConnAck) Unpack(r io.Reader) error {
	flags, err := codec.DecodeByte(r)
	if err != nil {
		return err
	}
	c.SessionPresent = (flags & 0x01) > 0

	c.ReturnCode, err = codec.DecodeByte(r)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConnAck) Pack(w io.Writer) error {
	_, err := w.Write(c.Encode())
	return err
}

func (c *ConnAck) Details() packets.Details {
	return packets.Details{Type: packets.ConnAckType}
}
