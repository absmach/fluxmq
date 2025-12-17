// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"fmt"
	"io"

	"github.com/absmach/mqtt/core/codec"
	"github.com/absmach/mqtt/core/packets"
)

// PubAck represents the MQTT V3.1.1 PUBACK packet.
type PubAck struct {
	packets.FixedHeader
	ID uint16
}

func (p *PubAck) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\n", p.FixedHeader, p.ID)
}

func (p *PubAck) Type() byte {
	return packets.PubAckType
}

func (p *PubAck) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(p.ID)...)
	p.FixedHeader.RemainingLength = len(body)
	return append(p.FixedHeader.Encode(), body...)
}

func (p *PubAck) Unpack(r io.Reader) error {
	var err error
	p.ID, err = codec.DecodeUint16(r)
	return err
}

func (p *PubAck) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *PubAck) Details() packets.Details {
	return packets.Details{Type: packets.PubAckType, ID: p.ID}
}
