// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"fmt"
	"io"

	"github.com/absmach/fluxmq/mqtt/codec"
	"github.com/absmach/fluxmq/mqtt/packets"
)

// PubComp represents the MQTT V3.1.1 PUBCOMP packet.
type PubComp struct {
	packets.FixedHeader
	ID uint16
}

func (p *PubComp) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\n", p.FixedHeader, p.ID)
}

func (p *PubComp) Type() byte {
	return packets.PubCompType
}

func (p *PubComp) Encode() []byte {
	return p.EncodeTo(nil)
}

// EncodeTo appends the encoded packet to dst and returns the extended buffer.
func (p *PubComp) EncodeTo(dst []byte) []byte {
	p.FixedHeader.RemainingLength = 2
	dst = p.FixedHeader.AppendTo(dst)
	return append(dst, byte(p.ID>>8), byte(p.ID))
}

func (p *PubComp) Unpack(r io.Reader) error {
	var err error
	p.ID, err = codec.DecodeUint16(r)
	return err
}

func (p *PubComp) Pack(w io.Writer) error {
	return packets.WriteEncoded(w, p)
}

func (p *PubComp) Details() packets.Details {
	return packets.Details{Type: packets.PubCompType, ID: p.ID}
}
