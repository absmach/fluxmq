// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	codec "github.com/absmach/fluxmq/mqtt/codec"
	"github.com/absmach/fluxmq/mqtt/packets"
)

// PubRel is an internal representation of the fields of the PUBREL MQTT packet.
type PubRel struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubRel) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

// Type returns the packet type.
func (pkt *PubRel) Type() byte {
	return PubRelType
}

func (pkt *PubRel) Encode() []byte {
	return pkt.EncodeTo(nil)
}

// EncodeTo appends the encoded packet to dst and returns the extended buffer.
func (pkt *PubRel) EncodeTo(dst []byte) []byte {
	return appendAck(dst, &pkt.FixedHeader, pkt.ID, pkt.ReasonCode, pkt.Properties, ackShortest)
}

func (pkt *PubRel) Pack(w io.Writer) error {
	return packets.WriteEncoded(w, pkt)
}

func (pkt *PubRel) Unpack(r io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	// MQTT 5.0 allows minimal packets with just packet ID (remaining length = 2).
	// In this case, reason code defaults to 0x00 (Success) and no properties.
	rc, err := codec.DecodeByte(r)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	pkt.ReasonCode = &rc
	length, err := codec.DecodeVBI(r)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	p := BasicProperties{}
	props := bytes.NewReader(buf)
	if err := p.Unpack(props); err != nil {
		return err
	}
	pkt.Properties = &p

	return nil
}

func (pkt *PubRel) Details() Details {
	return Details{Type: PubAckType, QoS: pkt.QoS}
}
