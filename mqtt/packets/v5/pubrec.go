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

// The List of valid PubRec reason codes.
const (
	PubRecSuccess                     = 0x00
	PubRecNoMatchingSubscribers       = 0x10
	PubRecUnspecifiedError            = 0x80
	PubRecImplementationSpecificError = 0x83
	PubRecNotAuthorized               = 0x87
	PubRecTopicNameInvalid            = 0x90
	PubRecPacketIdentifierInUse       = 0x91
	PubRecQuotaExceeded               = 0x97
	PubRecPayloadFormatInvalid        = 0x99
)

// PubRec is an internal representation of the fields of the PUBREC MQTT packet.
type PubRec struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubRec) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

// Type returns the packet type.
func (pkt *PubRec) Type() byte {
	return PubRecType
}

func (pkt *PubRec) Encode() []byte {
	return pkt.EncodeTo(nil)
}

// EncodeTo appends the encoded packet to dst and returns the extended buffer.
func (pkt *PubRec) EncodeTo(dst []byte) []byte {
	return appendAck(dst, &pkt.FixedHeader, pkt.ID, pkt.ReasonCode, pkt.Properties, ackShortest)
}

func (pkt *PubRec) Pack(w io.Writer) error {
	return packets.WriteEncoded(w, pkt)
}

func (pkt *PubRec) Unpack(r io.Reader) error {
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

func (pkt *PubRec) Details() Details {
	return Details{Type: PubAckType, QoS: pkt.QoS}
}
