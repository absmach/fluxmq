// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/absmach/mqtt/core/codec"
	"github.com/absmach/mqtt/core/packets"
)

// The List of valid PubAck reason codes.
const (
	PubAckSuccess                     = 0x00
	PubAckNoMatchingSubscribers       = 0x10
	PubAckUnspecifiedError            = 0x80
	PubAckImplementationSpecificError = 0x83
	PubAckNotAuthorized               = 0x87
	PubAckTopicNameInvalid            = 0x90
	PubAckPacketIdentifierInUse       = 0x91
	PubAckQuotaExceeded               = 0x97
	PubAckPayloadFormatInvalid        = 0x99
)

// PubAck is an internal representation of the fields of the PUBACK MQTT packet.
type PubAck struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

// Type returns the packet type.
func (pkt *PubAck) Type() byte {
	return PubAckType
}

func (pkt *PubAck) Encode() []byte {
	var ret []byte
	// Variable Header
	ret = append(ret, codec.EncodeUint16(pkt.ID)...)
	if pkt.ReasonCode != nil {
		ret = append(ret, *pkt.ReasonCode)
	} else {
		ret = append(ret, 0) // Success
	}
	// Properties (MQTT 5.0)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		ret = append(ret, proplen...)
		if l > 0 {
			ret = append(ret, props...)
		}
	} else {
		ret = append(ret, 0) // Zero-length properties
	}

	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)
	return ret
}

func (pkt *PubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *PubAck) Unpack(r io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	// MQTT 5.0 allows minimal packets with just packet ID (remaining length = 2).
	// In this case, reason code defaults to 0x00 (Success) and no properties.
	rc, err := codec.DecodeByte(r)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	pkt.ReasonCode = &rc
	length, err := codec.DecodeVBI(r)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	p := BasicProperties{}
	props := bytes.NewBuffer(buf)
	if err := p.Unpack(props); err != nil {
		return err
	}
	pkt.Properties = &p

	return nil
}

// Reason returns a string representation of the meaning of the ReasonCode
func (p *PubAck) Reason() string {
	switch *p.ReasonCode {
	case 0:
		return "The message is accepted. Publication of the QoS 1 message proceeds."
	case 16:
		return "The message is accepted but there are no subscribers. This is sent only by the Server. If the Server knows that there are no matching subscribers, it MAY use this Reason Code instead of 0x00 (Success)."
	case 128:
		return "The receiver does not accept the publish but either does not want to reveal the reason, or it does not match one of the other values."
	case 131:
		return "The PUBLISH is valid but the receiver is not willing to accept it."
	case 135:
		return "The PUBLISH is not authorized."
	case 144:
		return "The Topic Name is not malformed, but is not accepted by this Client or Server."
	case 145:
		return "The Packet Identifier is already in use. This might indicate a mismatch in the Session State between the Client and Server."
	case 151:
		return "An implementation or administrative imposed limit has been exceeded."
	case 153:
		return "The payload format does not match the specified Payload Format Indicator."
	}

	return ""
}

func (pkt *PubAck) Details() Details {
	return Details{Type: PubAckType, QoS: pkt.QoS}
}
