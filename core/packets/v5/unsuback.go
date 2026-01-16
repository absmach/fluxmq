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

// The list of valid UnsSubAck reason codes.
const (
	UnsubAckSuccess                     = 0x00
	UnsubAckNoSubscriptionFound         = 0x11
	UnsubAckUnspecifiedError            = 0x80
	UnsubAckImplementationSpecificError = 0x83
	UnsubAckNotAuthorized               = 0x87
	UnsubAckTopicFilterInvalid          = 0x8F
	UnsubAckPacketIdentifierInUse       = 0x91
)

// UnsubAck is an internal representation of the fields of the UNSUBACK MQTT packet.
type UnsubAck struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	Properties *BasicProperties
	// Payload
	ReasonCodes *[]byte
}

func (pkt *UnsubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

// Type returns the packet type.
func (pkt *UnsubAck) Type() byte {
	return packets.UnsubAckType
}

func (pkt *UnsubAck) Encode() []byte {
	ret := codec.EncodeUint16(pkt.ID)
	// MQTT 5.0 spec: UNSUBACK always requires property length
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
	if pkt.ReasonCodes != nil {
		ret = append(ret, *pkt.ReasonCodes...)
	}
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *UnsubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *UnsubAck) Unpack(r io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	// Properties (MQTT 5.0)
	length, err := codec.DecodeVBI(r)
	if err != nil {
		return err
	}
	if length != 0 {
		buf := make([]byte, length)
		if _, err := r.Read(buf); err != nil {
			return err
		}
		p := BasicProperties{}
		props := bytes.NewReader(buf)
		if err := p.Unpack(props); err != nil {
			return err
		}
		pkt.Properties = &p
	}
	// Reason codes (one per topic)
	rc, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	pkt.ReasonCodes = &rc

	return nil
}

func (pkt *UnsubAck) Details() Details {
	return Details{Type: UnsubAckType, ID: pkt.ID, QoS: 0}
}

// Reason returns a string representation of the meaning of the ReasonCode
func (u *UnsubAck) Reason(index int) string {
	if index >= 0 && index < len(*u.ReasonCodes) {
		switch (*u.ReasonCodes)[index] {
		case 0x00:
			return "Success - The subscription is deleted"
		case 0x11:
			return "No subscription found - No matching Topic Filter is being used by the Client."
		case 0x80:
			return "Unspecified error - The unsubscribe could not be completed and the Server either does not wish to reveal the reason or none of the other Reason Codes apply."
		case 0x83:
			return "Implementation specific error - The UNSUBSCRIBE is valid but the Server does not accept it."
		case 0x87:
			return "Not authorized - The Client is not authorized to unsubscribe."
		case 0x8F:
			return "Topic Filter invalid - The Topic Filter is correctly formed but is not allowed for this Client."
		case 0x91:
			return "Packet Identifier in use - The specified Packet Identifier is already in use."
		}
	}
	return "Invalid Reason index"
}
