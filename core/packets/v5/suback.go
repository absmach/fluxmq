// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/absmach/fluxmq/core/codec"
	"github.com/absmach/fluxmq/core/packets"
)

// The list of valid SubAck reason codes.
const (
	SubAckGrantedQoS0                         = 0x00
	SubAckGrantedQoS1                         = 0x01
	SubAckGrantedQoS2                         = 0x02
	SubAckUnspecifiedError                    = 0x80
	SubAckImplementationSpecificError         = 0x83
	SubAckNotAuthorized                       = 0x87
	SubAckTopicFilterInvalid                  = 0x8F
	SubAckPacketIdentifierInUse               = 0x91
	SubAckQuotaExceeded                       = 0x97
	SubAckSharedSubscriptionNotSupported      = 0x9E
	SubAckSubscriptionIdentifiersNotSupported = 0xA1
	SubAckWildcardSubscriptionsNotSupported   = 0xA2
)

// SubAck is an internal representation of the fields of the SUBACK MQTT packet.
type SubAck struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	Properties *BasicProperties
	// Payload
	ReasonCodes *[]byte // MQTT 5
}

func (pkt *SubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

// Type returns the packet type.
func (pkt *SubAck) Type() byte {
	return SubAckType
}

func (pkt *SubAck) Encode() []byte {
	ret := codec.EncodeUint16(pkt.ID)
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
	// Reason codes (one byte per topic)
	if pkt.ReasonCodes != nil {
		ret = append(ret, *pkt.ReasonCodes...)
	}

	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *SubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *SubAck) Unpack(r io.Reader) error {
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
	// Reason codes (one byte per topic, no length prefix)
	rcs, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	pkt.ReasonCodes = &rcs
	return nil
}

// Reason returns a string representation of the meaning of the ReasonCode.
func (s *SubAck) Reason(index int) string {
	if index >= 0 && index < len(*s.ReasonCodes) {
		switch (*s.ReasonCodes)[index] {
		case SubAckGrantedQoS0:
			return "Granted QoS 0 - The subscription is accepted and the maximum QoS sent will be QoS 0. This might be a lower QoS than was requested."
		case SubAckGrantedQoS1:
			return "Granted QoS 1 - The subscription is accepted and the maximum QoS sent will be QoS 1. This might be a lower QoS than was requested."
		case SubAckGrantedQoS2:
			return "Granted QoS 2 - The subscription is accepted and any received QoS will be sent to this subscription."
		case SubAckUnspecifiedError:
			return "Unspecified error - The subscription is not accepted and the Server either does not wish to reveal the reason or none of the other Reason Codes apply."
		case SubAckImplementationSpecificError:
			return "Implementation specific error - The SUBSCRIBE is valid but the Server does not accept it."
		case SubAckNotAuthorized:
			return "Not authorized - The Client is not authorized to make this subscription."
		case SubAckTopicFilterInvalid:
			return "Topic Filter invalid - The Topic Filter is correctly formed but is not allowed for this Client."
		case SubAckPacketIdentifierInUse:
			return "Packet Identifier in use - The specified Packet Identifier is already in use."
		case SubAckQuotaExceeded:
			return "Quota exceeded - An implementation or administrative imposed limit has been exceeded."
		case SubAckSharedSubscriptionNotSupported:
			return "Shared Subscription not supported - The Server does not support Shared Subscriptions for this Client."
		case SubAckSubscriptionIdentifiersNotSupported:
			return "Subscription Identifiers not supported - The Server does not support Subscription Identifiers; the subscription is not accepted."
		case SubAckWildcardSubscriptionsNotSupported:
			return "Wildcard subscriptions not supported - The Server does not support Wildcard subscription; the subscription is not accepted."
		}
	}
	return "Invalid Reason index"
}

func (pkt *SubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, QoS: 0}
}
