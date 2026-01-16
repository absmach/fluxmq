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
	ret := codec.EncodeUint16(pkt.ID)
	// MQTT 5.0 spec allows minimal formats:
	// - Remaining Length 2: Packet ID only (reason code defaults to 0x00)
	// - Remaining Length 3: Packet ID + Reason Code (no properties)
	// - Remaining Length >= 4: Packet ID + Reason Code + Property Length + Properties
	// If properties are present, reason code MUST be included
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		if len(props) > 0 || pkt.ReasonCode != nil {
			// Include reason code (use 0x00 if nil)
			if pkt.ReasonCode != nil {
				ret = append(ret, *pkt.ReasonCode)
			} else {
				ret = append(ret, 0x00)
			}
			proplen := codec.EncodeVBI(len(props))
			ret = append(ret, proplen...)
			ret = append(ret, props...)
		}
	} else if pkt.ReasonCode != nil && *pkt.ReasonCode != 0x00 {
		// Only include reason code if non-zero (minimal format optimization)
		ret = append(ret, *pkt.ReasonCode)
	}
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *PubRel) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
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
