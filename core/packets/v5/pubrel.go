package v5

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/codec"
	"github.com/dborovcanin/mqtt/core/packets"
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
	if pkt.ReasonCode != nil {
		ret = append(ret, *pkt.ReasonCode)
	}
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		ret = append(ret, proplen...)
		if l > 0 {
			ret = append(ret, props...)
		}
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
	// MQTT 5.0 reason code and properties
	rc, err := codec.DecodeByte(r)
	if err != nil {
		return err
	}
	pkt.ReasonCode = &rc
	length, err := codec.DecodeVBI(r)
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
