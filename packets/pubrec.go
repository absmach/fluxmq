package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// PubRec is an internal representation of the fields of the PUBREC MQTT packet.
type PubRec struct {
	FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubRec) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

func (pkt *PubRec) Pack(w io.Writer) error {
	bytes := pkt.FixedHeader.Encode()
	// Variable Header
	bytes = append(bytes, codec.EncodeUint16(pkt.ID)...)
	if pkt.ReasonCode != nil {
		bytes = append(bytes, *pkt.ReasonCode)
	}
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		bytes = append(bytes, proplen...)
		if l > 0 {
			bytes = append(bytes, props...)
		}
	}
	_, err := w.Write(bytes)

	return err
}

func (pkt *PubRec) Unpack(r io.Reader, v byte) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	if v == V5 {
		rc, err := codec.DecodeByte(r)
		if err != nil {
			return err
		}
		pkt.ReasonCode = &rc
		p := BasicProperties{}
		length, err := codec.DecodeVBI(r)
		if err != nil {
			return err
		}
		if length != 0 {
			if err := p.Unpack(r); err != nil {
				return err
			}
			pkt.Properties = &p
		}
	}

	return nil
}

func (pkt *PubRec) Details() Details {
	return Details{Type: PubAckType, Qos: pkt.QoS}
}
