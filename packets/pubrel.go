package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// PubRel is an internal representation of the fields of the PUBREL MQTT packet.
type PubRel struct {
	FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubRel) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

func (pkt *PubRel) Pack(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.encode()
	_, err = packet.WriteTo(w)

	return err
}

func (pkt *PubRel) Unpack(r io.Reader, v byte) error {
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

func (pkt *PubRel) Details() Details {
	return Details{Type: PubAckType, Qos: pkt.QoS}
}
