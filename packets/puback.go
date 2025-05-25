package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// PubAck is an internal representation of the fields of the PUBACK MQTT packet.
type PubAck struct {
	FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

func (pkt *PubAck) Encode() []byte {
	ret := pkt.FixedHeader.Encode()
	// Variable Header
	ret = append(ret, codec.EncodeUint16(pkt.ID)...)
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

	return ret
}

func (pkt *PubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *PubAck) Unpack(r io.Reader, v byte) error {
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
		if length == 0 {
			return nil
		}
		if err := p.Unpack(r); err != nil {
			return err
		}
		pkt.Properties = &p
	}

	return nil
}

func (pkt *PubAck) Details() Details {
	return Details{Type: PubAckType, Qos: pkt.QoS}
}
