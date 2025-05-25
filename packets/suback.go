package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// SubAck is an internal representation of the fields of the SUBACK MQTT packet.
type SubAck struct {
	FixedHeader
	// Variable Header
	ID         uint16
	Properties *BasicProperties
	// Payload
	ReasonCodes []byte
}

func (pkt *SubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *SubAck) Encode() []byte {
	ret := codec.EncodeUint16(pkt.ID)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		ret = append(ret, proplen...)
		if l > 0 {
			ret = append(ret, props...)
		}
	}
	ret = append(ret, pkt.ReasonCodes...)
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *SubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *SubAck) Unpack(r io.Reader, v byte) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	if v == V5 {
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
		pkt.ReasonCodes, err = codec.DecodeBytes(r)
	}
	return err
}

func (pkt *SubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, Qos: 0}
}
