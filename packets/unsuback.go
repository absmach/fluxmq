package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// UnSubAck is an internal representation of the fields of the UNSUBACK MQTT packet.

type UnSubAck struct {
	FixedHeader
	// Variable Header
	ID         uint16
	Properties *BasicProperties
	// Payload
	ReturnCodes *[]byte
}

func (pkt *UnSubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *UnSubAck) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(codec.EncodeUint16(pkt.ID))
	if pkt.ReturnCodes != nil {
		body.Write(*pkt.ReturnCodes)
	}
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.encode()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

func (pkt *UnSubAck) Unpack(r io.Reader, v byte) error {
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
		rc, err := codec.DecodeBytes(r)
		if err != nil {
			return err
		}
		pkt.ReturnCodes = &rc
	}

	return nil
}

func (pkt *UnSubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, Qos: 0}
}
