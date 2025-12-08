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
	ReasonCodes *[]byte
}

func (pkt *UnSubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

// Type returns the packet type.
func (pkt *UnSubAck) Type() byte {
	return UnsubAckType
}

func (pkt *UnSubAck) Encode() []byte {
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
	if pkt.ReasonCodes != nil {
		ret = append(ret, *pkt.ReasonCodes...)
	}
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *UnSubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *UnSubAck) Unpack(r io.Reader) error {
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

func (pkt *UnSubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, QoS: 0}
}
