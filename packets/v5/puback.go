package v5

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	codec "github.com/dborovcanin/mqtt/codec"
)

// PubAck is an internal representation of the fields of the PUBACK MQTT packet.
type PubAck struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code: %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

// Type returns the packet type.
func (pkt *PubAck) Type() byte {
	return PubAckType
}

func (pkt *PubAck) Encode() []byte {
	var ret []byte
	// Variable Header
	ret = append(ret, codec.EncodeUint16(pkt.ID)...)
	if pkt.ReasonCode != nil {
		ret = append(ret, *pkt.ReasonCode)
	} else {
		ret = append(ret, 0) // Success
	}
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

	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)
	return ret
}

func (pkt *PubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *PubAck) Unpack(r io.Reader) error {
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
	props := bytes.NewBuffer(buf)
	if err := p.Unpack(props); err != nil {
		return err
	}
	pkt.Properties = &p

	return nil
}

func (pkt *PubAck) Details() Details {
	return Details{Type: PubAckType, QoS: pkt.QoS}
}
