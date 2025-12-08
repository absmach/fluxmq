package v5

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// PubComp is an internal representation of the fields of the PUBCOMP MQTT packet.
type PubComp struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	ReasonCode *byte
	Properties *BasicProperties
}

func (pkt *PubComp) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\nreason_code %b", pkt.FixedHeader, pkt.ID, *pkt.ReasonCode)
}

// Type returns the packet type.
func (pkt *PubComp) Type() byte {
	return PubCompType
}

func (pkt *PubComp) Encode() []byte {
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

func (pkt *PubComp) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *PubComp) Unpack(r io.Reader) error {
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

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *PubComp) Details() Details {
	return Details{Type: PubCompType, ID: pkt.ID, QoS: pkt.QoS}
}
