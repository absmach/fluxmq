package packets

import (
	"bytes"
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
	ReasonCodes *[]byte // MQTT 5
	Reason      *byte   // MQTT 3
}

func (pkt *SubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

// Type returns the packet type.
func (pkt *SubAck) Type() byte {
	return SubAckType
}

func (pkt *SubAck) Encode() []byte {
	ret := codec.EncodeUint16(pkt.ID)
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
	// Reason codes (one byte per topic)
	if pkt.ReasonCodes != nil {
		ret = append(ret, *pkt.ReasonCodes...)
	}

	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *SubAck) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *SubAck) Unpack(r io.Reader) error {
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
	// Reason codes (one byte per topic, no length prefix)
	rcs, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	pkt.ReasonCodes = &rcs
	return nil
}

func (pkt *SubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, QoS: 0}
}
