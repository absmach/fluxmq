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
	Opts []SubOption
}

func (pkt *SubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *SubAck) Pack(w io.Writer) error {
	bytes := pkt.FixedHeader.Encode()
	// Variable Header
	bytes = append(bytes, codec.EncodeUint16(pkt.ID)...)
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
	}
	// Read subscription options.
	for {
		opt := SubOption{}
		err := opt.Unpack(r, v)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		pkt.Opts = append(pkt.Opts, opt)
	}
}

func (pkt *SubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, Qos: 0}
}
