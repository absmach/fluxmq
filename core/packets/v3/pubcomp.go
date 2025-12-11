package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/codec"
	"github.com/dborovcanin/mqtt/core/packets"
)

// PubComp represents the MQTT V3.1.1 PUBCOMP packet.
type PubComp struct {
	packets.FixedHeader
	ID uint16
}

func (p *PubComp) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\n", p.FixedHeader, p.ID)
}

func (p *PubComp) Type() byte {
	return packets.PubCompType
}

func (p *PubComp) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(p.ID)...)
	p.FixedHeader.RemainingLength = len(body)
	return append(p.FixedHeader.Encode(), body...)
}

func (p *PubComp) Unpack(r io.Reader) error {
	var err error
	p.ID, err = codec.DecodeUint16(r)
	return err
}

func (p *PubComp) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *PubComp) Details() packets.Details {
	return packets.Details{Type: packets.PubCompType, ID: p.ID}
}
