package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/core/codec"
	"github.com/dborovcanin/mqtt/core/packets"
)

// PubRec represents the MQTT V3.1.1 PUBREC packet.
type PubRec struct {
	packets.FixedHeader
	ID uint16
}

func (p *PubRec) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\n", p.FixedHeader, p.ID)
}

func (p *PubRec) Type() byte {
	return packets.PubRecType
}

func (p *PubRec) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(p.ID)...)
	p.FixedHeader.RemainingLength = len(body)
	return append(p.FixedHeader.Encode(), body...)
}

func (p *PubRec) Unpack(r io.Reader) error {
	var err error
	p.ID, err = codec.DecodeUint16(r)
	return err
}

func (p *PubRec) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *PubRec) Details() packets.Details {
	return packets.Details{Type: packets.PubRecType, ID: p.ID}
}
