package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/codec"
)

// PubRel represents the MQTT V3.1.1 PUBREL packet.
type PubRel struct {
	packets.FixedHeader
	ID uint16
}

func (p *PubRel) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\n", p.FixedHeader, p.ID)
}

func (p *PubRel) Type() byte {
	return packets.PubRelType
}

func (p *PubRel) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(p.ID)...)
	p.FixedHeader.RemainingLength = len(body)
	return append(p.FixedHeader.Encode(), body...)
}

func (p *PubRel) Unpack(r io.Reader) error {
	var err error
	p.ID, err = codec.DecodeUint16(r)
	return err
}

func (p *PubRel) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *PubRel) Details() packets.Details {
	return packets.Details{Type: packets.PubRelType, ID: p.ID, QoS: 1}
}
