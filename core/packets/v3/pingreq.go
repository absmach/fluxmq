package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/core/packets"
)

// PingReq represents the MQTT V3.1.1 PINGREQ packet.
type PingReq struct {
	packets.FixedHeader
}

func (p *PingReq) String() string {
	return fmt.Sprintf("%s\n", p.FixedHeader)
}

func (p *PingReq) Type() byte {
	return packets.PingReqType
}

func (p *PingReq) Encode() []byte {
	p.FixedHeader.RemainingLength = 0
	return p.FixedHeader.Encode()
}

func (p *PingReq) Unpack(r io.Reader) error {
	return nil
}

func (p *PingReq) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *PingReq) Details() packets.Details {
	return packets.Details{Type: packets.PingReqType}
}
