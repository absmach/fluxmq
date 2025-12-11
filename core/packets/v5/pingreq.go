package v5

import (
	"io"

	"github.com/dborovcanin/mqtt/core/packets"
)

// PingReq is an internal representation of the fields of the PINGREQ MQTT packet
type PingReq struct {
	packets.FixedHeader
}

func (pkt *PingReq) String() string {
	return pkt.FixedHeader.String()
}

// Type returns the packet type.
func (pkt *PingReq) Type() byte {
	return PingReqType
}

func (pkt *PingReq) Encode() []byte {
	return pkt.FixedHeader.Encode()
}

func (pkt *PingReq) Pack(w io.Writer) error {
	// No need for an extra function call of pkt.Encode().
	res := pkt.FixedHeader.Encode()
	_, err := w.Write(res)

	return err
}

func (pkt *PingReq) Unpack(r io.Reader) error {
	return nil
}

func (pkt *PingReq) Details() Details {
	return Details{Type: PingReqType, ID: 0, QoS: 0}
}
