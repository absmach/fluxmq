package packets

import (
	"io"
)

// PingResp is an internal representation of the fields of the PINGRESP MQTT packet.
type PingResp struct {
	FixedHeader
}

func (pkt *PingResp) String() string {
	return pkt.FixedHeader.String()
}

func (pkt *PingResp) Pack(w io.Writer) error {
	packet := pkt.FixedHeader.encode()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *PingResp) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *PingResp) Details() Details {
	return Details{Type: PingRespType, ID: 0, Qos: 0}
}
