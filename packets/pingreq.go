package packets

import (
	"io"
)

// PingReq is an internal representation of the fields of the PINGREQ MQTT packet
type PingReq struct {
	FixedHeader
}

func (pkt *PingReq) String() string {
	return pkt.FixedHeader.String()
}

func (pkt *PingReq) Write(w io.Writer) error {
	packet := pkt.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *PingReq) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *PingReq) Details() Details {
	return Details{Qos: 0, ID: 0}
}
