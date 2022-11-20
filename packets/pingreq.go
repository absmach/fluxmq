package packets

import (
	"fmt"
	"io"
)

// PingReq is an internal representation of the fields of the PINGREQ MQTT packet
type PingReq struct {
	FixedHeader
}

func (pr *PingReq) String() string {
	str := fmt.Sprintf("%s", pr.FixedHeader)
	return str
}

func (pr *PingReq) Write(w io.Writer) error {
	packet := pr.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pr *PingReq) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pr *PingReq) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
