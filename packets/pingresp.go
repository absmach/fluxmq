package packets

import (
	"fmt"
	"io"
)

// PingResp is an internal representation of the fields of the PINGRESP MQTT packet.
type PingResp struct {
	FixedHeader
}

func (pr *PingResp) String() string {
	str := fmt.Sprintf("%s", pr.FixedHeader)
	return str
}

func (pr *PingResp) Write(w io.Writer) error {
	packet := pr.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pr *PingResp) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pr *PingResp) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
