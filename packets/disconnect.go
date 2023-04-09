package packets

import (
	"io"
)

// Disconnect is an internal representation of the fields of the DISCONNECT MQTT packet
type Disconnect struct {
	FixedHeader
}

func (d *Disconnect) String() string {
	return d.FixedHeader.String()
}

func (d *Disconnect) Write(w io.Writer) error {
	packet := d.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (d *Disconnect) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (d *Disconnect) Details() Details {
	return Details{Qos: 0, ID: 0}
}
