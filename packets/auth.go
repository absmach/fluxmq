package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// Auth is an internal representation of the fields of Auth MQTT packet.
type Auth struct {
	FixedHeader
	ReasonCode byte
}

func (pkt *Auth) String() string {
	return fmt.Sprintf("%s\nreason_code %d\n", pkt.FixedHeader, pkt.ReasonCode)
}

func (pkt *Auth) Write(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.encode()
	packet.Write([]byte{pkt.ReasonCode})
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *Auth) Unpack(b io.Reader) error {
	var err error
	pkt.ReasonCode, err = codec.DecodeByte(b)
	return err
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *Auth) Details() Details {
	return Details{Type: AuthType, ID: 0, Qos: 0}
}
