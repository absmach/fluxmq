package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mbroker/packets/codec"
)

// Auth is an internal representation of the fields of Auth MQTT packet.
type Auth struct {
	FixedHeader
	ReasonCode byte
}

func (pkt *Auth) String() string {
	return fmt.Sprintf("%s \nReason Code %s", pkt.FixedHeader, pkt.ReasonCode)
}

func (pkt *Auth) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.WriteByte(pkt.ReasonCode)
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.pack()
	packet.Write(body.Bytes())
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
// MessageID of this ControlPacket
func (pkt *Auth) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
