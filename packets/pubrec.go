package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// PubRec is an internal representation of the fields of the PUBREC MQTT packet.
type PubRec struct {
	FixedHeader
	ID uint16
}

func (pkt *PubRec) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *PubRec) Pack(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.encode()
	packet.Write(codec.EncodeUint16(pkt.ID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *PubRec) Unpack(b io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *PubRec) Details() Details {
	return Details{Qos: pkt.Qos, ID: pkt.ID}
}
