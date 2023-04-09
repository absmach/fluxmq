package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mbroker/packets/codec"
)

// PubRel is an internal representation of the fields of the PUBREL MQTT packet.
type PubRel struct {
	FixedHeader
	MessageID uint16
}

func (pkt *PubRel) String() string {
	return fmt.Sprintf("%s\nmessage_id: %d\n", pkt.FixedHeader, pkt.MessageID)
}

func (pkt *PubRel) Write(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.pack()
	packet.Write(codec.EncodeUint16(pkt.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *PubRel) Unpack(b io.Reader) error {
	var err error
	pkt.MessageID, err = codec.DecodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pkt *PubRel) Details() Details {
	return Details{Qos: pkt.Qos, MessageID: pkt.MessageID}
}
