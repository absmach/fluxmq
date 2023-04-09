package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mbroker/packets/codec"
)

// PubAck is an internal representation of the fields of the PUBACK MQTT packet.
type PubAck struct {
	FixedHeader
	MessageID uint16
}

func (pkt *PubAck) String() string {
	return fmt.Sprintf("%s\nmessage_id: %d", pkt.FixedHeader, pkt.MessageID)
}

func (pkt *PubAck) Write(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.pack()
	packet.Write(codec.EncodeUint16(pkt.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *PubAck) Unpack(b io.Reader) error {
	var err error
	pkt.MessageID, err = codec.DecodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pkt *PubAck) Details() Details {
	return Details{Qos: pkt.Qos, MessageID: pkt.MessageID}
}
