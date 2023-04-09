package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mbroker/packets/codec"
)

// UnSubAck is an internal representation of the fields of the UNSUBACK MQTT packet.
type UnSubAck struct {
	FixedHeader
	MessageID uint16
}

func (pkt *UnSubAck) String() string {
	return fmt.Sprintf("%s\nmessage_id: %d\n", pkt.FixedHeader, pkt.MessageID)
}

func (pkt *UnSubAck) Write(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.pack()
	packet.Write(codec.EncodeUint16(pkt.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *UnSubAck) Unpack(b io.Reader) error {
	var err error
	pkt.MessageID, err = codec.DecodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pkt *UnSubAck) Details() Details {
	return Details{Qos: 0, MessageID: pkt.MessageID}
}
