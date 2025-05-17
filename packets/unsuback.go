package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// UnSubAck is an internal representation of the fields of the UNSUBACK MQTT packet.
type UnSubAck struct {
	FixedHeader
	Properties *BasicProperties
	ID         uint16
}

func (pkt *UnSubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *UnSubAck) Pack(w io.Writer) error {
	var err error
	pkt.FixedHeader.RemainingLength = 2
	packet := pkt.FixedHeader.encode()
	packet.Write(codec.EncodeUint16(pkt.ID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *UnSubAck) Unpack(b io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *UnSubAck) Details() Details {
	return Details{Type: UnsubAckType, ID: pkt.ID, Qos: 0}
}
