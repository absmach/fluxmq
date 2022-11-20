package packets

import (
	"fmt"
	"io"
)

// PubAck is an internal representation of the fields of the PUBACK MQTT packet.
type PubAck struct {
	FixedHeader
	MessageID uint16
}

func (pa *PubAck) String() string {
	str := fmt.Sprintf("%s", pa.FixedHeader)
	str += " "
	str += fmt.Sprintf("message_id: %d", pa.MessageID)
	return str
}

func (pa *PubAck) Write(w io.Writer) error {
	var err error
	pa.FixedHeader.RemainingLength = 2
	packet := pa.FixedHeader.pack()
	packet.Write(encodeUint16(pa.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pa *PubAck) Unpack(b io.Reader) error {
	var err error
	pa.MessageID, err = decodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pa *PubAck) Details() Details {
	return Details{Qos: pa.Qos, MessageID: pa.MessageID}
}
