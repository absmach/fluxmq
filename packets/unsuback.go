package packets

import (
	"fmt"
	"io"
)

// UnSubAck is an internal representation of the fields of the UNSUBACK MQTT packet.
type UnSubAck struct {
	FixedHeader
	MessageID uint16
}

func (ua *UnSubAck) String() string {
	return ua.FixedHeader.String() + fmt.Sprintf("message_id: %d", ua.MessageID)
}

func (ua *UnSubAck) Write(w io.Writer) error {
	var err error
	ua.FixedHeader.RemainingLength = 2
	packet := ua.FixedHeader.pack()
	packet.Write(encodeUint16(ua.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (ua *UnSubAck) Unpack(b io.Reader) error {
	var err error
	ua.MessageID, err = decodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (ua *UnSubAck) Details() Details {
	return Details{Qos: 0, MessageID: ua.MessageID}
}
