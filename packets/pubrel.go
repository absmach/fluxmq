package packets

import (
	"fmt"
	"io"
)

// PubRel is an internal representation of the fields of the PUBREL MQTT packet.
type PubRel struct {
	FixedHeader
	MessageID uint16
}

func (pr *PubRel) String() string {
	return pr.FixedHeader.String() + fmt.Sprintf("message_id: %d", pr.MessageID)
}

func (pr *PubRel) Write(w io.Writer) error {
	var err error
	pr.FixedHeader.RemainingLength = 2
	packet := pr.FixedHeader.pack()
	packet.Write(encodeUint16(pr.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pr *PubRel) Unpack(b io.Reader) error {
	var err error
	pr.MessageID, err = decodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pr *PubRel) Details() Details {
	return Details{Qos: pr.Qos, MessageID: pr.MessageID}
}
