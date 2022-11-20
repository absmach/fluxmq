package packets

import (
	"fmt"
	"io"
)

// PubRec is an internal representation of the fields of the PUBREC MQTT packet.
type PubRec struct {
	FixedHeader
	MessageID uint16
}

func (pr *PubRec) String() string {
	return pr.FixedHeader.String() + fmt.Sprintf("message_id: %d", pr.MessageID)
}

func (pr *PubRec) Write(w io.Writer) error {
	var err error
	pr.FixedHeader.RemainingLength = 2
	packet := pr.FixedHeader.pack()
	packet.Write(encodeUint16(pr.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pr *PubRec) Unpack(b io.Reader) error {
	var err error
	pr.MessageID, err = decodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pr *PubRec) Details() Details {
	return Details{Qos: pr.Qos, MessageID: pr.MessageID}
}
