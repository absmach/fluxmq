package packets

import (
	"fmt"
	"io"
)

// PubComp is an internal representation of the fields of the PUBCOMP MQTT packet.
type PubComp struct {
	FixedHeader
	MessageID uint16
}

func (pc *PubComp) String() string {
	return pc.FixedHeader.String() + fmt.Sprintf("message_id: %d\n", pc.MessageID)
}

func (pc *PubComp) Write(w io.Writer) error {
	var err error
	pc.FixedHeader.RemainingLength = 2
	packet := pc.FixedHeader.pack()
	packet.Write(encodeUint16(pc.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pc *PubComp) Unpack(b io.Reader) error {
	var err error
	pc.MessageID, err = decodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pc *PubComp) Details() Details {
	return Details{Qos: pc.Qos, MessageID: pc.MessageID}
}
