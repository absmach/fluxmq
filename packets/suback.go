package packets

import (
	"bytes"
	"fmt"
	"io"
)

// SubAck is an internal representation of the fields of the SUBACK MQTT packet.
type SubAck struct {
	FixedHeader
	MessageID   uint16
	ReturnCodes []byte
}

func (sa *SubAck) String() string {
	return sa.FixedHeader.String() + fmt.Sprintf("message_id: %d", sa.MessageID)
}

func (sa *SubAck) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(encodeUint16(sa.MessageID))
	body.Write(sa.ReturnCodes)
	sa.FixedHeader.RemainingLength = body.Len()
	packet := sa.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (sa *SubAck) Unpack(b io.Reader) error {
	var qosBuffer bytes.Buffer
	var err error
	sa.MessageID, err = decodeUint16(b)
	if err != nil {
		return err
	}

	_, err = qosBuffer.ReadFrom(b)
	if err != nil {
		return err
	}
	sa.ReturnCodes = qosBuffer.Bytes()

	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (sa *SubAck) Details() Details {
	return Details{Qos: 0, MessageID: sa.MessageID}
}
