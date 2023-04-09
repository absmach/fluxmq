package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mbroker/packets/codec"
)

// SubAck is an internal representation of the fields of the SUBACK MQTT packet.
type SubAck struct {
	FixedHeader
	MessageID   uint16
	ReturnCodes []byte
}

func (pkt *SubAck) String() string {
	return fmt.Sprintf("%s\nmessage_id: %d\n", pkt.FixedHeader, pkt.MessageID)
}

func (pkt *SubAck) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(codec.EncodeUint16(pkt.MessageID))
	body.Write(pkt.ReturnCodes)
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *SubAck) Unpack(b io.Reader) error {
	var qosBuffer bytes.Buffer
	var err error
	pkt.MessageID, err = codec.DecodeUint16(b)
	if err != nil {
		return err
	}

	_, err = qosBuffer.ReadFrom(b)
	if err != nil {
		return err
	}
	pkt.ReturnCodes = qosBuffer.Bytes()

	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pkt *SubAck) Details() Details {
	return Details{Qos: 0, MessageID: pkt.MessageID}
}
