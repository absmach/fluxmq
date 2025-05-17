package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// SubAck is an internal representation of the fields of the SUBACK MQTT packet.
type SubAck struct {
	FixedHeader
	ID          uint16
	ReturnCodes []byte
}

func (pkt *SubAck) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *SubAck) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(codec.EncodeUint16(pkt.ID))
	body.Write(pkt.ReturnCodes)
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.encode()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *SubAck) Unpack(b io.Reader) error {
	var qosBuffer bytes.Buffer
	var err error
	pkt.ID, err = codec.DecodeUint16(b)
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
// ID of this ControlPacket
func (pkt *SubAck) Details() Details {
	return Details{Type: SubAckType, ID: pkt.ID, Qos: 0}
}
