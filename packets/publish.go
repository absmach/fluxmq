package packets

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// ErrPublishInvalidLength represents invalid length of PUBLISH packet.
var ErrPublishInvalidLength = errors.New("error unpacking publish, payload length < 0")

// Publish is an internal representation of the fields of the PUBLISH MQTT packet.
type Publish struct {
	FixedHeader
	TopicName string
	ID        uint16
	Payload   []byte
}

func (pkt *Publish) String() string {
	return fmt.Sprintf("%s\ntopic_name: %s\npacket_id: %d\npayload: %s\n", pkt.FixedHeader, pkt.TopicName, pkt.ID, pkt.Payload)
}

func (pkt *Publish) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(codec.EncodeString(pkt.TopicName))
	if pkt.Qos > 0 {
		body.Write(codec.EncodeUint16(pkt.ID))
	}
	pkt.FixedHeader.RemainingLength = body.Len() + len(pkt.Payload)
	packet := pkt.FixedHeader.encode()
	packet.Write(body.Bytes())
	packet.Write(pkt.Payload)
	_, err = w.Write(packet.Bytes())

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *Publish) Unpack(b io.Reader) error {
	payloadLength := pkt.FixedHeader.RemainingLength
	var err error
	pkt.TopicName, err = codec.DecodeString(b)
	if err != nil {
		return err
	}

	if pkt.Qos > 0 {
		pkt.ID, err = codec.DecodeUint16(b)
		if err != nil {
			return err
		}
		payloadLength -= len(pkt.TopicName) + 4
	} else {
		payloadLength -= len(pkt.TopicName) + 2
	}
	if payloadLength < 0 {
		return ErrPublishInvalidLength
	}
	pkt.Payload = make([]byte, payloadLength)
	_, err = b.Read(pkt.Payload)

	return err
}

// Copy creates a new PublishPacket with the same topic and payload
// but an empty fixed header, useful for when you want to deliver
// a message with different properties such as Qos but the same
// content
func (pkt *Publish) Copy() *Publish {
	newP := NewControlPacket(PublishType).(*Publish)
	newP.TopicName = pkt.TopicName
	newP.Payload = pkt.Payload

	return newP
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *Publish) Details() Details {
	return Details{Type: PublishType, ID: pkt.ID, Qos: pkt.Qos}
}
