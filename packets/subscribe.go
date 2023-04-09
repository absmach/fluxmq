package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mbroker/packets/codec"
)

// Subscribe is an internal representation of the fields of the SUBSCRIBE MQTT packet
type Subscribe struct {
	FixedHeader
	MessageID uint16
	Topics    []string
	Qoss      []byte
}

func (pkt *Subscribe) String() string {
	return fmt.Sprintf("%s\nmessage_id: %d\ntopics: %s\n", pkt.FixedHeader, pkt.MessageID, pkt.Topics)
}

func (pkt *Subscribe) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(codec.EncodeUint16(pkt.MessageID))
	for i, topic := range pkt.Topics {
		body.Write(codec.EncodeString(topic))
		body.WriteByte(pkt.Qoss[i])
	}
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *Subscribe) Unpack(b io.Reader) error {
	var err error
	pkt.MessageID, err = codec.DecodeUint16(b)
	if err != nil {
		return err
	}
	payloadLength := pkt.FixedHeader.RemainingLength - 2
	for payloadLength > 0 {
		topic, err := codec.DecodeString(b)
		if err != nil {
			return err
		}
		pkt.Topics = append(pkt.Topics, topic)
		qos, err := codec.DecodeByte(b)
		if err != nil {
			return err
		}
		pkt.Qoss = append(pkt.Qoss, qos)
		payloadLength -= 2 + len(topic) + 1 //2 bytes of string length, plus string, plus 1 byte for Qos
	}

	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pkt *Subscribe) Details() Details {
	return Details{Qos: 1, MessageID: pkt.MessageID}
}
