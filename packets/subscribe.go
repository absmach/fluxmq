package packets

import (
	"bytes"
	"fmt"
	"io"
)

// Subscribe is an internal representation of the fields of the SUBSCRIBE MQTT packet
type Subscribe struct {
	FixedHeader
	MessageID uint16
	Topics    []string
	Qoss      []byte
}

func (s *Subscribe) String() string {
	return s.FixedHeader.String() + fmt.Sprintf("message_id: %d topics: %s", s.MessageID, s.Topics)
}

func (s *Subscribe) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(encodeUint16(s.MessageID))
	for i, topic := range s.Topics {
		body.Write(encodeString(topic))
		body.WriteByte(s.Qoss[i])
	}
	s.FixedHeader.RemainingLength = body.Len()
	packet := s.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (s *Subscribe) Unpack(b io.Reader) error {
	var err error
	s.MessageID, err = decodeUint16(b)
	if err != nil {
		return err
	}
	payloadLength := s.FixedHeader.RemainingLength - 2
	for payloadLength > 0 {
		topic, err := decodeString(b)
		if err != nil {
			return err
		}
		s.Topics = append(s.Topics, topic)
		qos, err := decodeByte(b)
		if err != nil {
			return err
		}
		s.Qoss = append(s.Qoss, qos)
		payloadLength -= 2 + len(topic) + 1 //2 bytes of string length, plus string, plus 1 byte for Qos
	}

	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (s *Subscribe) Details() Details {
	return Details{Qos: 1, MessageID: s.MessageID}
}
