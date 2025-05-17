package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// Subscribe is an internal representation of the fields of the SUBSCRIBE MQTT packet
type Subscribe struct {
	FixedHeader
	Properties *SubscribeProperties
	ID         uint16
	Topics     []string
	QoSs       []byte
}

type SubscribeProperties struct {
	// SubscriptionIdentifier is an identifier of the subscription to which
	// the Publish matched.
	SubscriptionIdentifier *int
	// User is a slice of user provided properties (key and value).
	User []User
}

func (p *SubscribeProperties) Unpack(r io.Reader) error {
	length, err := codec.DecodeVBI(r)
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	for {
		prop, err := codec.DecodeByte(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch prop {
		case SubscriptionIdentifierProp:
			si, err := codec.DecodeVBI(r)
			if err != nil {
				return err
			}
			p.SubscriptionIdentifier = &si
		case UserProp:
			k, err := codec.DecodeString(r)
			if err != nil {
				return err
			}
			v, err := codec.DecodeString(r)
			if err != nil {
				return err
			}
			p.User = append(p.User, User{k, v})
		}
	}
}

func (pkt *Subscribe) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\ntopics: %s\n", pkt.FixedHeader, pkt.ID, pkt.Topics)
}

func (pkt *Subscribe) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(codec.EncodeUint16(pkt.ID))
	for i, topic := range pkt.Topics {
		body.Write(codec.EncodeString(topic))
		body.WriteByte(pkt.QoSs[i])
	}
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.encode()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *Subscribe) Unpack(b io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(b)
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
		pkt.QoSs = append(pkt.QoSs, qos)
		payloadLength -= 2 + len(topic) + 1 // 2 bytes of string length, plus string, plus 1 byte for Qos
	}

	return nil
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *Subscribe) Details() Details {
	return Details{Type: SubscribeType, ID: pkt.ID, Qos: 1}
}
