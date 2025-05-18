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
	Properties *PublishProperties
	TopicName  string
	ID         uint16
	Payload    []byte
}

type PublishProperties struct {
	// PayloadFormat indicates the format of the payload of the message
	// 0 is unspecified bytes
	// 1 is UTF8 encoded character data
	PayloadFormat *byte
	// MessageExpiry is the lifetime of the message in seconds.
	MessageExpiry *uint32
	// // TopicAliasMax is the highest value permitted as a Topic Alias.
	TopicAliasMax *uint16
	// ResponseTopic is a UTF8 string indicating the topic name to which any
	// response to this message should be sent.
	ResponseTopic string
	// CorrelationData is binary data used to associate future response
	// messages with the original request message.
	CorrelationData []byte
	// User is a slice of user provided properties (key and value).
	User []User
	// SubscriptionIdentifier is an identifier of the subscription to which
	// the Publish matched.
	SubscriptionIdentifier *int
	// ContentType is a UTF8 string describing the content of the message
	// for example it could be a MIME type.
	ContentType string
}

func (p *PublishProperties) Unpack(r io.Reader) error {
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
		case PayloadFormatProp:
			pf, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.PayloadFormat = &pf
		case MessageExpiryProp:
			me, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.MessageExpiry = &me
		case ContentTypeProp:
			p.ContentType, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case TopicAliasMaximumProp:
			tam, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAliasMax = &tam
		case ResponseTopicProp:
			p.ResponseTopic, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case CorrelationDataProp:
			p.CorrelationData, err = codec.DecodeBytes(r)
			if err != nil {
				return err
			}
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
		case SubscriptionIdentifierProp:
			si, err := codec.DecodeVBI(r)
			if err != nil {
				return err
			}
			p.SubscriptionIdentifier = &si
		}
	}
}

func (pkt *Publish) String() string {
	return fmt.Sprintf("%s\ntopic_name: %s\npacket_id: %d\npayload: %s\n", pkt.FixedHeader, pkt.TopicName, pkt.ID, pkt.Payload)
}

func (pkt *Publish) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(codec.EncodeBytes([]byte(pkt.TopicName)))
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
