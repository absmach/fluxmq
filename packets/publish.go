package packets

import (
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
	// Variable Header
	ID         uint16
	TopicName  string
	Properties *PublishProperties
	Payload    []byte
}

type PublishProperties struct {
	// PayloadFormat indicates the format of the payload of the message
	// 0 is unspecified bytes
	// 1 is UTF8 encoded character data
	PayloadFormat *byte
	// MessageExpiry is the lifetime of the message in seconds.
	MessageExpiry *uint32
	// TopicAlias is an identifier of a Topic Alias.
	TopicAlias *uint16
	// ResponseTopic is a UTF8 string indicating the topic name to which any
	// response to this message should be sent.
	ResponseTopic string
	// CorrelationData is binary data used to associate future response
	// messages with the original request message.
	CorrelationData []byte
	// User is a slice of user provided properties (key and value).
	User []User
	// SubscriptionID is an identifier of the subscription to which
	// the Publish matched.
	SubscriptionID *int
	// ContentType is a UTF8 string describing the content of the message
	// for example it could be a MIME type.
	ContentType string
}

func (p *PublishProperties) Unpack(r io.Reader) error {
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
			ta, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAlias = &ta
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
			p.SubscriptionID = &si
		default:
			return fmt.Errorf("invalid property type %d for publish packet", prop)
		}
	}
}

func (p *PublishProperties) Encode() []byte {
	var ret []byte
	if p.PayloadFormat != nil {
		ret = append(ret, *p.PayloadFormat)
	}
	if p.MessageExpiry != nil {
		ret = append(ret, codec.EncodeUint32(*p.MessageExpiry)...)
	}
	if p.TopicAlias != nil {
		ret = append(ret, codec.EncodeUint16(*p.TopicAlias)...)
	}
	if p.ResponseTopic != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.ResponseTopic))...)
	}
	if len(p.CorrelationData) > 0 {
		ret = append(ret, p.CorrelationData...)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
			ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
		}
	}
	if p.SubscriptionID != nil {
		ret = append(ret, codec.EncodeVBI(*p.SubscriptionID)...)
	}
	if p.ContentType != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.ContentType))...)
	}

	return ret
}

func (pkt *Publish) String() string {
	return fmt.Sprintf("%s\ntopic_name: %s\npacket_id: %d\npayload: %s\n", pkt.FixedHeader, pkt.TopicName, pkt.ID, pkt.Payload)
}

func (pkt *Publish) Pack(w io.Writer) error {
	bytes := codec.EncodeBytes([]byte(pkt.TopicName))
	if pkt.QoS > 0 {
		bytes = append(bytes, codec.EncodeUint16(pkt.ID)...)
	}
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		bytes = append(bytes, proplen...)
		if l > 0 {
			bytes = append(bytes, props...)
		}
	}
	pkt.FixedHeader.RemainingLength = len(bytes) + len(pkt.Payload)
	bytes = append(bytes, pkt.Payload...)
	bytes = append(pkt.FixedHeader.Encode(), bytes...)
	_, err := w.Write(bytes)

	return err
}

func (pkt *Publish) Unpack(r io.Reader, v byte) error {
	var err error
	if pkt.TopicName, err = codec.DecodeString(r); err != nil {
		return err
	}
	if pkt.QoS > 0 {
		if pkt.ID, err = codec.DecodeUint16(r); err != nil {
			return err
		}
	}
	if v == V5 {
		length, err := codec.DecodeVBI(r)
		if err != nil {
			return err
		}
		if length != 0 {
			p := PublishProperties{}
			if err := p.Unpack(r); err != nil {
				return err
			}
			pkt.Properties = &p
		}
	}
	pkt.Payload, err = io.ReadAll(r)

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

func (pkt *Publish) Details() Details {
	return Details{Type: PublishType, ID: pkt.ID, Qos: pkt.QoS}
}
