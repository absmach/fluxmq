package v5

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/codec"
	"github.com/dborovcanin/mqtt/core/packets"
)

// Subscribe is an internal representation of the fields of the SUBSCRIBE MQTT packet
type Subscribe struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	Properties *SubscribeProperties
	Opts       []SubOption
}

// SubOption represent a subscription options. For more information, check spec:
// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169
// Fields in the struct are ordered for memory alignment.
//
//	Topic
//	MaxQoS
//	NoLocal
//	RetainAsPublished
//	RetainHandling
type SubOption struct {
	Topic             string
	RetainHandling    *byte
	NoLocal           *bool
	RetainAsPublished *bool
	MaxQoS            byte
}

func (s *SubOption) Encode() []byte {
	var flag byte
	flag |= s.MaxQoS & 0x03
	if s.NoLocal != nil {
		flag |= 1 << 2
	}
	if s.RetainAsPublished != nil {
		flag |= 1 << 3
	}
	if s.RetainHandling != nil {
		flag |= (*s.RetainHandling & 0x03) << 4
	}
	return append(codec.EncodeBytes([]byte(s.Topic)), flag)
}

func (s *SubOption) Unpack(r io.Reader) error {
	topic, err := codec.DecodeString(r)
	if err != nil {
		return err
	}
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	s.Topic = topic
	flags := b[0]
	s.MaxQoS = flags & 0x03
	// MQTT 5.0 subscription options
	noLocal := (flags & (1 << 2)) != 0
	retainAsPublished := (flags & (1 << 3)) != 0
	rh := (flags >> 4) & 0x03
	s.NoLocal = &noLocal
	s.RetainAsPublished = &retainAsPublished
	s.RetainHandling = &rh

	return nil
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
		default:
			return fmt.Errorf("invalid property type %d for subscribe packet", prop)
		}
	}
}

func (p *SubscribeProperties) Encode() []byte {
	var ret []byte
	if p.SubscriptionIdentifier != nil {
		ret = append(ret, SubscriptionIdentifierProp)
		ret = append(ret, codec.EncodeVBI(*p.SubscriptionIdentifier)...)
	}
	for _, u := range p.User {
		ret = append(ret, UserProp)
		ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
		ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
	}
	return ret
}

func (pkt *Subscribe) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

// Type returns the packet type.
func (pkt *Subscribe) Type() byte {
	return SubscribeType
}

func (pkt *Subscribe) Encode() []byte {
	ret := codec.EncodeUint16(pkt.ID)
	// Properties (MQTT 5.0)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		ret = append(ret, proplen...)
		if l > 0 {
			ret = append(ret, props...)
		}
	} else {
		ret = append(ret, 0) // Zero-length properties
	}
	// Payload
	for _, opt := range pkt.Opts {
		ret = append(ret, opt.Encode()...)
	}
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *Subscribe) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *Subscribe) Unpack(r io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	// Properties (MQTT 5.0)
	length, err := codec.DecodeVBI(r)
	if err != nil {
		return err
	}
	if length != 0 {
		buf := make([]byte, length)
		if _, err := r.Read(buf); err != nil {
			return err
		}
		p := SubscribeProperties{}
		props := bytes.NewReader(buf)
		if err := p.Unpack(props); err != nil {
			return err
		}
		pkt.Properties = &p
	}
	// Read subscription options.
	for {
		opt := SubOption{}
		err := opt.Unpack(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		pkt.Opts = append(pkt.Opts, opt)
	}
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (pkt *Subscribe) Details() Details {
	return Details{Type: SubscribeType, ID: pkt.ID, QoS: 1}
}
