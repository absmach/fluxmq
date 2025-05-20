package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// Unsubscribe is an internal representation of the fields of the UNSUBSCRIBE MQTT packet.
type Unsubscribe struct {
	FixedHeader
	// Variable Header
	ID         uint16
	Properties *UnsubscribeProperties
	// Payload
	Topics []string
}

type UnsubscribeProperties struct {
	// User is a slice of user provided properties (key and value).
	User []User
}

func (p *UnsubscribeProperties) Unpack(r io.Reader) error {
	for {
		prop, err := codec.DecodeByte(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch prop {
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
			return fmt.Errorf("invalid property type %d for unsubscribe packet", prop)
		}
	}
}

func (p *UnsubscribeProperties) Encode() []byte {
	var ret []byte
	for _, u := range p.User {
		ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
		ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
	}

	return ret
}

func (pkt *Unsubscribe) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *Unsubscribe) Pack(w io.Writer) error {
	bytes := codec.EncodeUint16(pkt.ID)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		bytes = append(bytes, proplen...)
		if l > 0 {
			bytes = append(bytes, props...)
		}
	}
	for _, t := range pkt.Topics {
		bytes = append(bytes, codec.EncodeBytes([]byte(t))...)
	}
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(bytes)
	bytes = append(pkt.FixedHeader.Encode(), bytes...)
	_, err := w.Write(bytes)

	return err
}

func (pkt *Unsubscribe) Unpack(r io.Reader, v byte) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	if v == V5 {
		length, err := codec.DecodeVBI(r)
		if err != nil {
			return err
		}
		if length != 0 {
			p := UnsubscribeProperties{}
			if err := p.Unpack(r); err != nil {
				return err
			}
			pkt.Properties = &p
		}
	}
	for {
		t, err := codec.DecodeBytes(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		pkt.Topics = append(pkt.Topics, string(t))
	}
}

func (pkt *Unsubscribe) Details() Details {
	return Details{Type: UnsubscribeType, ID: pkt.ID, Qos: 1}
}
