package packets

import (
	"bytes"
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
		}
	}
}

func (p *UnsubscribeProperties) encode() []byte {
	var ret []byte
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
			ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
		}
	}

	return ret
}

func (pkt *Unsubscribe) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *Unsubscribe) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(codec.EncodeUint16(pkt.ID))
	for _, topic := range pkt.Topics {
		body.Write(codec.EncodeBytes([]byte(topic)))
	}
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.encode()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

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
