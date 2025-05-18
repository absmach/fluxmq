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
	Properties *UnsubscribeProperties
	ID         uint16
	Topics     []string
}

type UnsubscribeProperties struct {
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// User is a slice of user provided properties (key and value).
	User []User
}

func (p *UnsubscribeProperties) Unpack(r io.Reader) error {
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

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *Unsubscribe) Unpack(b io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(b)
	if err != nil {
		return err
	}

	for topic, err := codec.DecodeString(b); err == nil && topic != ""; topic, err = codec.DecodeString(b) {
		pkt.Topics = append(pkt.Topics, topic)
	}

	return err
}

// Details returns a struct containing the Qos and packet_id of this control packet.
func (pkt *Unsubscribe) Details() Details {
	return Details{Type: UnsubscribeType, ID: pkt.ID, Qos: 1}
}
