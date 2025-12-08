package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/packets/codec"
)

// Unsubscribe represents the MQTT V3.1.1 UNSUBSCRIBE packet.
type Unsubscribe struct {
	packets.FixedHeader
	ID     uint16
	Topics []string
}

func (u *Unsubscribe) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\nTopics: %v\n", u.FixedHeader, u.ID, u.Topics)
}

func (u *Unsubscribe) Type() byte {
	return packets.UnsubscribeType
}

func (u *Unsubscribe) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(u.ID)...)
	for _, t := range u.Topics {
		body = append(body, codec.EncodeString(t)...)
	}
	u.FixedHeader.RemainingLength = len(body)
	return append(u.FixedHeader.Encode(), body...)
}

func (u *Unsubscribe) Unpack(r io.Reader) error {
	var err error
	u.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	for {
		t, err := codec.DecodeString(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		u.Topics = append(u.Topics, t)
	}
	return nil
}

func (u *Unsubscribe) Pack(w io.Writer) error {
	_, err := w.Write(u.Encode())
	return err
}

func (u *Unsubscribe) Details() packets.Details {
	return packets.Details{Type: packets.UnsubscribeType, ID: u.ID, QoS: 1}
}
