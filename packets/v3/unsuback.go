package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/codec"
)

// UnSubAck represents the MQTT V3.1.1 UNSUBACK packet.
type UnSubAck struct {
	packets.FixedHeader
	ID uint16
}

func (u *UnSubAck) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\n", u.FixedHeader, u.ID)
}

func (u *UnSubAck) Type() byte {
	return packets.UnsubAckType
}

func (u *UnSubAck) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(u.ID)...)
	u.FixedHeader.RemainingLength = len(body)
	return append(u.FixedHeader.Encode(), body...)
}

func (u *UnSubAck) Unpack(r io.Reader) error {
	var err error
	u.ID, err = codec.DecodeUint16(r)
	return err
}

func (u *UnSubAck) Pack(w io.Writer) error {
	_, err := w.Write(u.Encode())
	return err
}

func (u *UnSubAck) Details() packets.Details {
	return packets.Details{Type: packets.UnsubAckType, ID: u.ID}
}
