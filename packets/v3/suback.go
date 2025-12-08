package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/packets/codec"
)

// SubAck represents the MQTT V3.1.1 SUBACK packet.
type SubAck struct {
	packets.FixedHeader
	ID          uint16
	ReturnCodes []byte
}

func (s *SubAck) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\nReturnCodes: %v\n", s.FixedHeader, s.ID, s.ReturnCodes)
}

func (s *SubAck) Type() byte {
	return packets.SubAckType
}

func (s *SubAck) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(s.ID)...)
	body = append(body, s.ReturnCodes...)
	s.FixedHeader.RemainingLength = len(body)
	return append(s.FixedHeader.Encode(), body...)
}

func (s *SubAck) Unpack(r io.Reader) error {
	var err error
	s.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	s.ReturnCodes, err = io.ReadAll(r)
	return err
}

func (s *SubAck) Pack(w io.Writer) error {
	_, err := w.Write(s.Encode())
	return err
}

func (s *SubAck) Details() packets.Details {
	return packets.Details{Type: packets.SubAckType, ID: s.ID}
}
