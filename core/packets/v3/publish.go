package v3

import (
	"fmt"
	"io"

	"github.com/absmach/mqtt/core/codec"
	"github.com/absmach/mqtt/core/packets"
)

// Publish represents the MQTT V3.1.1 PUBLISH packet.
type Publish struct {
	packets.FixedHeader
	TopicName string
	ID        uint16 // Packet Identifier
	Payload   []byte
}

func (p *Publish) String() string {
	return fmt.Sprintf("%s\nTopic: %s\nPacketID: %d\nPayload: %s\n", p.FixedHeader, p.TopicName, p.ID, string(p.Payload))
}

func (p *Publish) Type() byte {
	return packets.PublishType
}

func (p *Publish) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeString(p.TopicName)...)
	if p.QoS > 0 {
		body = append(body, codec.EncodeUint16(p.ID)...)
	}
	body = append(body, p.Payload...)
	p.FixedHeader.RemainingLength = len(body)
	return append(p.FixedHeader.Encode(), body...)
}

func (p *Publish) Unpack(r io.Reader) error {
	var err error
	p.TopicName, err = codec.DecodeString(r)
	if err != nil {
		return err
	}
	if p.QoS > 0 {
		p.ID, err = codec.DecodeUint16(r)
		if err != nil {
			return err
		}
	}
	p.Payload, err = io.ReadAll(r)
	return err
}

func (p *Publish) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *Publish) Details() packets.Details {
	return packets.Details{Type: packets.PublishType, ID: p.ID, QoS: p.QoS}
}
