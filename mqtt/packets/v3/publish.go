// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"fmt"
	"io"

	"github.com/absmach/fluxmq/mqtt/codec"
	"github.com/absmach/fluxmq/mqtt/packets"
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

func (p *Publish) Release() {
	ReleasePublish(p)
}

func (p *Publish) Encode() []byte {
	return p.EncodeTo(nil)
}

// EncodeTo appends the encoded packet to dst and returns the extended buffer.
func (p *Publish) EncodeTo(dst []byte) []byte {
	remainingLen := 2 + len(p.TopicName) + len(p.Payload)
	if p.QoS > 0 {
		remainingLen += 2
	}
	p.FixedHeader.RemainingLength = remainingLen

	var dup, retain byte
	if p.Dup {
		dup = 1
	}
	if p.Retain {
		retain = 1
	}
	dst = append(dst, p.PacketType<<4|dup<<3|p.QoS<<1|retain)
	dst = appendVBI(dst, remainingLen)

	topicLen := len(p.TopicName)
	dst = append(dst, byte(topicLen>>8), byte(topicLen))
	dst = append(dst, p.TopicName...)

	if p.QoS > 0 {
		dst = append(dst, byte(p.ID>>8), byte(p.ID))
	}
	dst = append(dst, p.Payload...)

	return dst
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

func appendVBI(dst []byte, num int) []byte {
	v := uint32(num)
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v > 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if v == 0 {
			return dst
		}
	}
}
