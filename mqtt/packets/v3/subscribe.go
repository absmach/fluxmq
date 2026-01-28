// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"fmt"
	"io"

	"github.com/absmach/fluxmq/mqtt/codec"
	"github.com/absmach/fluxmq/mqtt/packets"
)

// Topic represents a topic subscription.
type Topic struct {
	Name string
	QoS  byte
}

// Subscribe represents the MQTT V3.1.1 SUBSCRIBE packet.
type Subscribe struct {
	packets.FixedHeader
	ID     uint16
	Topics []Topic
}

func (s *Subscribe) String() string {
	return fmt.Sprintf("%s\nPacketID: %d\nTopics: %v\n", s.FixedHeader, s.ID, s.Topics)
}

func (s *Subscribe) Type() byte {
	return packets.SubscribeType
}

func (s *Subscribe) Encode() []byte {
	var body []byte
	body = append(body, codec.EncodeUint16(s.ID)...)
	for _, t := range s.Topics {
		body = append(body, codec.EncodeString(t.Name)...)
		body = append(body, t.QoS)
	}
	s.FixedHeader.RemainingLength = len(body)
	return append(s.FixedHeader.Encode(), body...)
}

func (s *Subscribe) Unpack(r io.Reader) error {
	var err error
	s.ID, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}

	for {
		tName, err := codec.DecodeString(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		qos, err := codec.DecodeByte(r)
		if err != nil {
			return err
		}
		s.Topics = append(s.Topics, Topic{Name: tName, QoS: qos})
	}
	return nil
}

func (s *Subscribe) Pack(w io.Writer) error {
	_, err := w.Write(s.Encode())
	return err
}

func (s *Subscribe) Details() packets.Details {
	return packets.Details{Type: packets.SubscribeType, ID: s.ID, QoS: 1}
}
