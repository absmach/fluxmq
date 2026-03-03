// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	codec "github.com/absmach/fluxmq/mqtt/codec"
	"github.com/absmach/fluxmq/mqtt/packets"
)

// ErrPublishInvalidLength represents invalid length of PUBLISH packet.
var ErrPublishInvalidLength = errors.New("error unpacking publish, payload length < 0")

// Publish is an internal representation of the fields of the PUBLISH MQTT packet.
type Publish struct {
	packets.FixedHeader
	// Variable Header
	ID         uint16
	TopicName  string
	Properties *PublishProperties
	Payload    []byte
}

type PublishProperties struct {
	// PayloadFormat indicates the format of the payload of the message
	// 0 is unspecified bytes
	// 1 is UTF8 encoded character data
	PayloadFormat *byte
	// MessageExpiry is the lifetime of the message in seconds.
	MessageExpiry *uint32
	// TopicAlias is an identifier of a Topic Alias.
	TopicAlias *uint16
	// ResponseTopic is a UTF8 string indicating the topic name to which any
	// response to this message should be sent.
	ResponseTopic string
	// CorrelationData is binary data used to associate future response
	// messages with the original request message.
	CorrelationData []byte
	// User is a slice of user provided properties (key and value).
	User []User
	// SubscriptionID is an identifier of the subscription to which
	// the Publish matched.
	SubscriptionID *int
	// ContentType is a UTF8 string describing the content of the message
	// for example it could be a MIME type.
	ContentType string
}

func (p *PublishProperties) Unpack(r io.Reader) error {
	for {
		prop, err := codec.DecodeByte(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch prop {
		case PayloadFormatProp:
			pf, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.PayloadFormat = &pf
		case MessageExpiryProp:
			me, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.MessageExpiry = &me
		case ContentTypeProp:
			p.ContentType, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case TopicAliasProp:
			ta, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAlias = &ta
		case ResponseTopicProp:
			p.ResponseTopic, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case CorrelationDataProp:
			p.CorrelationData, err = codec.DecodeBytes(r)
			if err != nil {
				return err
			}
		case UserProp:
			k, err := codec.DecodeString(r)
			if err != nil {
				return err
			}
			v, err := codec.DecodeString(r)
			if err != nil {
				return err
			}
			p.User = append(p.User, User{Key: k, Value: v})
		case SubscriptionIdentifierProp:
			si, err := codec.DecodeVBI(r)
			if err != nil {
				return err
			}
			p.SubscriptionID = &si
		default:
			return fmt.Errorf("invalid property type %d for publish packet", prop)
		}
	}
}

func (p *PublishProperties) Encode() []byte {
	return p.EncodeTo(nil)
}

// EncodeTo appends the encoded properties to dst and returns the extended buffer.
func (p *PublishProperties) EncodeTo(dst []byte) []byte {
	if p.PayloadFormat != nil {
		dst = append(dst, PayloadFormatProp, *p.PayloadFormat)
	}
	if p.MessageExpiry != nil {
		dst = append(dst, MessageExpiryProp)
		dst = appendUint32(dst, *p.MessageExpiry)
	}
	if p.TopicAlias != nil {
		dst = append(dst, TopicAliasProp)
		dst = appendUint16(dst, *p.TopicAlias)
	}
	if p.ResponseTopic != "" {
		dst = append(dst, ResponseTopicProp)
		dst = appendString(dst, p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		dst = append(dst, CorrelationDataProp)
		dst = appendBytes(dst, p.CorrelationData)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			dst = append(dst, UserProp)
			dst = appendString(dst, u.Key)
			dst = appendString(dst, u.Value)
		}
	}
	if p.SubscriptionID != nil {
		dst = append(dst, SubscriptionIdentifierProp)
		dst = appendVBI(dst, *p.SubscriptionID)
	}
	if p.ContentType != "" {
		dst = append(dst, ContentTypeProp)
		dst = appendString(dst, p.ContentType)
	}
	return dst
}

func (p *PublishProperties) encodedLen() int {
	if p == nil {
		return 0
	}

	n := 0
	if p.PayloadFormat != nil {
		n += 2
	}
	if p.MessageExpiry != nil {
		n += 5
	}
	if p.TopicAlias != nil {
		n += 3
	}
	if p.ResponseTopic != "" {
		n += 1 + 2 + len(p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		n += 1 + 2 + len(p.CorrelationData)
	}
	for _, u := range p.User {
		n += 1 + 2 + len(u.Key) + 2 + len(u.Value)
	}
	if p.SubscriptionID != nil {
		n += 1 + vbiLen(*p.SubscriptionID)
	}
	if p.ContentType != "" {
		n += 1 + 2 + len(p.ContentType)
	}
	return n
}

func (pkt *Publish) String() string {
	return fmt.Sprintf("%s\ntopic_name: %s\npacket_id: %d\npayload: %s\n", pkt.FixedHeader, pkt.TopicName, pkt.ID, pkt.Payload)
}

// Type returns the packet type.
func (pkt *Publish) Type() byte {
	return PublishType
}

func (pkt *Publish) Release() {
	ReleasePublish(pkt)
}

func (pkt *Publish) Encode() []byte {
	return pkt.EncodeTo(nil)
}

// EncodeTo appends the encoded packet to dst and returns the extended buffer.
func (pkt *Publish) EncodeTo(dst []byte) []byte {
	propsLen := 0
	if pkt.Properties != nil {
		propsLen = pkt.Properties.encodedLen()
	}

	remainingLen := 2 + len(pkt.TopicName) + len(pkt.Payload)
	if pkt.QoS > 0 {
		remainingLen += 2
	}
	remainingLen += vbiLen(propsLen) + propsLen
	pkt.FixedHeader.RemainingLength = remainingLen

	var dup, retain byte
	if pkt.Dup {
		dup = 1
	}
	if pkt.Retain {
		retain = 1
	}
	dst = append(dst, pkt.PacketType<<4|dup<<3|pkt.QoS<<1|retain)
	dst = appendVBI(dst, remainingLen)

	dst = appendString(dst, pkt.TopicName)
	if pkt.QoS > 0 {
		dst = appendUint16(dst, pkt.ID)
	}
	dst = appendVBI(dst, propsLen)
	if propsLen > 0 {
		dst = pkt.Properties.EncodeTo(dst)
	}
	dst = append(dst, pkt.Payload...)

	return dst
}

func appendString(dst []byte, s string) []byte {
	dst = append(dst, byte(len(s)>>8), byte(len(s)))
	return append(dst, s...)
}

func appendBytes(dst []byte, b []byte) []byte {
	dst = append(dst, byte(len(b)>>8), byte(len(b)))
	return append(dst, b...)
}

func appendUint16(dst []byte, v uint16) []byte {
	return append(dst, byte(v>>8), byte(v))
}

func appendUint32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
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

func vbiLen(num int) int {
	n := 1
	for num >= 128 {
		num /= 128
		n++
	}
	return n
}

func (pkt *Publish) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *Publish) Unpack(r io.Reader) error {
	var err error
	if pkt.TopicName, err = codec.DecodeString(r); err != nil {
		return err
	}
	if pkt.QoS > 0 {
		if pkt.ID, err = codec.DecodeUint16(r); err != nil {
			return err
		}
	}
	length, err := codec.DecodeVBI(r)
	if err != nil {
		return err
	}
	if length != 0 {
		buf := make([]byte, length)
		if _, err := r.Read(buf); err != nil {
			return err
		}
		props := bytes.NewReader(buf)
		p := PublishProperties{}
		if err := p.Unpack(props); err != nil {
			return err
		}
		pkt.Properties = &p
	}
	pkt.Payload, err = io.ReadAll(r)

	return err
}

// Copy creates a new PublishPacket with the same topic and payload
// but an empty fixed header, useful for when you want to deliver
// a message with different properties such as Qos but the same
// content.
func (pkt *Publish) Copy() *Publish {
	newP := NewControlPacket(PublishType).(*Publish)
	newP.TopicName = pkt.TopicName
	newP.Payload = pkt.Payload

	return newP
}

func (pkt *Publish) Details() Details {
	return Details{Type: PublishType, ID: pkt.ID, QoS: pkt.QoS}
}
