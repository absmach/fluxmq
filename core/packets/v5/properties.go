// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"fmt"
	"io"

	"github.com/absmach/fluxmq/core/codec"
)

// PropPayloadFormat, etc are the list of property codes for the
// MQTT packet properties.
const (
	PayloadFormatProp          byte = 1
	MessageExpiryProp          byte = 2
	ContentTypeProp            byte = 3
	ResponseTopicProp          byte = 8
	CorrelationDataProp        byte = 9
	SubscriptionIdentifierProp byte = 11
	SessionExpiryIntervalProp  byte = 17
	AssignedClientIDProp       byte = 18
	ServerKeepAliveProp        byte = 19
	AuthMethodProp             byte = 21
	AuthDataProp               byte = 22
	RequestProblemInfoProp     byte = 23
	WillDelayIntervalProp      byte = 24
	RequestResponseInfoProp    byte = 25
	ResponseInfoProp           byte = 26
	ServerReferenceProp        byte = 28
	ReasonStringProp           byte = 31
	ReceiveMaximumProp         byte = 33
	TopicAliasMaximumProp      byte = 34
	TopicAliasProp             byte = 35
	MaximumQOSProp             byte = 36
	RetainAvailableProp        byte = 37
	UserProp                   byte = 38
	MaximumPacketSizeProp      byte = 39
	WildcardSubAvailableProp   byte = 40
	SubIDAvailableProp         byte = 41
	SharedSubAvailableProp     byte = 42
)

type BasicProperties struct {
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// User is a slice of user provided properties (key and value).
	User []User
}

func (p *BasicProperties) Unpack(r io.Reader) error {
	for {
		prop, err := codec.DecodeByte(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch prop {
		case ReasonStringProp:
			p.ReasonString, err = codec.DecodeString(r)
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
		default:
			return fmt.Errorf("invalid property type %d", prop)
		}
	}
}

func (p *BasicProperties) Encode() []byte {
	var ret []byte
	if p.ReasonString != "" {
		ret = append(ret, ReasonStringProp)
		ret = append(ret, codec.EncodeBytes([]byte(p.ReasonString))...)
	}
	for _, u := range p.User {
		ret = append(ret, UserProp)
		ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
		ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
	}

	return ret
}
