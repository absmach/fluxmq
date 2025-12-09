package v5

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	codec "github.com/dborovcanin/mqtt/codec"
)

// Auth is an internal representation of the fields of Auth MQTT packet.
type Auth struct {
	packets.FixedHeader
	// Variable Header
	ReasonCode byte
	Properties *AuthProperties
}

type AuthProperties struct {
	// AuthMethod is a UTF8 string containing the name of the authentication
	// method to be used for extended authentication
	AuthMethod string
	// AuthData is binary data containing authentication data
	AuthData []byte
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// User is a slice of user provided properties (key and value)
	User []User
}

func (p *AuthProperties) Encode() []byte {
	var ret []byte
	if p.AuthMethod != "" {
		ret = append(ret, AuthMethodProp)
		ret = append(ret, codec.EncodeBytes([]byte(p.AuthMethod))...)
	}
	if len(p.AuthData) > 0 {
		ret = append(ret, AuthDataProp)
		ret = append(ret, codec.EncodeBytes(p.AuthData)...)
	}
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

func (p *AuthProperties) Unpack(r io.Reader) error {
	for {
		prop, err := codec.DecodeByte(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch prop {
		case AuthMethodProp:
			p.AuthMethod, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case AuthDataProp:
			p.AuthData, err = codec.DecodeBytes(r)
			if err != nil {
				return err
			}
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
			p.User = append(p.User, User{k, v})
		default:
			return fmt.Errorf("invalid property type %d for connect packet", prop)
		}
	}
}

func (pkt *Auth) String() string {
	return fmt.Sprintf("%s\nreason_code %d\n", pkt.FixedHeader, pkt.ReasonCode)
}

// Type returns the packet type.
func (pkt *Auth) Type() byte {
	return AuthType
}

func (pkt *Auth) Encode() []byte {
	var ret []byte
	ret = append(ret, pkt.ReasonCode)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		ret = append(ret, proplen...)
		if l > 0 {
			ret = append(ret, props...)
		}
	} else {
		ret = append(ret, 0) // Zero-length properties
	}
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)
	return ret
}

func (pkt *Auth) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *Auth) Unpack(r io.Reader) error {
	var err error
	pkt.ReasonCode, err = codec.DecodeByte(r)
	if err != nil {
		return err
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
		props := bytes.NewBuffer(buf)
		p := AuthProperties{}
		if err := p.Unpack(props); err != nil {
			return err
		}
		pkt.Properties = &p
	}

	return nil
}

func (pkt *Auth) Details() Details {
	return Details{Type: AuthType, ID: 0, QoS: 0}
}
