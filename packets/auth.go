package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// Auth is an internal representation of the fields of Auth MQTT packet.
type Auth struct {
	FixedHeader
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

func (pkt *Auth) Pack(w io.Writer) error {
	ret := pkt.FixedHeader.Encode()
	ret = append(ret, byte(pkt.ReasonCode))
	if pkt.Properties != nil {
		ret = append(ret, pkt.Encode()...)
	}
	_, err := w.Write(ret)

	return err
}

func (pkt *Auth) Unpack(r io.Reader, v byte) error {
	var err error
	pkt.ReasonCode, err = codec.DecodeByte(r)
	if v == V5 {
		length, err := codec.DecodeVBI(r)
		if err != nil {
			return err
		}
		if length != 0 {
			prop := AuthProperties{}
			if err := prop.Unpack(r); err != nil {
				return err
			}
			pkt.Properties = &prop
		}
	}

	return err
}

func (pkt *Auth) Details() Details {
	return Details{Type: AuthType, ID: 0, Qos: 0}
}
