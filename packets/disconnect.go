package packets

import (
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// Disconnect is an internal representation of the fields of the DISCONNECT MQTT packet
type Disconnect struct {
	FixedHeader
	Properties *DisconnectProperties
}

type DisconnectProperties struct {
	// SessionExpiryInterval is the time in seconds after a client disconnects
	// that the server should retain the session information (subscriptions etc).
	SessionExpiryInterval *uint32
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// User is a slice of user provided properties (key and value).
	User []User
	// ServerReference is a UTF8 string indicating another server the client
	// can use.
	ServerReference string
}

func (p *DisconnectProperties) Unpack(r io.Reader) error {
	length, err := codec.DecodeVBI(r)
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	for {
		prop, err := codec.DecodeByte(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch prop {
		case SessionExpiryIntervalProp:
			sei, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.SessionExpiryInterval = &sei
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
		case ServerReferenceProp:
			p.ServerReference, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		}
	}
}

func (p *DisconnectProperties) decode() []byte {
	var ret []byte
	if p.SessionExpiryInterval != nil {
		ret = append(ret, codec.EncodeUint32(*p.SessionExpiryInterval)...)
	}
	if p.ReasonString != "" {
		ret = append(ret, codec.EncodeString(p.ReasonString)...)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeString(u.Key)...)
			ret = append(ret, codec.EncodeString(u.Value)...)
		}
	}
	if p.ServerReference != "" {
		ret = append(ret, codec.EncodeString(p.ServerReference)...)
	}

	return ret
}

func (d *Disconnect) String() string {
	return d.FixedHeader.String()
}

func (d *Disconnect) Pack(w io.Writer) error {
	packet := d.FixedHeader.encode()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (d *Disconnect) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (d *Disconnect) Details() Details {
	return Details{Type: DisconnectType, ID: 0, Qos: 0}
}
