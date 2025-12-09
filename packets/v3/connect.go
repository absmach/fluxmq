package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/codec"
)

// Connect represents the MQTT V3.1.1 CONNECT packet.
type Connect struct {
	packets.FixedHeader
	ProtocolName    string
	ProtocolVersion byte
	UsernameFlag    bool
	PasswordFlag    bool
	WillRetain      bool
	WillQoS         byte
	WillFlag        bool
	CleanSession    bool // Replaces CleanStart in V5
	ReservedBit     byte
	KeepAlive       uint16

	ClientID    string
	WillTopic   string
	WillMessage []byte
	Username    string
	Password    []byte
}

func (c *Connect) String() string {
	return fmt.Sprintf("%s\nProtocol: %s %d\nClientID: %s\nCleanSession: %t\nKeepAlive: %d\nUsername: %s\nPassword: %s\n",
		c.FixedHeader, c.ProtocolName, c.ProtocolVersion, c.ClientID, c.CleanSession, c.KeepAlive, c.Username, string(c.Password))
}

func (c *Connect) Type() byte {
	return packets.ConnectType
}

func (c *Connect) Encode() []byte {
	var body []byte
	// Variable Header
	body = append(body, codec.EncodeString(c.ProtocolName)...)
	body = append(body, c.ProtocolVersion)

	var flags byte
	if c.UsernameFlag {
		flags |= 1 << 7
	}
	if c.PasswordFlag {
		flags |= 1 << 6
	}
	if c.WillRetain {
		flags |= 1 << 5
	}
	flags |= (c.WillQoS & 0x03) << 3
	if c.WillFlag {
		flags |= 1 << 2
	}
	if c.CleanSession {
		flags |= 1 << 1
	}
	// Reserved bit 0
	body = append(body, flags)
	body = append(body, codec.EncodeUint16(c.KeepAlive)...)

	// Payload
	body = append(body, codec.EncodeString(c.ClientID)...)
	if c.WillFlag {
		body = append(body, codec.EncodeString(c.WillTopic)...)
		body = append(body, codec.EncodeBytes(c.WillMessage)...)
	}
	if c.UsernameFlag {
		body = append(body, codec.EncodeString(c.Username)...)
	}
	if c.PasswordFlag {
		body = append(body, codec.EncodeBytes(c.Password)...)
	}

	c.FixedHeader.RemainingLength = len(body)
	return append(c.FixedHeader.Encode(), body...)
}

func (c *Connect) Unpack(r io.Reader) error {
	var err error
	c.ProtocolName, err = codec.DecodeString(r)
	if err != nil {
		return err
	}
	c.ProtocolVersion, err = codec.DecodeByte(r)
	if err != nil {
		return err
	}

	flags, err := codec.DecodeByte(r)
	if err != nil {
		return err
	}
	c.UsernameFlag = (flags & (1 << 7)) > 0
	c.PasswordFlag = (flags & (1 << 6)) > 0
	c.WillRetain = (flags & (1 << 5)) > 0
	c.WillQoS = (flags >> 3) & 0x03
	c.WillFlag = (flags & (1 << 2)) > 0
	c.CleanSession = (flags & (1 << 1)) > 0
	c.ReservedBit = flags & 1

	c.KeepAlive, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}

	c.ClientID, err = codec.DecodeString(r)
	if err != nil {
		return err
	}
	if c.WillFlag {
		c.WillTopic, err = codec.DecodeString(r)
		if err != nil {
			return err
		}
		c.WillMessage, err = codec.DecodeBytes(r)
		if err != nil {
			return err
		}
	}
	if c.UsernameFlag {
		c.Username, err = codec.DecodeString(r)
		if err != nil {
			return err
		}
	}
	if c.PasswordFlag {
		c.Password, err = codec.DecodeBytes(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Connect) Pack(w io.Writer) error {
	_, err := w.Write(c.Encode())
	return err
}

func (c *Connect) Details() packets.Details {
	return packets.Details{Type: packets.ConnectType}
}
