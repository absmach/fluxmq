package packets

import (
	"bytes"
	"fmt"
	"io"
)

// Error codes returned by Connect()
const (
	Accepted                        = 0x00
	ErrRefusedBadProtocolVersion    = 0x01
	ErrRefusedIDRejected            = 0x02
	ErrRefusedServerUnavailable     = 0x03
	ErrRefusedBadUsernameOrPassword = 0x04
	ErrRefusedNotAuthorized         = 0x05
	ErrNetworkError                 = 0xFE
	ErrProtocolViolation            = 0xFF
)

const stringFormat = `protocol_version: %d
protocol_name: %s
clean_session: %t
will: %t
will_qos: %d
will_retain: %t
username_flag: %t
password_flag: %t
keepalive: %d
clientId: %s
will_topic: %s
will_message: %s
username: %s
password: %s`

// Connect is an internal representation of the fields of the MQTT CONNECT packet.
type Connect struct {
	FixedHeader
	ProtocolName    string
	ProtocolVersion byte
	CleanSession    bool
	WillFlag        bool
	WillQos         byte
	WillRetain      bool
	UsernameFlag    bool
	PasswordFlag    bool
	ReservedBit     byte
	KeepAlive       uint16

	ClientIdentifier string
	WillTopic        string
	WillMessage      []byte
	Username         string
	Password         []byte
}

func (c *Connect) String() string {
	return c.FixedHeader.String() + " " + fmt.Sprintf(stringFormat, c.ProtocolVersion, c.ProtocolName, c.CleanSession,
		c.WillFlag, c.WillQos, c.WillRetain, c.UsernameFlag, c.PasswordFlag, c.KeepAlive,
		c.ClientIdentifier, c.WillTopic, c.WillMessage, c.Username, c.Password)
}

func (c *Connect) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(encodeString(c.ProtocolName))
	body.WriteByte(c.ProtocolVersion)
	body.WriteByte(boolToByte(c.CleanSession)<<1 | boolToByte(c.WillFlag)<<2 | c.WillQos<<3 | boolToByte(c.WillRetain)<<5 | boolToByte(c.PasswordFlag)<<6 | boolToByte(c.UsernameFlag)<<7)
	body.Write(encodeUint16(c.KeepAlive))
	body.Write(encodeString(c.ClientIdentifier))
	if c.WillFlag {
		body.Write(encodeString(c.WillTopic))
		body.Write(encodeBytes(c.WillMessage))
	}
	if c.UsernameFlag {
		body.Write(encodeString(c.Username))
	}
	if c.PasswordFlag {
		body.Write(encodeBytes(c.Password))
	}
	c.FixedHeader.RemainingLength = body.Len()
	packet := c.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (c *Connect) Unpack(b io.Reader) error {
	var err error
	c.ProtocolName, err = decodeString(b)
	if err != nil {
		return err
	}
	c.ProtocolVersion, err = decodeByte(b)
	if err != nil {
		return err
	}
	options, err := decodeByte(b)
	if err != nil {
		return err
	}
	c.ReservedBit = 1 & options
	c.CleanSession = 1&(options>>1) > 0
	c.WillFlag = 1&(options>>2) > 0
	c.WillQos = 3 & (options >> 3)
	c.WillRetain = 1&(options>>5) > 0
	c.PasswordFlag = 1&(options>>6) > 0
	c.UsernameFlag = 1&(options>>7) > 0
	c.KeepAlive, err = decodeUint16(b)
	if err != nil {
		return err
	}
	c.ClientIdentifier, err = decodeString(b)
	if err != nil {
		return err
	}
	if c.WillFlag {
		c.WillTopic, err = decodeString(b)
		if err != nil {
			return err
		}
		c.WillMessage, err = decodeBytes(b)
		if err != nil {
			return err
		}
	}
	if c.UsernameFlag {
		c.Username, err = decodeString(b)
		if err != nil {
			return err
		}
	}
	if c.PasswordFlag {
		c.Password, err = decodeBytes(b)
		if err != nil {
			return err
		}
	}

	return nil
}

// Validate performs validation of the fields of a Connect packet
func (c *Connect) Validate() byte {
	if c.PasswordFlag && !c.UsernameFlag {
		return ErrRefusedBadUsernameOrPassword
	}
	if c.ReservedBit != 0 {
		//Bad reserved bit
		return ErrProtocolViolation
	}
	if (c.ProtocolName == "MQIsdp" && c.ProtocolVersion != 3) || (c.ProtocolName == "MQTT" && c.ProtocolVersion != 4) {
		//Mismatched or unsupported protocol version
		return ErrRefusedBadProtocolVersion
	}
	if c.ProtocolName != "MQIsdp" && c.ProtocolName != "MQTT" {
		//Bad protocol name
		return ErrProtocolViolation
	}
	if len(c.ClientIdentifier) > 65535 || len(c.Username) > 65535 || len(c.Password) > 65535 {
		//Bad size field
		return ErrProtocolViolation
	}
	if len(c.ClientIdentifier) == 0 && !c.CleanSession {
		//Bad client identifier
		return ErrRefusedIDRejected
	}
	return Accepted
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (c *Connect) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
