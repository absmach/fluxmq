package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
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

const stringFormat = `%s
protocol_version: %d
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
	CleanStart      bool
	WillFlag        bool
	WillQoS         byte
	WillRetain      bool
	UsernameFlag    bool
	PasswordFlag    bool
	ReservedBit     byte
	KeepAlive       uint16

	Properties     *ConnectProperties
	WillProperties *WillProperties

	// Payload
	ClientIdentifier string
	WillTopic        string
	WillPayload      []byte
	Username         string
	Password         []byte
}

type ConnectProperties struct {
	// SessionExpiryInterval is the time in seconds after a client disconnects
	// that the server should retain the session information (subscriptions etc)
	SessionExpiryInterval *uint32
	// ReceiveMaximum is the maximum number of QOS1 & 2 messages allowed to be
	// 'inflight' (not having received a PUBACK/PUBCOMP response for)
	ReceiveMaximum *uint16
	// MaximumPacketSize allows the client or server to specify the maximum packet
	// size in bytes that they support
	MaximumPacketSize *uint32
	// TopicAliasMaximum is the highest value permitted as a Topic Alias
	TopicAliasMaximum *uint16
	// RequestResponseInfo is used by the Client to request the Server provide
	// Response Information in the Connack
	RequestResponseInfo *byte
	// RequestProblemInfo is used by the Client to indicate to the server to
	// include the Reason String and/or User Properties in case of failures
	RequestProblemInfo *byte
	// User is a slice of user provided properties (key and value)
	User []User
	// AuthMethod is a UTF8 string containing the name of the authentication
	// method to be used for extended authentication
	AuthMethod string
	// AuthData is binary data containing authentication data
	AuthData []byte
}

func (p *ConnectProperties) Unpack(r io.Reader) error {
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
		case ReceiveMaximumProp:
			rm, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.ReceiveMaximum = &rm
		case MaximumPacketSizeProp:
			mps, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.MaximumPacketSize = &mps
		case TopicAliasMaximumProp:
			tam, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAliasMaximum = &tam
		case RequestResponseInfoProp:
			rri, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.RequestResponseInfo = &rri
		case RequestProblemInfoProp:
			rpi, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.RequestProblemInfo = &rpi
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
		}
	}
}

func (p *ConnectProperties) encode() []byte {
	var ret []byte

	if p.SessionExpiryInterval != nil {
		ret = append(ret, codec.EncodeUint32(*p.SessionExpiryInterval)...)
	}
	if p.ReceiveMaximum != nil {
		ret = append(ret, codec.EncodeUint16(*p.ReceiveMaximum)...)
	}
	if p.MaximumPacketSize != nil {
		ret = append(ret, codec.EncodeUint16(uint16(*p.MaximumPacketSize))...)
	}
	if p.TopicAliasMaximum != nil {
		ret = append(ret, codec.EncodeUint16(*p.TopicAliasMaximum)...)
	}
	if p.RequestResponseInfo != nil {
		ret = append(ret, *p.RequestResponseInfo)
	}
	if p.RequestProblemInfo != nil {
		ret = append(ret, *p.RequestProblemInfo)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeString(u.Key)...)
			ret = append(ret, codec.EncodeString(u.Value)...)
		}
	}
	if p.AuthMethod != "" {
		ret = append(ret, codec.EncodeString(p.AuthMethod)...)
	}
	if len(p.AuthData) > 0 {
		ret = append(ret, p.AuthData...)
	}

	return ret
}

type WillProperties struct {
	// WillDelayInterval is the number of seconds the server waits after the
	// point at which it would otherwise send the will message before sending
	// it. The client reconnecting before that time expires causes the server
	// to cancel sending the will.
	WillDelayInterval *uint32
	// PayloadFormat indicates the format of the payload of the message
	// 0 is unspecified bytes
	// 1 is UTF8 encoded character data
	PayloadFormat *byte
	// MessageExpiry is the lifetime of the message in seconds.
	MessageExpiry *uint32
	// ContentType is a UTF8 string describing the content of the message
	// for example it could be a MIME type.
	ContentType string
	// ResponseTopic is a UTF8 string indicating the topic name to which any
	// response to this message should be sent.
	ResponseTopic string
	// CorrelationData is binary data used to associate future response
	// messages with the original request message.
	CorrelationData []byte
	// User is a slice of user provided properties (key and value).
	User []User
	// MaximumPacketSize allows the client or server to specify the maximum packet.
}

func (p *WillProperties) Unpack(r io.Reader) error {
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
		case WillDelayIntervalProp:
			wdi, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.WillDelayInterval = &wdi
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
			p.User = append(p.User, User{k, v})
		}
	}
}

func (p *WillProperties) encode() []byte {
	var ret []byte
	if p.WillDelayInterval != nil {
		ret = append(ret, codec.EncodeUint32(*p.WillDelayInterval)...)
	}
	if p.PayloadFormat != nil {
		ret = append(ret, *p.PayloadFormat)
	}
	if p.MessageExpiry != nil {
		ret = append(ret, codec.EncodeUint32(*p.MessageExpiry)...)
	}
	if p.ContentType != "" {
		ret = append(ret, codec.EncodeString(p.ContentType)...)
	}
	if p.ResponseTopic != "" {
		ret = append(ret, codec.EncodeString(p.ResponseTopic)...)
	}
	if len(p.CorrelationData) > 0 {
		ret = append(ret, p.CorrelationData...)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeString(u.Key)...)
			ret = append(ret, codec.EncodeString(u.Value)...)
		}
	}

	return ret
}

func (c *Connect) String() string {
	return fmt.Sprintf(stringFormat, c.FixedHeader, c.ProtocolVersion, c.ProtocolName, c.CleanStart,
		c.WillFlag, c.WillQoS, c.WillRetain, c.UsernameFlag, c.PasswordFlag, c.KeepAlive,
		c.ClientIdentifier, c.WillTopic, c.WillPayload, c.Username, c.Password)
}

func (c *Connect) Pack(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(codec.EncodeString(c.ProtocolName))
	body.WriteByte(c.ProtocolVersion)
	body.WriteByte(codec.EncodeBool(c.CleanStart)<<1 | codec.EncodeBool(c.WillFlag)<<2 | c.WillQoS<<3 | codec.EncodeBool(c.WillRetain)<<5 | codec.EncodeBool(c.PasswordFlag)<<6 | codec.EncodeBool(c.UsernameFlag)<<7)
	body.Write(codec.EncodeUint16(c.KeepAlive))
	body.Write(codec.EncodeString(c.ClientIdentifier))
	if c.WillFlag {
		body.Write(codec.EncodeString(c.WillTopic))
		body.Write(codec.EncodeBytes(c.WillPayload))
	}
	if c.UsernameFlag {
		body.Write(codec.EncodeString(c.Username))
	}
	if c.PasswordFlag {
		body.Write(codec.EncodeBytes(c.Password))
	}
	c.FixedHeader.RemainingLength = body.Len()
	packet := c.FixedHeader.encode()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (c *Connect) Unpack(b io.Reader) error {
	var err error
	c.ProtocolName, err = codec.DecodeString(b)
	if err != nil {
		return err
	}
	c.ProtocolVersion, err = codec.DecodeByte(b)
	if err != nil {
		return err
	}
	options, err := codec.DecodeByte(b)
	if err != nil {
		return err
	}
	c.ReservedBit = 1 & options
	c.CleanStart = 1&(options>>1) > 0
	c.WillFlag = 1&(options>>2) > 0
	c.WillQoS = 3 & (options >> 3)
	c.WillRetain = 1&(options>>5) > 0
	c.PasswordFlag = 1&(options>>6) > 0
	c.UsernameFlag = 1&(options>>7) > 0
	c.KeepAlive, err = codec.DecodeUint16(b)
	if err != nil {
		return err
	}
	c.ClientIdentifier, err = codec.DecodeString(b)
	if err != nil {
		return err
	}
	if c.WillFlag {
		c.WillTopic, err = codec.DecodeString(b)
		if err != nil {
			return err
		}
		c.WillPayload, err = codec.DecodeBytes(b)
		if err != nil {
			return err
		}
	}
	if c.UsernameFlag {
		c.Username, err = codec.DecodeString(b)
		if err != nil {
			return err
		}
	}
	if c.PasswordFlag {
		c.Password, err = codec.DecodeBytes(b)
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
		// Bad reserved bit
		return ErrProtocolViolation
	}
	if (c.ProtocolName == "MQIsdp" && c.ProtocolVersion != 3) || (c.ProtocolName == "MQTT" && c.ProtocolVersion != 4) {
		// Mismatched or unsupported protocol version
		return ErrRefusedBadProtocolVersion
	}
	if c.ProtocolName != "MQIsdp" && c.ProtocolName != "MQTT" {
		// Bad protocol name
		return ErrProtocolViolation
	}
	if len(c.ClientIdentifier) > 65535 || len(c.Username) > 65535 || len(c.Password) > 65535 {
		// Bad size field
		return ErrProtocolViolation
	}
	if len(c.ClientIdentifier) == 0 && !c.CleanStart {
		// Bad client identifier
		return ErrRefusedIDRejected
	}
	return Accepted
}

// Details returns a Details struct containing the Qos and
// ID of this ControlPacket
func (c *Connect) Details() Details {
	return Details{Type: ConnectType, ID: 0, Qos: 0}
}
