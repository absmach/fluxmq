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
	// Variable Header
	ProtocolName    string
	ProtocolVersion byte
	UsernameFlag    bool
	PasswordFlag    bool
	WillRetain      bool
	WillQoS         byte
	WillFlag        bool
	CleanStart      bool
	ReservedBit     byte
	KeepAlive       uint16
	Properties      *ConnectProperties
	// Payload
	ClientID       string
	WillProperties *WillProperties
	WillTopic      string
	WillPayload    []byte
	Username       string
	Password       []byte
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
		default:
			return fmt.Errorf("invalid property type %d for connect packet", prop)
		}
	}
}

func (p *ConnectProperties) Encode() []byte {
	var ret []byte

	if p.SessionExpiryInterval != nil {
		ret = append(ret, SessionExpiryIntervalProp)
		ret = append(ret, codec.EncodeUint32(*p.SessionExpiryInterval)...)
	}
	if p.ReceiveMaximum != nil {
		ret = append(ret, ReceiveMaximumProp)
		ret = append(ret, codec.EncodeUint16(*p.ReceiveMaximum)...)
	}
	if p.MaximumPacketSize != nil {
		ret = append(ret, MaximumPacketSizeProp)
		ret = append(ret, codec.EncodeUint16(uint16(*p.MaximumPacketSize))...)
	}
	if p.TopicAliasMaximum != nil {
		ret = append(ret, TopicAliasMaximumProp)
		ret = append(ret, codec.EncodeUint16(*p.TopicAliasMaximum)...)
	}
	if p.RequestResponseInfo != nil {
		ret = append(ret, RequestProblemInfoProp)
		ret = append(ret, *p.RequestResponseInfo)
	}
	if p.RequestProblemInfo != nil {
		ret = append(ret, RequestProblemInfoProp)
		ret = append(ret, *p.RequestProblemInfo)
	}
	if len(p.User) > 0 {
		ret = append(ret, UserProp)
		for _, u := range p.User {
			ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
			ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
		}
	}
	if p.AuthMethod != "" {
		ret = append(ret, AuthMethodProp)
		ret = append(ret, codec.EncodeBytes([]byte(p.AuthMethod))...)
	}
	if len(p.AuthData) > 0 {
		ret = append(ret, AuthDataProp)
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
		default:
			return fmt.Errorf("invalid will property type %d", prop)
		}
	}
}

func (p *WillProperties) Encode() []byte {
	var ret []byte
	if p.WillDelayInterval != nil {
		ret = append(ret, WillDelayIntervalProp)
		ret = append(ret, codec.EncodeUint32(*p.WillDelayInterval)...)
	}
	if p.PayloadFormat != nil {
		ret = append(ret, PayloadFormatProp, *p.PayloadFormat)
	}
	if p.MessageExpiry != nil {
		ret = append(ret, MessageExpiryProp)
		ret = append(ret, codec.EncodeUint32(*p.MessageExpiry)...)
	}
	if p.ContentType != "" {
		ret = append(ret, ContentTypeProp)
		ret = append(ret, codec.EncodeBytes([]byte(p.ContentType))...)
	}
	if p.ResponseTopic != "" {
		ret = append(ret, ResponseTopicProp)
		ret = append(ret, codec.EncodeBytes([]byte(p.ResponseTopic))...)
	}
	if len(p.CorrelationData) > 0 {
		ret = append(ret, CorrelationDataProp)
		ret = append(ret, p.CorrelationData...)
	}
	if len(p.User) > 0 {
		ret = append(ret, UserProp)
		for _, u := range p.User {
			ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
			ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
		}
	}

	return ret
}

func (pkt *Connect) String() string {
	return fmt.Sprintf(stringFormat, pkt.FixedHeader, pkt.ProtocolVersion, pkt.ProtocolName, pkt.CleanStart,
		pkt.WillFlag, pkt.WillQoS, pkt.WillRetain, pkt.UsernameFlag, pkt.PasswordFlag, pkt.KeepAlive,
		pkt.ClientID, pkt.WillTopic, pkt.WillPayload, pkt.Username, pkt.Password)
}

func (pkt *Connect) PackFlags() byte {
	var flags byte
	if pkt.UsernameFlag {
		flags |= 1 << 7
	}
	if pkt.PasswordFlag {
		flags |= 1 << 6
	}
	if pkt.WillRetain {
		flags |= 1 << 5
	}
	flags |= (pkt.WillQoS & 0x03) << 3
	if pkt.WillFlag {
		flags |= 1 << 2
	}
	if pkt.CleanStart {
		flags |= 1 << 1
	}

	return flags
}

func (pkt *Connect) Encode() []byte {
	ret := codec.EncodeBytes([]byte(pkt.ProtocolName))
	ret = append(ret, pkt.ProtocolVersion)
	ret = append(ret, pkt.PackFlags())
	ret = append(ret, codec.EncodeUint16(pkt.KeepAlive)...)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		ret = append(ret, proplen...)
		if l > 0 {
			ret = append(ret, props...)
		}
	}
	// Payload
	ret = append(ret, codec.EncodeBytes([]byte(pkt.ClientID))...)
	if pkt.WillFlag {
		if pkt.Properties != nil {
			props := pkt.Properties.Encode()
			l := len(props)
			proplen := codec.EncodeVBI(l)
			ret = append(ret, proplen...)
			if l > 0 {
				ret = append(ret, props...)
			}
		}
	}
	if pkt.UsernameFlag {
		ret = append(ret, codec.EncodeBytes([]byte(pkt.Username))...)
	}
	if pkt.PasswordFlag {
		ret = append(ret, codec.EncodeBytes(pkt.Password)...)
	}
	// Take care size is calculated properly if someone tempered with the packet.
	pkt.FixedHeader.RemainingLength = len(ret)
	ret = append(pkt.FixedHeader.Encode(), ret...)

	return ret
}

func (pkt *Connect) Pack(w io.Writer) error {
	_, err := w.Write(pkt.Encode())
	return err
}

func (pkt *Connect) Unpack(r io.Reader, v byte) error {
	var err error
	pkt.ProtocolName, err = codec.DecodeString(r)
	if err != nil {
		return err
	}
	pkt.ProtocolVersion, err = codec.DecodeByte(r)
	if err != nil {
		return err
	}
	opts, err := codec.DecodeByte(r)
	if err != nil {
		return err
	}
	pkt.ReservedBit = 1 & opts
	pkt.CleanStart = 1&(opts>>1) > 0
	pkt.WillFlag = 1&(opts>>2) > 0
	pkt.WillQoS = 3 & (opts >> 3)
	pkt.WillRetain = 1&(opts>>5) > 0
	pkt.PasswordFlag = 1&(opts>>6) > 0
	pkt.UsernameFlag = 1&(opts>>7) > 0
	pkt.KeepAlive, err = codec.DecodeUint16(r)
	if err != nil {
		return err
	}
	if v == V5 {
		length, err := codec.DecodeVBI(r)
		if err != nil {
			return err
		}
		if length != 0 {
			buf := make([]byte, length)
			if _, err := r.Read(buf); err != nil {
				return err
			}
			p := ConnectProperties{}
			props := bytes.NewReader(buf)
			if err := p.Unpack(props); err != nil {
				return err
			}
			pkt.Properties = &p
		}
	}
	pkt.ClientID, err = codec.DecodeString(r)
	if err != nil {
		return err
	}
	if pkt.WillFlag {
		if v == V5 {
			length, err := codec.DecodeVBI(r)
			if err != nil {
				return err
			}
			if length != 0 {
				buf := make([]byte, length)
				if _, err := r.Read(buf); err != nil {
					return err
				}
				p := WillProperties{}
				props := bytes.NewReader(buf)
				if err := p.Unpack(props); err != nil {
					return err
				}
				pkt.WillProperties = &p
			}
		}
		pkt.WillTopic, err = codec.DecodeString(r)
		if err != nil {
			return err
		}
		pkt.WillPayload, err = codec.DecodeBytes(r)
		if err != nil {
			return err
		}
	}
	if pkt.UsernameFlag {
		pkt.Username, err = codec.DecodeString(r)
		if err != nil {
			return err
		}
	}
	if pkt.PasswordFlag {
		pkt.Password, err = codec.DecodeBytes(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pkt *Connect) Validate() byte {
	if pkt.PasswordFlag && !pkt.UsernameFlag {
		return ErrRefusedBadUsernameOrPassword
	}
	if pkt.ReservedBit != 0 {
		// Bad reserved bit
		return ErrProtocolViolation
	}
	if (pkt.ProtocolName == "MQIsdp" && pkt.ProtocolVersion != 3) || (pkt.ProtocolName == "MQTT" && pkt.ProtocolVersion != 4) {
		// Mismatched or unsupported protocol version
		return ErrRefusedBadProtocolVersion
	}
	if pkt.ProtocolName != "MQIsdp" && pkt.ProtocolName != "MQTT" {
		// Bad protocol name
		return ErrProtocolViolation
	}
	if len(pkt.ClientID) > 65535 || len(pkt.Username) > 65535 || len(pkt.Password) > 65535 {
		// Bad size field
		return ErrProtocolViolation
	}
	if len(pkt.ClientID) == 0 && !pkt.CleanStart {
		// Bad client identifier
		return ErrRefusedIDRejected
	}
	return Accepted
}

func (pkt *Connect) Details() Details {
	return Details{Type: ConnectType, ID: 0, Qos: 0}
}
