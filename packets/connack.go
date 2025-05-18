package packets

import (
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// ConnackReturnCodes is a map of the error codes constants for Connect()
// to a string representation of the error
var ConnackReturnCodes = map[uint8]string{
	0:   "Connection Accepted",
	1:   "Connection Refused: Bad Protocol Version",
	2:   "Connection Refused: Client Identifier Rejected",
	3:   "Connection Refused: Server Unavailable",
	4:   "Connection Refused: Username or Password in unknown format",
	5:   "Connection Refused: Not Authorised",
	254: "Connection Error",
	255: "Connection Refused: Protocol Violation",
}

// ConnAck is an internal representation of the fields of the ConnAck MQTT packet.
type ConnAck struct {
	FixedHeader
	// Variable Header
	SessionPresent bool
	ReasonCode     byte
	Properties     *ConnAckProperties
}

type ConnAckProperties struct {
	// SessionExpiryInterval is the time in seconds after a client disconnects
	// that the server should retain the session information (subscriptions etc).
	SessionExpiryInterval *uint32
	// ReceiveMax is the maximum number of QOS1 & 2 messages allowed to be
	// 'inflight' (not having received a PUBACK/PUBCOMP response for).
	ReceiveMax *uint16
	// MaxQoS is the highest QOS level permitted for a Publish.
	MaxQoS *byte
	// RetainAvailable indicates whether the server supports messages with the
	// retain flag set.
	RetainAvailable *byte
	// MaximumPacketSize allows the client or server to specify the maximum packet.
	// size in bytes that they support
	MaximumPacketSize *uint32
	// AssignedClientID is the server assigned client identifier in the case
	// that a client connected without specifying a clientID the server
	// generates one and returns it in the Connack.
	AssignedClientID string
	// TopicAliasMax is the highest value permitted as a Topic Alias.
	TopicAliasMax *uint16
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// User is a slice of user provided properties (key and value).
	User []User
	// WildcardSubAvailable indicates whether wildcard subscriptions are permitted.
	WildcardSubAvailable *byte
	// SubIDAvailable indicates whether subscription identifiers are supported.
	SubIDAvailable *byte
	// ServerKeepAlive allows the server to specify in the Connack packet
	// the time in seconds to be used as the keep alive value.
	ServerKeepAlive *uint16
	// ResponseInfo is a UTF8 encoded string that can be used as the basis for
	// createing a Response Topic. The way in which the Client creates a
	// Response Topic from the Response Information is not defined. A common
	// use of this is to pass a globally unique portion of the topic tree which
	// is reserved for this Client for at least the lifetime of its Session. This
	// often cannot just be a random name as both the requesting Client and the
	// responding Client need to be authorized to use it. It is normal to use this
	// as the root of a topic tree for a particular Client. For the Server to
	// return this information, it normally needs to be correctly configured.
	// Using this mechanism allows this configuration to be done once in the
	// Server rather than in each Client.
	ResponseInfo string
	// ServerReference is a UTF8 string indicating another server the client
	// can use.
	ServerReference string
	// AuthMethod is a UTF8 string containing the name of the authentication
	// method to be used for extended authentication.
	AuthMethod string
	// AuthData is binary data containing authentication data.
	AuthData []byte
}

func (p *ConnAckProperties) Unpack(r io.Reader) error {
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
			p.ReceiveMax = &rm
		case MaximumQOSProp:
			mq, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.MaxQoS = &mq
		case RetainAvailableProp:
			ra, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.RetainAvailable = &ra
		case MaximumPacketSizeProp:
			mps, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.MaximumPacketSize = &mps
		case AssignedClientIDProp:
			p.AssignedClientID, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case TopicAliasMaximumProp:
			tam, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAliasMax = &tam
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
		case WildcardSubAvailableProp:
			wsa, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.WildcardSubAvailable = &wsa
		case SubIDAvailableProp:
			sia, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.SubIDAvailable = &sia
		case ServerKeepAliveProp:
			ska, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.ServerKeepAlive = &ska
		case ResponseInfoProp:
			p.ResponseInfo, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case ServerReferenceProp:
			p.ServerReference, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
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
			return fmt.Errorf("invalid property type %d for connack packet", prop)
		}
	}
}

func (p *ConnAckProperties) Encode() []byte {
	var ret []byte
	if p.SessionExpiryInterval != nil {
		ret = append(ret, codec.EncodeUint32(*p.SessionExpiryInterval)...)
	}
	if p.ReceiveMax != nil {
		ret = append(ret, codec.EncodeUint16(*p.ReceiveMax)...)
	}
	if p.MaxQoS != nil {
		ret = append(ret, *p.MaxQoS)
	}
	if p.RetainAvailable != nil {
		ret = append(ret, *p.RetainAvailable)
	}
	if p.MaximumPacketSize != nil {
		ret = append(ret, codec.EncodeUint16(uint16(*p.MaximumPacketSize))...)
	}
	if p.AssignedClientID != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.AssignedClientID))...)
	}
	if p.TopicAliasMax != nil {
		ret = append(ret, codec.EncodeUint16(*p.TopicAliasMax)...)
	}
	if p.ReasonString != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.ReasonString))...)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeBytes([]byte(u.Key))...)
			ret = append(ret, codec.EncodeBytes([]byte(u.Value))...)
		}
	}
	if p.WildcardSubAvailable != nil {
		ret = append(ret, *p.WildcardSubAvailable)
	}
	if p.SubIDAvailable != nil {
		ret = append(ret, *p.SubIDAvailable)
	}
	if p.ServerKeepAlive != nil {
		ret = append(ret, codec.EncodeUint16(*p.ServerKeepAlive)...)
	}
	if p.ResponseInfo != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.ResponseInfo))...)
	}
	if p.ServerReference != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.ServerReference))...)
	}
	if p.AuthMethod != "" {
		ret = append(ret, codec.EncodeBytes([]byte(p.AuthMethod))...)
	}
	if len(p.AuthData) > 0 {
		ret = append(ret, p.AuthData...)
	}

	return ret
}

func (pkt *ConnAck) String() string {
	return fmt.Sprintf("%s SessionPresent: %t ReturnCode %d", pkt.FixedHeader, pkt.SessionPresent, pkt.ReasonCode)
}

func (pkt *ConnAck) Pack(w io.Writer) error {
	bytes := pkt.FixedHeader.Encode()
	bytes = append(bytes, codec.EncodeBool(pkt.SessionPresent))
	bytes = append(bytes, pkt.ReasonCode)
	if pkt.Properties != nil {
		props := pkt.Properties.Encode()
		l := len(props)
		proplen := codec.EncodeVBI(l)
		bytes = append(bytes, proplen...)
		if l > 0 {
			bytes = append(bytes, props...)
		}
	}
	_, err := w.Write(bytes)

	return err
}

func (pkt *ConnAck) Unpack(r io.Reader, v byte) error {
	flags, err := codec.DecodeByte(r)
	if err != nil {
		return err
	}
	pkt.SessionPresent = 1&flags > 0
	pkt.ReasonCode, err = codec.DecodeByte(r)
	if v == V5 {
		length, err := codec.DecodeVBI(r)
		if err != nil {
			return err
		}
		if length != 0 {
			prop := ConnAckProperties{}
			if err := prop.Unpack(r); err != nil {
				return err
			}
			pkt.Properties = &prop
		}
	}

	return err
}

func (pkt *ConnAck) Details() Details {
	return Details{Type: ConnAckType, ID: 0, Qos: 0}
}
