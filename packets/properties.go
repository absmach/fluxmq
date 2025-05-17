package packets

import (
	"io"

	"github.com/dborovcanin/mqtt/packets/codec"
)

// PropPayloadFormat, etc are the list of property codes for the
// MQTT packet properties
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

type Properties struct {
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
	// SubscriptionIdentifier is an identifier of the subscription to which
	// the Publish matched.
	SubscriptionIdentifier *int
	// SessionExpiryInterval is the time in seconds after a client disconnects
	// that the server should retain the session information (subscriptions etc).
	SessionExpiryInterval *uint32
	// AssignedClientID is the server assigned client identifier in the case
	// that a client connected without specifying a clientID the server
	// generates one and returns it in the Connack.
	AssignedClientID string
	// ServerKeepAlive allows the server to specify in the Connack packet
	// the time in seconds to be used as the keep alive value.
	ServerKeepAlive *uint16
	// AuthMethod is a UTF8 string containing the name of the authentication
	// method to be used for extended authentication.
	AuthMethod string
	// AuthData is binary data containing authentication data.
	AuthData []byte
	// RequestProblemInfo is used by the Client to indicate to the server to
	// include the Reason String and/or User Properties in case of failures.
	RequestProblemInfo *byte
	// WillDelayInterval is the number of seconds the server waits after the
	// point at which it would otherwise send the will message before sending
	// it. The client reconnecting before that time expires causes the server
	// to cancel sending the will.
	WillDelayInterval *uint32
	// RequestResponseInfo is used by the Client to request the Server provide
	// Response Information in the Connack.
	RequestResponseInfo *byte
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
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// ReceiveMax is the maximum number of QOS1 & 2 messages allowed to be
	// 'inflight' (not having received a PUBACK/PUBCOMP response for).
	ReceiveMax *uint16
	// TopicAliasMax is the highest value permitted as a Topic Alias.
	TopicAliasMax *uint16
	// TopicAlias is used in place of the topic string to reduce the size of
	// packets for repeated messages on a topic.
	TopicAlias *uint16
	// MaxQoS is the highest QOS level permitted for a Publish.
	MaxQoS *byte
	// RetainAvailable indicates whether the server supports messages with the
	// retain flag set.
	RetainAvailable *byte
	// User is a slice of user provided properties (key and value).
	User []User
	// MaximumPacketSize allows the client or server to specify the maximum packet.
	// size in bytes that they support
	MaximumPacketSize *uint32
	// WildcardSubAvailable indicates whether wildcard subscriptions are permitted.
	WildcardSubAvailable *byte
	// SubIDAvailable indicates whether subscription identifiers are supported.
	SubIDAvailable *byte
	// SharedSubAvailable indicates whether shared subscriptions are supported.
	SharedSubAvailable *byte
}

type User struct {
	Key, Value string
}

type BasicProperties struct {
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes.
	ReasonString string
	// User is a slice of user provided properties (key and value).
	User []User
}

func (p *BasicProperties) Unpack(r io.Reader) error {
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
		}
	}
}

func (p *BasicProperties) encode() []byte {
	var ret []byte
	if p.ReasonString != "" {
		ret = append(ret, codec.EncodeString(p.ReasonString)...)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeString(u.Key)...)
			ret = append(ret, codec.EncodeString(u.Value)...)
		}
	}

	return ret
}

func (p *Properties) Unpack(r io.Reader) error {
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
		case SubscriptionIdentifierProp:
			si, err := codec.DecodeVBI(r)
			if err != nil {
				return err
			}
			p.SubscriptionIdentifier = &si
		case SessionExpiryIntervalProp:
			sei, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.SessionExpiryInterval = &sei
		case AssignedClientIDProp:
			p.AssignedClientID, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case ServerKeepAliveProp:
			ska, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.ServerKeepAlive = &ska
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
		case RequestProblemInfoProp:
			rpi, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.RequestProblemInfo = &rpi
		case WillDelayIntervalProp:
			wdi, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.WillDelayInterval = &wdi
		case RequestResponseInfoProp:
			rri, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.RequestResponseInfo = &rri
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
		case ReasonStringProp:
			p.ReasonString, err = codec.DecodeString(r)
			if err != nil {
				return err
			}
		case ReceiveMaximumProp:
			rm, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.ReceiveMax = &rm
		case TopicAliasMaximumProp:
			tam, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAliasMax = &tam
		case TopicAliasProp:
			ta, err := codec.DecodeUint16(r)
			if err != nil {
				return err
			}
			p.TopicAlias = &ta
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
		case MaximumPacketSizeProp:
			mps, err := codec.DecodeUint32(r)
			if err != nil {
				return err
			}
			p.MaximumPacketSize = &mps
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
		case SharedSubAvailableProp:
			ssa, err := codec.DecodeByte(r)
			if err != nil {
				return err
			}
			p.SharedSubAvailable = &ssa
		}
	}
}

func (p *Properties) encode() []byte {
	var ret []byte
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
	if p.SubscriptionIdentifier != nil {
		ret = append(ret, codec.EncodeVBI(*p.SubscriptionIdentifier)...)
	}
	if p.SessionExpiryInterval != nil {
		ret = append(ret, codec.EncodeUint32(*p.SessionExpiryInterval)...)
	}
	if p.AssignedClientID != "" {
		ret = append(ret, codec.EncodeString(p.AssignedClientID)...)
	}
	if p.ServerKeepAlive != nil {
		ret = append(ret, codec.EncodeUint16(*p.ServerKeepAlive)...)
	}
	if p.AuthMethod != "" {
		ret = append(ret, codec.EncodeString(p.AuthMethod)...)
	}
	if len(p.AuthData) > 0 {
		ret = append(ret, p.AuthData...)
	}
	if p.RequestProblemInfo != nil {
		ret = append(ret, *p.RequestProblemInfo)
	}
	if p.WillDelayInterval != nil {
		ret = append(ret, codec.EncodeUint32(*p.WillDelayInterval)...)
	}
	if p.RequestResponseInfo != nil {
		ret = append(ret, *p.RequestResponseInfo)
	}
	if p.ResponseInfo != "" {
		ret = append(ret, codec.EncodeString(p.ResponseInfo)...)
	}
	if p.ServerReference != "" {
		ret = append(ret, codec.EncodeString(p.ServerReference)...)
	}
	if p.ReasonString != "" {
		ret = append(ret, codec.EncodeString(p.ReasonString)...)
	}
	if p.ReceiveMax != nil {
		ret = append(ret, codec.EncodeUint16(*p.ReceiveMax)...)
	}
	if p.TopicAliasMax != nil {
		ret = append(ret, codec.EncodeUint16(*p.TopicAliasMax)...)
	}
	if p.TopicAlias != nil {
		ret = append(ret, codec.EncodeUint16(*p.TopicAlias)...)
	}
	if p.MaxQoS != nil {
		ret = append(ret, *p.MaxQoS)
	}
	if p.RetainAvailable != nil {
		ret = append(ret, *p.RetainAvailable)
	}
	if len(p.User) > 0 {
		for _, u := range p.User {
			ret = append(ret, codec.EncodeString(u.Key)...)
			ret = append(ret, codec.EncodeString(u.Value)...)
		}
	}
	if p.MaximumPacketSize != nil {
		ret = append(ret, codec.EncodeUint16(uint16(*p.MaximumPacketSize))...)
	}
	if p.WildcardSubAvailable != nil {
		ret = append(ret, *p.WildcardSubAvailable)
	}
	if p.SubIDAvailable != nil {
		ret = append(ret, *p.SubIDAvailable)
	}
	if p.SharedSubAvailable != nil {
		ret = append(ret, *p.SharedSubAvailable)
	}

	return ret
}
