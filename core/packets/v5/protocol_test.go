package v5_test

import (
	"bytes"
	"io"
	"testing"

	. "github.com/absmach/mqtt/core/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	validClientID = "test-client-123"
	maxPacketID   = uint16(65535)
	testTopic     = "test/topic"
	validPayload  = []byte("test payload")
)

func TestConnectV5ProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Connect
		wantErr bool
	}{
		{
			desc: "valid connect with all fields",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       60,
				ClientID:        validClientID,
				UsernameFlag:    true,
				PasswordFlag:    true,
				Username:        "user",
				Password:        []byte("pass"),
				WillFlag:        true,
				WillQoS:         1,
				WillRetain:      true,
				WillTopic:       "will/topic",
				WillPayload:     []byte("gone"),
			},
			wantErr: false,
		},
		{
			desc: "valid connect with properties",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       60,
				ClientID:        validClientID,
				Properties: &ConnectProperties{
					SessionExpiryInterval: ptr(uint32(3600)),
					ReceiveMaximum:        ptr(uint16(100)),
					MaximumPacketSize:     ptr(uint32(268435455)),
					TopicAliasMaximum:     ptr(uint16(10)),
					RequestResponseInfo:   ptr(byte(1)),
					RequestProblemInfo:    ptr(byte(1)),
					User:                  []User{{Key: "key1", Value: "value1"}},
					AuthMethod:            "SCRAM-SHA-256",
					AuthData:              []byte("auth-data"),
				},
			},
			wantErr: false,
		},
		{
			desc: "valid connect with will properties",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       60,
				ClientID:        validClientID,
				WillFlag:        true,
				WillQoS:         1,
				WillRetain:      true,
				WillTopic:       "will/topic",
				WillPayload:     []byte("gone"),
				WillProperties: &WillProperties{
					WillDelayInterval: ptr(uint32(30)),
					PayloadFormat:     ptr(byte(1)),
					MessageExpiry:     ptr(uint32(7200)),
					ContentType:       "application/json",
					ResponseTopic:     "response",
					CorrelationData:   []byte("correlation"),
					User:              []User{{Key: "will-key", Value: "will-value"}},
				},
			},
			wantErr: false,
		},
		{
			desc: "valid connect minimal",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       0,
				ClientID:        validClientID,
			},
			wantErr: false,
		},
		{
			desc: "valid connect with empty client ID and clean start",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       60,
				ClientID:        "",
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			connect, ok := decoded.(*Connect)
			require.True(t, ok)

			assert.Equal(t, tc.pkt.ProtocolName, connect.ProtocolName)
			assert.Equal(t, tc.pkt.ProtocolVersion, connect.ProtocolVersion)
			assert.Equal(t, tc.pkt.ClientID, connect.ClientID)
			assert.Equal(t, tc.pkt.CleanStart, connect.CleanStart)
			assert.Equal(t, tc.pkt.KeepAlive, connect.KeepAlive)
		})
	}
}

func TestConnAckV5ProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *ConnAck
		wantErr bool
	}{
		{
			desc: "valid connack minimal",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: false,
				ReasonCode:     0x00,
			},
			wantErr: false,
		},
		{
			desc: "valid connack with session present",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: true,
				ReasonCode:     0x00,
			},
			wantErr: false,
		},
		{
			desc: "valid connack with properties",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: false,
				ReasonCode:     0x00,
				Properties: &ConnAckProperties{
					SessionExpiryInterval: ptr(uint32(3600)),
					ReceiveMax:            ptr(uint16(65535)),
					MaxQoS:                ptr(byte(2)),
					RetainAvailable:       ptr(byte(1)),
					MaximumPacketSize:     ptr(uint32(268435455)),
					AssignedClientID:      "server-assigned",
					TopicAliasMax:         ptr(uint16(10)),
					ReasonString:          "Connection accepted",
					User:                  []User{{Key: "server-info", Value: "value"}},
					WildcardSubAvailable:  ptr(byte(1)),
					SubIDAvailable:        ptr(byte(1)),
					ServerKeepAlive:       ptr(uint16(120)),
					ResponseInfo:          "response info",
					ServerReference:       "other-server",
					AuthMethod:            "SCRAM-SHA-256",
					AuthData:              []byte("auth-response"),
				},
			},
			wantErr: false,
		},
		{
			desc: "valid connack with error reason code",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: false,
				ReasonCode:     0x80, // Unspecified error
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			connack, ok := decoded.(*ConnAck)
			require.True(t, ok)

			assert.Equal(t, tc.pkt.SessionPresent, connack.SessionPresent)
			assert.Equal(t, tc.pkt.ReasonCode, connack.ReasonCode)
		})
	}
}

func TestPublishV5ProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Publish
		wantErr bool
	}{
		{
			desc: "valid publish QoS 0 without packet ID",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   testTopic,
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with properties",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
				TopicName:   testTopic,
				ID:          1,
				Payload:     validPayload,
				Properties: &PublishProperties{
					PayloadFormat:   ptr(byte(1)),
					MessageExpiry:   ptr(uint32(3600)),
					TopicAlias:      ptr(uint16(5)),
					ResponseTopic:   "response/topic",
					CorrelationData: []byte("correlation-123"),
					User:            []User{{Key: "custom", Value: "header"}},
					SubscriptionID:  ptr(1),
					ContentType:     "application/json",
				},
			},
			wantErr: false,
		},
		{
			desc: "valid publish with topic alias",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
				TopicName:   "",
				ID:          1,
				Payload:     validPayload,
				Properties: &PublishProperties{
					TopicAlias: ptr(uint16(1)),
				},
			},
			wantErr: false,
		},
		{
			desc: "valid publish QoS 2 with retain",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 2, Retain: true},
				TopicName:   testTopic,
				ID:          maxPacketID,
				Payload:     validPayload,
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			pub, ok := decoded.(*Publish)
			require.True(t, ok)

			assert.Equal(t, tc.pkt.TopicName, pub.TopicName)
			assert.Equal(t, tc.pkt.QoS, pub.QoS)
			assert.Equal(t, tc.pkt.Retain, pub.Retain)
			if tc.pkt.QoS > 0 {
				assert.Equal(t, tc.pkt.ID, pub.ID)
			}
			assert.Equal(t, tc.pkt.Payload, pub.Payload)
		})
	}
}

func TestSubscribeV5ProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Subscribe
		wantErr bool
	}{
		{
			desc: "valid subscribe single topic",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Opts:        []SubOption{{Topic: testTopic, MaxQoS: 1}},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe with options",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Opts: []SubOption{
					{
						Topic:             testTopic,
						MaxQoS:            2,
						NoLocal:           ptr(true),
						RetainAsPublished: ptr(true),
						RetainHandling:    ptr(byte(1)),
					},
				},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe multiple topics with different options",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          100,
				Opts: []SubOption{
					{Topic: "topic/one", MaxQoS: 0},
					{Topic: "topic/two", MaxQoS: 1, NoLocal: ptr(true)},
					{Topic: "topic/three", MaxQoS: 2, RetainAsPublished: ptr(true)},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			sub, ok := decoded.(*Subscribe)
			require.True(t, ok)

			assert.Equal(t, tc.pkt.ID, sub.ID)
			assert.Equal(t, len(tc.pkt.Opts), len(sub.Opts))
		})
	}
}

func TestPubAckV5ReasonCodes(t *testing.T) {
	cases := []struct {
		desc       string
		reasonCode byte
	}{
		{desc: "success", reasonCode: 0x00},
		{desc: "no matching subscribers", reasonCode: 0x10},
		{desc: "unspecified error", reasonCode: 0x80},
		{desc: "implementation specific error", reasonCode: 0x83},
		{desc: "not authorized", reasonCode: 0x87},
		{desc: "topic name invalid", reasonCode: 0x90},
		{desc: "packet ID in use", reasonCode: 0x91},
		{desc: "quota exceeded", reasonCode: 0x97},
		{desc: "payload format invalid", reasonCode: 0x99},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			pkt := &PubAck{
				FixedHeader: FixedHeader{PacketType: PubAckType},
				ID:          1,
				ReasonCode:  ptr(tc.reasonCode),
			}

			encoded := pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			require.NoError(t, err)

			puback, ok := decoded.(*PubAck)
			require.True(t, ok)
			assert.Equal(t, uint16(1), puback.ID)
			if puback.ReasonCode != nil {
				assert.Equal(t, tc.reasonCode, *puback.ReasonCode)
			}
		})
	}
}

func TestDisconnectV5ProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Disconnect
		wantErr bool
	}{
		{
			desc: "valid disconnect normal",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
				ReasonCode:  0x00,
			},
			wantErr: false,
		},
		{
			desc: "valid disconnect with will message",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
				ReasonCode:  0x04,
			},
			wantErr: false,
		},
		{
			desc: "valid disconnect with properties",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
				ReasonCode:  0x00,
				Properties: &DisconnectProperties{
					SessionExpiryInterval: ptr(uint32(0)),
					ReasonString:          "client shutdown",
					User:                  []User{{Key: "reason", Value: "maintenance"}},
					ServerReference:       "backup-server:1883",
				},
			},
			wantErr: false,
		},
		{
			desc: "valid disconnect unspecified error",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
				ReasonCode:  0x80,
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			disc, ok := decoded.(*Disconnect)
			require.True(t, ok)
			assert.Equal(t, tc.pkt.ReasonCode, disc.ReasonCode)
		})
	}
}

func TestAuthV5ProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Auth
		wantErr bool
	}{
		{
			desc: "valid auth success",
			pkt: &Auth{
				FixedHeader: FixedHeader{PacketType: AuthType},
				ReasonCode:  0x00,
				Properties: &AuthProperties{
					AuthMethod:   "SCRAM-SHA-256",
					AuthData:     []byte("auth-data"),
					ReasonString: "authentication successful",
					User:         []User{{Key: "session", Value: "info"}},
				},
			},
			wantErr: false,
		},
		{
			desc: "valid auth continue",
			pkt: &Auth{
				FixedHeader: FixedHeader{PacketType: AuthType},
				ReasonCode:  0x18,
				Properties: &AuthProperties{
					AuthMethod: "SCRAM-SHA-256",
					AuthData:   []byte("challenge-response"),
				},
			},
			wantErr: false,
		},
		{
			desc: "valid auth re-authenticate",
			pkt: &Auth{
				FixedHeader: FixedHeader{PacketType: AuthType},
				ReasonCode:  0x19,
				Properties: &AuthProperties{
					AuthMethod: "SCRAM-SHA-256",
					AuthData:   []byte("re-auth-data"),
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			auth, ok := decoded.(*Auth)
			require.True(t, ok)
			assert.Equal(t, tc.pkt.ReasonCode, auth.ReasonCode)
		})
	}
}

func TestUserProperties(t *testing.T) {
	cases := []struct {
		desc  string
		users []User
	}{
		{
			desc:  "single user property",
			users: []User{{Key: "key1", Value: "value1"}},
		},
		{
			desc: "multiple user properties",
			users: []User{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
				{Key: "key3", Value: "value3"},
			},
		},
		{
			desc:  "empty user properties",
			users: []User{},
		},
		{
			desc: "user properties with special characters",
			users: []User{
				{Key: "emoji", Value: "🔥"},
				{Key: "unicode", Value: "世界"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			pkt := &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				ClientID:        "test",
				Properties: &ConnectProperties{
					User: tc.users,
				},
			}

			encoded := pkt.Encode()
			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			require.NoError(t, err)

			connect, ok := decoded.(*Connect)
			require.True(t, ok)

			if len(tc.users) > 0 {
				require.NotNil(t, connect.Properties)
				assert.Equal(t, len(tc.users), len(connect.Properties.User))
			}
		})
	}
}

func TestMalformedV5Packets(t *testing.T) {
	cases := []struct {
		desc string
		data []byte
	}{
		{
			desc: "malformed packet empty data",
			data: []byte{},
		},
		{
			desc: "malformed packet incomplete fixed header",
			data: []byte{0x10},
		},
		{
			desc: "malformed connect packet truncated properties",
			data: []byte{0x10, 0x0A, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05},
		},
		{
			desc: "malformed publish packet no topic",
			data: []byte{0x30, 0x00},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.data)
			_, _, _, err := ReadPacket(reader)
			assert.Error(t, err)
		})
	}
}

func TestV5ControlPackets(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     ControlPacket
		wantErr bool
	}{
		{
			desc: "valid pingreq",
			pkt: &PingReq{
				FixedHeader: FixedHeader{PacketType: PingReqType},
			},
			wantErr: false,
		},
		{
			desc: "valid pingresp",
			pkt: &PingResp{
				FixedHeader: FixedHeader{PacketType: PingRespType},
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.pkt.Type(), decoded.Type())
		})
	}
}

func TestReadPacketEOF(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	_, _, _, err := ReadPacket(reader)
	assert.Equal(t, io.EOF, err)
}

func TestV5PacketBoundaryValues(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     ControlPacket
		wantErr bool
	}{
		{
			desc: "connect with max session expiry",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				ClientID:        "test",
				Properties: &ConnectProperties{
					SessionExpiryInterval: ptr(uint32(4294967295)),
				},
			},
			wantErr: false,
		},
		{
			desc: "publish with max message expiry",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   testTopic,
				Payload:     validPayload,
				Properties: &PublishProperties{
					MessageExpiry: ptr(uint32(4294967295)),
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, decoded)
		})
	}
}
