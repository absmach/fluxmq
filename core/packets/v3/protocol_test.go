package v3_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	. "github.com/absmach/mqtt/core/packets/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	validClientID   = "test-client-123"
	longClientID    = strings.Repeat("a", 65535)
	maxPacketID     = uint16(65535)
	testTopic       = "test/topic"
	validPayload    = []byte("test payload")
	maxPayload      = make([]byte, 65535)
	emptyPayload    = []byte{}
	wildcardTopics  = []string{"test/+/topic", "test/#", "+/+/+", "#"}
)

func TestConnectProtocolCompliance(t *testing.T) {
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
				ProtocolVersion: 4,
				CleanSession:    true,
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
				WillMessage:     []byte("gone"),
			},
			wantErr: false,
		},
		{
			desc: "valid connect minimal",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       0,
				ClientID:        validClientID,
			},
			wantErr: false,
		},
		{
			desc: "valid connect with empty client ID and clean session",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       60,
				ClientID:        "",
			},
			wantErr: false,
		},
		{
			desc: "valid connect with long client ID",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       60,
				ClientID:        longClientID,
			},
			wantErr: false,
		},
		{
			desc: "valid connect with will QoS 0",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       60,
				ClientID:        validClientID,
				WillFlag:        true,
				WillQoS:         0,
				WillTopic:       "will",
				WillMessage:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid connect with will QoS 2",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       60,
				ClientID:        validClientID,
				WillFlag:        true,
				WillQoS:         2,
				WillTopic:       "will",
				WillMessage:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid connect with max keep alive",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       65535,
				ClientID:        validClientID,
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
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
			assert.Equal(t, tc.pkt.CleanSession, connect.CleanSession)
			assert.Equal(t, tc.pkt.KeepAlive, connect.KeepAlive)
			assert.Equal(t, tc.pkt.Username, connect.Username)
			assert.Equal(t, tc.pkt.Password, connect.Password)
			assert.Equal(t, tc.pkt.WillTopic, connect.WillTopic)
			assert.Equal(t, tc.pkt.WillMessage, connect.WillMessage)
		})
	}
}

func TestPublishProtocolCompliance(t *testing.T) {
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
			desc: "valid publish QoS 1 with packet ID",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
				TopicName:   testTopic,
				ID:          1,
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish QoS 2 with packet ID",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 2},
				TopicName:   testTopic,
				ID:          maxPacketID,
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with retain flag",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0, Retain: true},
				TopicName:   testTopic,
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with dup flag QoS 1",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1, Dup: true},
				TopicName:   testTopic,
				ID:          100,
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with empty payload",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   testTopic,
				Payload:     emptyPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with max payload",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   testTopic,
				Payload:     maxPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with multilevel topic",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   "level1/level2/level3/level4",
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "valid publish with special characters in topic",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   "test-topic_123.abc",
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
			decoded, err := ReadPacket(reader)
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
			assert.Equal(t, tc.pkt.Dup, pub.Dup)
			if tc.pkt.QoS > 0 {
				assert.Equal(t, tc.pkt.ID, pub.ID)
			}
			assert.Equal(t, tc.pkt.Payload, pub.Payload)
		})
	}
}

func TestSubscribeProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Subscribe
		wantErr bool
	}{
		{
			desc: "valid subscribe single topic QoS 0",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Topics:      []Topic{{Name: testTopic, QoS: 0}},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe single topic QoS 1",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Topics:      []Topic{{Name: testTopic, QoS: 1}},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe single topic QoS 2",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Topics:      []Topic{{Name: testTopic, QoS: 2}},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe multiple topics",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          100,
				Topics: []Topic{
					{Name: "topic/one", QoS: 0},
					{Name: "topic/two", QoS: 1},
					{Name: "topic/three", QoS: 2},
				},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe with wildcard single level",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Topics:      []Topic{{Name: "test/+/sensor", QoS: 1}},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe with wildcard multilevel",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Topics:      []Topic{{Name: "test/#", QoS: 1}},
			},
			wantErr: false,
		},
		{
			desc: "valid subscribe with max packet ID",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          maxPacketID,
				Topics:      []Topic{{Name: testTopic, QoS: 1}},
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			sub, ok := decoded.(*Subscribe)
			require.True(t, ok)

			assert.Equal(t, tc.pkt.ID, sub.ID)
			assert.Equal(t, len(tc.pkt.Topics), len(sub.Topics))
			for i, topic := range tc.pkt.Topics {
				assert.Equal(t, topic.Name, sub.Topics[i].Name)
				assert.Equal(t, topic.QoS, sub.Topics[i].QoS)
			}
		})
	}
}

func TestUnsubscribeProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     *Unsubscribe
		wantErr bool
	}{
		{
			desc: "valid unsubscribe single topic",
			pkt: &Unsubscribe{
				FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1},
				ID:          1,
				Topics:      []string{testTopic},
			},
			wantErr: false,
		},
		{
			desc: "valid unsubscribe multiple topics",
			pkt: &Unsubscribe{
				FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1},
				ID:          100,
				Topics:      []string{"topic/one", "topic/two", "topic/three"},
			},
			wantErr: false,
		},
		{
			desc: "valid unsubscribe with wildcards",
			pkt: &Unsubscribe{
				FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1},
				ID:          1,
				Topics:      wildcardTopics,
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			unsub, ok := decoded.(*Unsubscribe)
			require.True(t, ok)

			assert.Equal(t, tc.pkt.ID, unsub.ID)
			assert.Equal(t, tc.pkt.Topics, unsub.Topics)
		})
	}
}

func TestAckPacketsProtocolCompliance(t *testing.T) {
	cases := []struct {
		desc    string
		create  func(uint16) ControlPacket
		wantErr bool
	}{
		{
			desc: "valid puback",
			create: func(id uint16) ControlPacket {
				return &PubAck{
					FixedHeader: FixedHeader{PacketType: PubAckType},
					ID:          id,
				}
			},
			wantErr: false,
		},
		{
			desc: "valid pubrec",
			create: func(id uint16) ControlPacket {
				return &PubRec{
					FixedHeader: FixedHeader{PacketType: PubRecType},
					ID:          id,
				}
			},
			wantErr: false,
		},
		{
			desc: "valid pubrel",
			create: func(id uint16) ControlPacket {
				return &PubRel{
					FixedHeader: FixedHeader{PacketType: PubRelType, QoS: 1},
					ID:          id,
				}
			},
			wantErr: false,
		},
		{
			desc: "valid pubcomp",
			create: func(id uint16) ControlPacket {
				return &PubComp{
					FixedHeader: FixedHeader{PacketType: PubCompType},
					ID:          id,
				}
			},
			wantErr: false,
		},
	}

	testIDs := []uint16{1, 100, 32768, maxPacketID}

	for _, tc := range cases {
		for _, id := range testIDs {
			t.Run(tc.desc+" with ID "+string(rune(id)), func(t *testing.T) {
				pkt := tc.create(id)
				encoded := pkt.Encode()
				require.NotEmpty(t, encoded)

				reader := bytes.NewReader(encoded)
				decoded, err := ReadPacket(reader)
				if tc.wantErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.NotNil(t, decoded)

				if detailer, ok := decoded.(Detailer); ok {
					details := detailer.Details()
					assert.Equal(t, id, details.ID)
				}
			})
		}
	}
}

func TestControlPacketsProtocolCompliance(t *testing.T) {
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
		{
			desc: "valid disconnect",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.pkt.Type(), decoded.Type())
		})
	}
}

func TestMalformedPackets(t *testing.T) {
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
			desc: "malformed packet invalid remaining length encoding",
			data: []byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			desc: "malformed connect packet truncated",
			data: []byte{0x10, 0x05, 0x00, 0x04, 0x4D},
		},
		{
			desc: "malformed publish packet no topic",
			data: []byte{0x30, 0x00},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.data)
			_, err := ReadPacket(reader)
			assert.Error(t, err)
		})
	}
}

func TestPacketBoundaryValues(t *testing.T) {
	cases := []struct {
		desc    string
		pkt     ControlPacket
		wantErr bool
	}{
		{
			desc: "publish with packet ID 0 (invalid for QoS > 0)",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
				TopicName:   testTopic,
				ID:          0,
				Payload:     validPayload,
			},
			wantErr: false,
		},
		{
			desc: "subscribe with packet ID 0 (technically invalid but encoded)",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          0,
				Topics:      []Topic{{Name: testTopic, QoS: 1}},
			},
			wantErr: false,
		},
		{
			desc: "connect with keep alive 0 (disable)",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       0,
				ClientID:        validClientID,
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := tc.pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, decoded)
		})
	}
}

func TestConnAckReturnCodes(t *testing.T) {
	cases := []struct {
		desc           string
		returnCode     byte
		sessionPresent bool
	}{
		{
			desc:           "connection accepted",
			returnCode:     0x00,
			sessionPresent: false,
		},
		{
			desc:           "connection accepted with session",
			returnCode:     0x00,
			sessionPresent: true,
		},
		{
			desc:           "unacceptable protocol version",
			returnCode:     0x01,
			sessionPresent: false,
		},
		{
			desc:           "identifier rejected",
			returnCode:     0x02,
			sessionPresent: false,
		},
		{
			desc:           "server unavailable",
			returnCode:     0x03,
			sessionPresent: false,
		},
		{
			desc:           "bad username or password",
			returnCode:     0x04,
			sessionPresent: false,
		},
		{
			desc:           "not authorized",
			returnCode:     0x05,
			sessionPresent: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			pkt := &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: tc.sessionPresent,
				ReturnCode:     tc.returnCode,
			}

			encoded := pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			require.NoError(t, err)

			connack, ok := decoded.(*ConnAck)
			require.True(t, ok)
			assert.Equal(t, tc.returnCode, connack.ReturnCode)
			assert.Equal(t, tc.sessionPresent, connack.SessionPresent)
		})
	}
}

func TestSubAckReturnCodes(t *testing.T) {
	cases := []struct {
		desc        string
		returnCodes []byte
	}{
		{
			desc:        "single success QoS 0",
			returnCodes: []byte{0x00},
		},
		{
			desc:        "single success QoS 1",
			returnCodes: []byte{0x01},
		},
		{
			desc:        "single success QoS 2",
			returnCodes: []byte{0x02},
		},
		{
			desc:        "single failure",
			returnCodes: []byte{0x80},
		},
		{
			desc:        "multiple mixed results",
			returnCodes: []byte{0x00, 0x01, 0x02, 0x80},
		},
		{
			desc:        "all failures",
			returnCodes: []byte{0x80, 0x80, 0x80},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			pkt := &SubAck{
				FixedHeader: FixedHeader{PacketType: SubAckType},
				ID:          1,
				ReturnCodes: tc.returnCodes,
			}

			encoded := pkt.Encode()
			require.NotEmpty(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			require.NoError(t, err)

			suback, ok := decoded.(*SubAck)
			require.True(t, ok)
			assert.Equal(t, tc.returnCodes, suback.ReturnCodes)
		})
	}
}

func TestReadPacketEOF(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	_, err := ReadPacket(reader)
	assert.Equal(t, io.EOF, err)
}

func TestPacketTypeDetection(t *testing.T) {
	cases := []struct {
		desc       string
		packetType byte
		create     func() ControlPacket
	}{
		{
			desc:       "detect connect packet",
			packetType: ConnectType,
			create: func() ControlPacket {
				return &Connect{
					FixedHeader:     FixedHeader{PacketType: ConnectType},
					ProtocolName:    "MQTT",
					ProtocolVersion: 4,
					ClientID:        "test",
				}
			},
		},
		{
			desc:       "detect connack packet",
			packetType: ConnAckType,
			create: func() ControlPacket {
				return &ConnAck{
					FixedHeader: FixedHeader{PacketType: ConnAckType},
					ReturnCode:  0,
				}
			},
		},
		{
			desc:       "detect publish packet",
			packetType: PublishType,
			create: func() ControlPacket {
				return &Publish{
					FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
					TopicName:   "test",
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			pkt := tc.create()
			encoded := pkt.Encode()
			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
			require.NoError(t, err)
			assert.Equal(t, tc.packetType, decoded.Type())
		})
	}
}
