// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAckEncodings pins the wire form of every acknowledgement shape: PUBACK
// always carries its reason code and property length, the others take the
// shortest form, and Encode, EncodeTo and Pack all produce the same bytes.
func TestAckEncodings(t *testing.T) {
	want := map[string]string{
		"puback/nil/noprops":      "400412340000",
		"pubrec/nil/noprops":      "50021234",
		"pubrel/nil/noprops":      "62021234",
		"pubcomp/nil/noprops":     "70021234",
		"puback/nil/emptyprops":   "400412340000",
		"pubrec/nil/emptyprops":   "50021234",
		"pubrel/nil/emptyprops":   "62021234",
		"pubcomp/nil/emptyprops":  "70021234",
		"puback/nil/props":        "400c123400081f000571756f7461",
		"pubrec/nil/props":        "500c123400081f000571756f7461",
		"pubrel/nil/props":        "620c123400081f000571756f7461",
		"pubcomp/nil/props":       "700c123400081f000571756f7461",
		"puback/zero/noprops":     "400412340000",
		"pubrec/zero/noprops":     "50021234",
		"pubrel/zero/noprops":     "62021234",
		"pubcomp/zero/noprops":    "70021234",
		"puback/zero/emptyprops":  "400412340000",
		"pubrec/zero/emptyprops":  "50021234",
		"pubrel/zero/emptyprops":  "62021234",
		"pubcomp/zero/emptyprops": "70021234",
		"puback/zero/props":       "400c123400081f000571756f7461",
		"pubrec/zero/props":       "500c123400081f000571756f7461",
		"pubrel/zero/props":       "620c123400081f000571756f7461",
		"pubcomp/zero/props":      "700c123400081f000571756f7461",
		"puback/fail/noprops":     "400412348700",
		"pubrec/fail/noprops":     "5003123487",
		"pubrel/fail/noprops":     "6203123487",
		"pubcomp/fail/noprops":    "7003123487",
		"puback/fail/emptyprops":  "400412348700",
		"pubrec/fail/emptyprops":  "5003123487",
		"pubrel/fail/emptyprops":  "6203123487",
		"pubcomp/fail/emptyprops": "7003123487",
		"puback/fail/props":       "400c123487081f000571756f7461",
		"pubrec/fail/props":       "500c123487081f000571756f7461",
		"pubrel/fail/props":       "620c123487081f000571756f7461",
		"pubcomp/fail/props":      "700c123487081f000571756f7461",
	}

	zero, fail := byte(0), byte(0x87)
	empty := &BasicProperties{}
	withReason := &BasicProperties{ReasonString: "quota"}
	reasons := []struct {
		name string
		code *byte
	}{{"nil", nil}, {"zero", &zero}, {"fail", &fail}}
	properties := []struct {
		name  string
		props *BasicProperties
	}{{"noprops", nil}, {"emptyprops", empty}, {"props", withReason}}

	for _, rc := range reasons {
		for _, pr := range properties {
			acks := map[string]interface {
				Encode() []byte
				EncodeTo(dst []byte) []byte
			}{
				"puback":  &PubAck{FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType}, ID: 0x1234, ReasonCode: rc.code, Properties: pr.props},
				"pubrec":  &PubRec{FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType}, ID: 0x1234, ReasonCode: rc.code, Properties: pr.props},
				"pubrel":  &PubRel{FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1}, ID: 0x1234, ReasonCode: rc.code, Properties: pr.props},
				"pubcomp": &PubComp{FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType}, ID: 0x1234, ReasonCode: rc.code, Properties: pr.props},
			}
			for kind, ack := range acks {
				name := kind + "/" + rc.name + "/" + pr.name
				t.Run(name, func(t *testing.T) {
					expected, ok := want[name]
					require.True(t, ok, "no expected encoding")
					assert.Equal(t, expected, hex.EncodeToString(ack.Encode()))

					prefix := []byte{0xAA}
					assert.Equal(t, expected, hex.EncodeToString(ack.EncodeTo(prefix)[1:]), "EncodeTo must append")

					var buf bytes.Buffer
					require.NoError(t, ack.(packets.ControlPacket).Pack(&buf))
					assert.Equal(t, expected, hex.EncodeToString(buf.Bytes()))
				})
			}
		}
	}
}
