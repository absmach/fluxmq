// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"io"

	"github.com/absmach/fluxmq/mqtt/packets"
)

// PingResp is an internal representation of the fields of the PINGRESP MQTT packet.
type PingResp struct {
	packets.FixedHeader
}

func (pkt *PingResp) String() string {
	return pkt.FixedHeader.String()
}

// Type returns the packet type.
func (pkt *PingResp) Type() byte {
	return PingRespType
}

func (pkt *PingResp) Encode() []byte {
	return pkt.FixedHeader.Encode()
}

func (pkt *PingResp) Pack(w io.Writer) error {
	// No need for an extra function call of pkt.Encode().
	ret := pkt.FixedHeader.Encode()
	_, err := w.Write(ret)

	return err
}

func (pkt *PingResp) Unpack(r io.Reader) error {
	return nil
}

func (pkt *PingResp) Details() Details {
	return Details{Type: PingRespType, ID: 0, QoS: 0}
}
