// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"fmt"
	"io"

	"github.com/absmach/mqtt/core/packets"
)

// PingResp represents the MQTT V3.1.1 PINGRESP packet.
type PingResp struct {
	packets.FixedHeader
}

func (p *PingResp) String() string {
	return fmt.Sprintf("%s\n", p.FixedHeader)
}

func (p *PingResp) Type() byte {
	return packets.PingRespType
}

func (p *PingResp) Encode() []byte {
	p.FixedHeader.RemainingLength = 0
	return p.FixedHeader.Encode()
}

func (p *PingResp) Unpack(r io.Reader) error {
	return nil
}

func (p *PingResp) Pack(w io.Writer) error {
	_, err := w.Write(p.Encode())
	return err
}

func (p *PingResp) Details() packets.Details {
	return packets.Details{Type: packets.PingRespType}
}
