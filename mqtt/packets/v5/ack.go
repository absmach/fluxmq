// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import "github.com/absmach/fluxmq/mqtt/packets"

// ackForm selects how much of an acknowledgement's variable header is written
// when it carries nothing beyond the packet identifier.
type ackForm uint8

const (
	// ackFull always writes the reason code and the property length.
	ackFull ackForm = iota
	// ackShortest omits a success reason code and an empty property length
	// [MQTT-3.4.2.1], so the same acknowledgement always has one encoding.
	ackShortest
)

// appendAck appends a PUBACK, PUBREC, PUBREL or PUBCOMP to dst.
func appendAck(dst []byte, fh *packets.FixedHeader, id uint16, reason *byte, props *BasicProperties, form ackForm) []byte {
	var encodedProps []byte
	if props != nil {
		encodedProps = props.Encode()
	}
	var code byte
	if reason != nil {
		code = *reason
	}

	remaining := 2
	switch {
	case form == ackFull, len(encodedProps) > 0:
		remaining += 1 + vbiLen(len(encodedProps)) + len(encodedProps)
	case code != 0:
		remaining++
	}
	fh.RemainingLength = remaining

	dst = fh.AppendTo(dst)
	dst = appendUint16(dst, id)
	if remaining > 2 {
		dst = append(dst, code)
	}
	if remaining > 3 {
		dst = appendVBI(dst, len(encodedProps))
		dst = append(dst, encodedProps...)
	}
	return dst
}
