// Package v5 provides MQTT 5.0 packet implementations.
package v5

import (
	"bytes"
	"errors"
	"io"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/packets/codec"
)

// ErrFailRemaining indicates remaining data does not match the size of sent data.
var ErrFailRemaining = errors.New("remaining data length does not match data size")

// ReadPacket reads an MQTT 5.0 packet from the reader.
// Returns the packet, encoded fixed header, packet body bytes, and any error.
func ReadPacket(r io.Reader) (packets.ControlPacket, []byte, []byte, error) {
	var fh packets.FixedHeader
	b := make([]byte, 1)

	_, err := io.ReadFull(r, b)
	if err != nil {
		return nil, nil, nil, err
	}

	err = fh.Decode(b[0], r)
	if err != nil {
		return nil, nil, nil, err
	}

	cp, err := NewPacketWithHeader(fh)
	if err != nil {
		return nil, nil, nil, err
	}

	packetBytes := make([]byte, fh.RemainingLength)
	n, err := io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, nil, nil, err
	}
	if n != fh.RemainingLength {
		return nil, nil, nil, ErrFailRemaining
	}

	err = cp.Unpack(bytes.NewReader(packetBytes))
	return cp, fh.Encode(), packetBytes, err
}

// ReadPacketBytes reads an MQTT 5.0 packet from a byte slice (zero-copy where possible).
// Returns the packet and number of bytes consumed.
func ReadPacketBytes(data []byte) (packets.ControlPacket, int, error) {
	var fh packets.FixedHeader
	headerLen, err := fh.DecodeFromBytes(data)
	if err != nil {
		return nil, 0, err
	}

	totalLen := headerLen + fh.RemainingLength
	if len(data) < totalLen {
		return nil, 0, codec.ErrBufferTooShort
	}

	cp, err := NewPacketWithHeader(fh)
	if err != nil {
		return nil, 0, err
	}

	packetData := data[headerLen:totalLen]

	// Use zero-copy UnpackBytes where available
	switch pkt := cp.(type) {
	case *packets.Publish:
		err = pkt.UnpackBytes(packetData)
	case *packets.Connect:
		err = pkt.UnpackBytes(packetData)
	case *packets.Subscribe:
		err = pkt.UnpackBytes(packetData)
	default:
		err = cp.Unpack(bytes.NewReader(packetData))
	}

	if err != nil {
		return nil, 0, err
	}
	return cp, totalLen, nil
}

// NewPacket creates a new packet of the specified type.
func NewPacket(packetType byte) packets.ControlPacket {
	return packets.NewControlPacket(packetType)
}

// NewPacketWithHeader creates a new packet with the given fixed header.
func NewPacketWithHeader(fh packets.FixedHeader) (packets.ControlPacket, error) {
	return packets.NewControlPacketWithHeader(fh)
}
