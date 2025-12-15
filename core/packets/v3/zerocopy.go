package v3

import "github.com/absmach/mqtt/core/codec"

// UnpackBytes parses a Publish packet from a byte slice using zero-copy semantics.
// The TopicName will be allocated (Go strings are immutable), but Payload
// points directly into the original data slice.
// IMPORTANT: The packet's Payload is only valid as long as the original data is not modified.
func (pkt *Publish) UnpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)

	// Topic name (must allocate for string)
	topicBytes, err := r.ReadBytesNoCopy()
	if err != nil {
		return err
	}
	pkt.TopicName = string(topicBytes)

	// Packet ID (only for QoS > 0)
	if pkt.QoS > 0 {
		pkt.ID, err = r.ReadUint16()
		if err != nil {
			return err
		}
	}

	// Payload (zero-copy - points into original data)
	pkt.Payload = r.ReadRemaining()
	return nil
}

// UnpackBytes parses a Connect packet from a byte slice.
func (pkt *Connect) UnpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)
	var err error

	// Protocol name
	pkt.ProtocolName, err = r.ReadString()
	if err != nil {
		return err
	}

	// Protocol version
	pkt.ProtocolVersion, err = r.ReadByte()
	if err != nil {
		return err
	}

	// Connect flags
	opts, err := r.ReadByte()
	if err != nil {
		return err
	}
	pkt.UsernameFlag = (opts & (1 << 7)) > 0
	pkt.PasswordFlag = (opts & (1 << 6)) > 0
	pkt.WillRetain = (opts & (1 << 5)) > 0
	pkt.WillQoS = (opts >> 3) & 0x03
	pkt.WillFlag = (opts & (1 << 2)) > 0
	pkt.CleanSession = (opts & (1 << 1)) > 0
	pkt.ReservedBit = opts & 1

	// Keep alive
	pkt.KeepAlive, err = r.ReadUint16()
	if err != nil {
		return err
	}

	// Client ID
	pkt.ClientID, err = r.ReadString()
	if err != nil {
		return err
	}

	// Will topic and message
	if pkt.WillFlag {
		pkt.WillTopic, err = r.ReadString()
		if err != nil {
			return err
		}
		willMsg, err := r.ReadBytesNoCopy()
		if err != nil {
			return err
		}
		pkt.WillMessage = make([]byte, len(willMsg))
		copy(pkt.WillMessage, willMsg)
	}

	// Username
	if pkt.UsernameFlag {
		pkt.Username, err = r.ReadString()
		if err != nil {
			return err
		}
	}

	// Password
	if pkt.PasswordFlag {
		pwd, err := r.ReadBytesNoCopy()
		if err != nil {
			return err
		}
		pkt.Password = make([]byte, len(pwd))
		copy(pkt.Password, pwd)
	}

	return nil
}

// UnpackBytes parses a Subscribe packet from a byte slice.
func (pkt *Subscribe) UnpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)
	var err error

	// Packet ID
	pkt.ID, err = r.ReadUint16()
	if err != nil {
		return err
	}

	// Topics
	for r.Remaining() > 0 {
		topic, err := r.ReadString()
		if err != nil {
			return err
		}
		qos, err := r.ReadByte()
		if err != nil {
			return err
		}
		pkt.Topics = append(pkt.Topics, Topic{Name: topic, QoS: qos})
	}
	return nil
}

// ReadPacketBytes is a zero-copy alternative to ReadPacket.
// It reads a packet from data and returns the parsed packet along with
// the number of bytes consumed.
// The packet's payload data may point into the original slice.
func ReadPacketBytes(data []byte) (ControlPacket, int, error) {
	if len(data) < 2 {
		return nil, 0, codec.ErrBufferTooShort
	}

	// Fixed header
	var fh FixedHeader
	fh.PacketType = data[0] >> 4
	fh.Dup = (data[0]>>3)&0x01 > 0
	fh.QoS = (data[0] >> 1) & 0x03
	fh.Retain = data[0]&0x01 > 0

	// Remaining length (VBI)
	r := codec.NewZeroCopyReader(data[1:])
	remainingLen, err := r.ReadVBI()
	if err != nil {
		return nil, 0, err
	}
	fh.RemainingLength = remainingLen

	headerLen := 1 + r.Offset()
	totalLen := headerLen + remainingLen

	if len(data) < totalLen {
		return nil, 0, codec.ErrBufferTooShort
	}

	// Create packet based on type
	cp, err := NewControlPacketWithHeader(fh)
	if err != nil {
		return nil, 0, err
	}

	// Parse packet body using zero-copy where available
	packetData := data[headerLen:totalLen]

	switch pkt := cp.(type) {
	case *Publish:
		err = pkt.UnpackBytes(packetData)
	case *Connect:
		err = pkt.UnpackBytes(packetData)
	case *Subscribe:
		err = pkt.UnpackBytes(packetData)
	default:
		// Fall back to standard Unpack for other packet types
		err = cp.Unpack(codec.NewZeroCopyReader(packetData))
	}

	if err != nil {
		return nil, 0, err
	}

	return cp, totalLen, nil
}
