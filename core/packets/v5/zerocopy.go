package v5

import "github.com/dborovcanin/mqtt/codec"

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

	// Properties
	propLen, err := r.ReadVBI()
	if err != nil {
		return err
	}
	if propLen > 0 {
		propData, err := r.ReadN(propLen)
		if err != nil {
			return err
		}
		p := PublishProperties{}
		if err := p.unpackBytes(propData); err != nil {
			return err
		}
		pkt.Properties = &p
	}

	// Payload (zero-copy - points into original data)
	pkt.Payload = r.ReadRemaining()
	return nil
}

// unpackBytes parses PublishProperties from a byte slice.
func (p *PublishProperties) unpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)

	for r.Remaining() > 0 {
		prop, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch prop {
		case PayloadFormatProp:
			pf, err := r.ReadByte()
			if err != nil {
				return err
			}
			p.PayloadFormat = &pf
		case MessageExpiryProp:
			me, err := r.ReadUint32()
			if err != nil {
				return err
			}
			p.MessageExpiry = &me
		case ContentTypeProp:
			ct, err := r.ReadString()
			if err != nil {
				return err
			}
			p.ContentType = ct
		case TopicAliasProp:
			ta, err := r.ReadUint16()
			if err != nil {
				return err
			}
			p.TopicAlias = &ta
		case ResponseTopicProp:
			rt, err := r.ReadString()
			if err != nil {
				return err
			}
			p.ResponseTopic = rt
		case CorrelationDataProp:
			cd, err := r.ReadBytesNoCopy()
			if err != nil {
				return err
			}
			// Make a copy since CorrelationData may outlive the packet buffer
			p.CorrelationData = make([]byte, len(cd))
			copy(p.CorrelationData, cd)
		case UserProp:
			k, err := r.ReadString()
			if err != nil {
				return err
			}
			v, err := r.ReadString()
			if err != nil {
				return err
			}
			p.User = append(p.User, User{k, v})
		case SubscriptionIdentifierProp:
			si, err := r.ReadVBI()
			if err != nil {
				return err
			}
			p.SubscriptionID = &si
		}
	}
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
	pkt.ReservedBit = 1 & opts
	pkt.CleanStart = 1&(opts>>1) > 0
	pkt.WillFlag = 1&(opts>>2) > 0
	pkt.WillQoS = 3 & (opts >> 3)
	pkt.WillRetain = 1&(opts>>5) > 0
	pkt.PasswordFlag = 1&(opts>>6) > 0
	pkt.UsernameFlag = 1&(opts>>7) > 0

	// Keep alive
	pkt.KeepAlive, err = r.ReadUint16()
	if err != nil {
		return err
	}

	// Properties
	propLen, err := r.ReadVBI()
	if err != nil {
		return err
	}
	if propLen > 0 {
		propData, err := r.ReadN(propLen)
		if err != nil {
			return err
		}
		p := ConnectProperties{}
		if err := p.unpackBytes(propData); err != nil {
			return err
		}
		pkt.Properties = &p
	}

	// Client ID
	pkt.ClientID, err = r.ReadString()
	if err != nil {
		return err
	}

	// Will properties and payload
	if pkt.WillFlag {
		willPropLen, err := r.ReadVBI()
		if err != nil {
			return err
		}
		if willPropLen > 0 {
			willPropData, err := r.ReadN(willPropLen)
			if err != nil {
				return err
			}
			wp := WillProperties{}
			if err := wp.unpackBytes(willPropData); err != nil {
				return err
			}
			pkt.WillProperties = &wp
		}
		pkt.WillTopic, err = r.ReadString()
		if err != nil {
			return err
		}
		willPayload, err := r.ReadBytesNoCopy()
		if err != nil {
			return err
		}
		pkt.WillPayload = make([]byte, len(willPayload))
		copy(pkt.WillPayload, willPayload)
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

// unpackBytes parses ConnectProperties from a byte slice.
func (p *ConnectProperties) unpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)

	for r.Remaining() > 0 {
		prop, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch prop {
		case SessionExpiryIntervalProp:
			sei, err := r.ReadUint32()
			if err != nil {
				return err
			}
			p.SessionExpiryInterval = &sei
		case ReceiveMaximumProp:
			rm, err := r.ReadUint16()
			if err != nil {
				return err
			}
			p.ReceiveMaximum = &rm
		case MaximumPacketSizeProp:
			mps, err := r.ReadUint32()
			if err != nil {
				return err
			}
			p.MaximumPacketSize = &mps
		case TopicAliasMaximumProp:
			tam, err := r.ReadUint16()
			if err != nil {
				return err
			}
			p.TopicAliasMaximum = &tam
		case RequestResponseInfoProp:
			rri, err := r.ReadByte()
			if err != nil {
				return err
			}
			p.RequestResponseInfo = &rri
		case RequestProblemInfoProp:
			rpi, err := r.ReadByte()
			if err != nil {
				return err
			}
			p.RequestProblemInfo = &rpi
		case UserProp:
			k, err := r.ReadString()
			if err != nil {
				return err
			}
			v, err := r.ReadString()
			if err != nil {
				return err
			}
			p.User = append(p.User, User{k, v})
		case AuthMethodProp:
			p.AuthMethod, err = r.ReadString()
			if err != nil {
				return err
			}
		case AuthDataProp:
			ad, err := r.ReadBytesNoCopy()
			if err != nil {
				return err
			}
			p.AuthData = make([]byte, len(ad))
			copy(p.AuthData, ad)
		}
	}
	return nil
}

// unpackBytes parses WillProperties from a byte slice.
func (p *WillProperties) unpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)

	for r.Remaining() > 0 {
		prop, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch prop {
		case WillDelayIntervalProp:
			wdi, err := r.ReadUint32()
			if err != nil {
				return err
			}
			p.WillDelayInterval = &wdi
		case PayloadFormatProp:
			pf, err := r.ReadByte()
			if err != nil {
				return err
			}
			p.PayloadFormat = &pf
		case MessageExpiryProp:
			me, err := r.ReadUint32()
			if err != nil {
				return err
			}
			p.MessageExpiry = &me
		case ContentTypeProp:
			p.ContentType, err = r.ReadString()
			if err != nil {
				return err
			}
		case ResponseTopicProp:
			p.ResponseTopic, err = r.ReadString()
			if err != nil {
				return err
			}
		case CorrelationDataProp:
			cd, err := r.ReadBytesNoCopy()
			if err != nil {
				return err
			}
			p.CorrelationData = make([]byte, len(cd))
			copy(p.CorrelationData, cd)
		case UserProp:
			k, err := r.ReadString()
			if err != nil {
				return err
			}
			v, err := r.ReadString()
			if err != nil {
				return err
			}
			p.User = append(p.User, User{k, v})
		}
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

	// Properties
	propLen, err := r.ReadVBI()
	if err != nil {
		return err
	}
	if propLen > 0 {
		propData, err := r.ReadN(propLen)
		if err != nil {
			return err
		}
		p := SubscribeProperties{}
		if err := p.unpackBytes(propData); err != nil {
			return err
		}
		pkt.Properties = &p
	}

	// Subscription options
	for r.Remaining() > 0 {
		topic, err := r.ReadString()
		if err != nil {
			return err
		}
		flags, err := r.ReadByte()
		if err != nil {
			return err
		}
		opt := SubOption{
			Topic:  topic,
			MaxQoS: flags & 0x03,
		}
		noLocal := (flags & (1 << 2)) != 0
		retainAsPublished := (flags & (1 << 3)) != 0
		rh := (flags >> 4) & 0x03
		opt.NoLocal = &noLocal
		opt.RetainAsPublished = &retainAsPublished
		opt.RetainHandling = &rh
		pkt.Opts = append(pkt.Opts, opt)
	}
	return nil
}

// unpackBytes parses SubscribeProperties from a byte slice.
func (p *SubscribeProperties) unpackBytes(data []byte) error {
	r := codec.NewZeroCopyReader(data)

	for r.Remaining() > 0 {
		prop, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch prop {
		case SubscriptionIdentifierProp:
			si, err := r.ReadVBI()
			if err != nil {
				return err
			}
			p.SubscriptionIdentifier = &si
		case UserProp:
			k, err := r.ReadString()
			if err != nil {
				return err
			}
			v, err := r.ReadString()
			if err != nil {
				return err
			}
			p.User = append(p.User, User{k, v})
		}
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
