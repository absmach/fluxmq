// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"bytes"
	"io"
)

// Frame represents a single AMQP frame.
type Frame struct {
	Type    byte
	Channel uint16
	Payload []byte
}

// ReadFrame reads a single frame from the reader.
func ReadFrame(r io.Reader) (*Frame, error) {
	frameType, err := ReadOctet(r)
	if err != nil {
		return nil, err
	}

	channel, err := ReadShort(r)
	if err != nil {
		return nil, err
	}

	size, err := ReadLong(r)
	if err != nil {
		return nil, err
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	frameEnd, err := ReadOctet(r)
	if err != nil {
		return nil, err
	}

	if frameEnd != FrameEnd {
		return nil, NewErr(FrameError, "malformed frame: incorrect frame-end marker", nil)
	}

	return &Frame{
		Type:    frameType,
		Channel: channel,
		Payload: payload,
	}, nil
}

// WriteFrame writes a single frame to the writer.
func (f *Frame) WriteFrame(w io.Writer) error {
	if err := WriteOctet(w, f.Type); err != nil {
		return err
	}
	if err := WriteShort(w, f.Channel); err != nil {
		return err
	}
	if err := WriteLong(w, uint32(len(f.Payload))); err != nil {
		return err
	}
	if _, err := w.Write(f.Payload); err != nil {
		return err
	}
	return WriteOctet(w, FrameEnd)
}

// Decode decodes the frame payload into a specific method, content header, or heartbeat.
func (f *Frame) Decode() (interface{}, error) {
	switch f.Type {
	case FrameMethod:
		return f.decodeMethod()
	case FrameHeader:
		b := bytes.NewReader(f.Payload)
		return ReadContentHeader(b)
	case FrameBody:
		return f.Payload, nil
	case FrameHeartbeat:
		return nil, nil
	default:
		return nil, NewErr(FrameError, "unknown frame type", nil)
	}
}

func decodeMethodSwitch(b *bytes.Reader, classID, methodID uint16) (interface{}, error) {
	type methodReader interface {
		Read(*bytes.Reader) error
	}

	var m methodReader
	switch classID {
	case ClassConnection:
		switch methodID {
		case MethodConnectionStart:
			m = &ConnectionStart{}
		case MethodConnectionStartOk:
			m = &ConnectionStartOk{}
		case MethodConnectionSecure:
			m = &ConnectionSecure{}
		case MethodConnectionSecureOk:
			m = &ConnectionSecureOk{}
		case MethodConnectionTune:
			m = &ConnectionTune{}
		case MethodConnectionTuneOk:
			m = &ConnectionTuneOk{}
		case MethodConnectionOpen:
			m = &ConnectionOpen{}
		case MethodConnectionOpenOk:
			m = &ConnectionOpenOk{}
		case MethodConnectionClose:
			m = &ConnectionClose{}
		case MethodConnectionCloseOk:
			m = &ConnectionCloseOk{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Connection class", nil)
		}
	case ClassChannel:
		switch methodID {
		case MethodChannelOpen:
			m = &ChannelOpen{}
		case MethodChannelOpenOk:
			m = &ChannelOpenOk{}
		case MethodChannelFlow:
			m = &ChannelFlow{}
		case MethodChannelFlowOk:
			m = &ChannelFlowOk{}
		case MethodChannelClose:
			m = &ChannelClose{}
		case MethodChannelCloseOk:
			m = &ChannelCloseOk{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Channel class", nil)
		}
	case ClassExchange:
		switch methodID {
		case MethodExchangeDeclare:
			m = &ExchangeDeclare{}
		case MethodExchangeDeclareOk:
			m = &ExchangeDeclareOk{}
		case MethodExchangeDelete:
			m = &ExchangeDelete{}
		case MethodExchangeDeleteOk:
			m = &ExchangeDeleteOk{}
		case MethodExchangeBind:
			m = &ExchangeBind{}
		case MethodExchangeBindOk:
			m = &ExchangeBindOk{}
		case MethodExchangeUnbind:
			m = &ExchangeUnbind{}
		case MethodExchangeUnbindOk:
			m = &ExchangeUnbindOk{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Exchange class", nil)
		}
	case ClassQueue:
		switch methodID {
		case MethodQueueDeclare:
			m = &QueueDeclare{}
		case MethodQueueDeclareOk:
			m = &QueueDeclareOk{}
		case MethodQueueBind:
			m = &QueueBind{}
		case MethodQueueBindOk:
			m = &QueueBindOk{}
		case MethodQueuePurge:
			m = &QueuePurge{}
		case MethodQueuePurgeOk:
			m = &QueuePurgeOk{}
		case MethodQueueDelete:
			m = &QueueDelete{}
		case MethodQueueDeleteOk:
			m = &QueueDeleteOk{}
		case MethodQueueUnbind:
			m = &QueueUnbind{}
		case MethodQueueUnbindOk:
			m = &QueueUnbindOk{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Queue class", nil)
		}
	case ClassBasic:
		switch methodID {
		case MethodBasicQos:
			m = &BasicQos{}
		case MethodBasicQosOk:
			m = &BasicQosOk{}
		case MethodBasicConsume:
			m = &BasicConsume{}
		case MethodBasicConsumeOk:
			m = &BasicConsumeOk{}
		case MethodBasicCancel:
			m = &BasicCancel{}
		case MethodBasicCancelOk:
			m = &BasicCancelOk{}
		case MethodBasicPublish:
			m = &BasicPublish{}
		case MethodBasicReturn:
			m = &BasicReturn{}
		case MethodBasicDeliver:
			m = &BasicDeliver{}
		case MethodBasicGet:
			m = &BasicGet{}
		case MethodBasicGetOk:
			m = &BasicGetOk{}
		case MethodBasicGetEmpty:
			m = &BasicGetEmpty{}
		case MethodBasicAck:
			m = &BasicAck{}
		case MethodBasicReject:
			m = &BasicReject{}
		case MethodBasicRecoverAsync:
			m = &BasicRecoverAsync{}
		case MethodBasicRecover:
			m = &BasicRecover{}
		case MethodBasicRecoverOk:
			m = &BasicRecoverOk{}
		case MethodBasicNack:
			m = &BasicNack{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Basic class", nil)
		}
	case ClassConfirm:
		switch methodID {
		case MethodConfirmSelect:
			m = &ConfirmSelect{}
		case MethodConfirmSelectOk:
			m = &ConfirmSelectOk{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Confirm class", nil)
		}
	case ClassTx:
		switch methodID {
		case MethodTxSelect:
			m = &TxSelect{}
		case MethodTxSelectOk:
			m = &TxSelectOk{}
		case MethodTxCommit:
			m = &TxCommit{}
		case MethodTxCommitOk:
			m = &TxCommitOk{}
		case MethodTxRollback:
			m = &TxRollback{}
		case MethodTxRollbackOk:
			m = &TxRollbackOk{}
		default:
			return nil, NewErr(FrameError, "unknown method ID for Tx class", nil)
		}
	default:
		return nil, NewErr(FrameError, "unknown class ID", nil)
	}

	if err := m.Read(b); err != nil {
		return nil, err
	}
	return m, nil
}

func (f *Frame) decodeMethod() (interface{}, error) {
	b := bytes.NewReader(f.Payload)

	classID, err := ReadShort(b)
	if err != nil {
		return nil, err
	}
	methodID, err := ReadShort(b)
	if err != nil {
		return nil, err
	}

	return decodeMethodSwitch(b, classID, methodID)
}
