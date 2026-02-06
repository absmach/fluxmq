// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"bytes"
	"io"
)

// Class and Method IDs
const (
	ClassConnection = 10
)

const (
	MethodConnectionStart    = 10
	MethodConnectionStartOk  = 11
	MethodConnectionSecure   = 20
	MethodConnectionSecureOk = 21
	MethodConnectionTune     = 30
	MethodConnectionTuneOk   = 31
	MethodConnectionOpen     = 40
	MethodConnectionOpenOk   = 41
	MethodConnectionClose    = 50
	MethodConnectionCloseOk  = 51
)

// ConnectionStart is the initial method sent by the server.
type ConnectionStart struct {
	VersionMajor     byte
	VersionMinor     byte
	ServerProperties map[string]interface{} // Actually a table
	Mechanisms       string                 // Long string
	Locales          string                 // Long string
}

func (m *ConnectionStart) Read(r *bytes.Reader) (err error) {
	if m.VersionMajor, err = ReadOctet(r); err != nil {
		return err
	}
	if m.VersionMinor, err = ReadOctet(r); err != nil {
		return err
	}
	if m.ServerProperties, err = ReadTable(r); err != nil {
		return err
	}
	if m.Mechanisms, err = ReadLongStr(r); err != nil {
		return err
	}
	if m.Locales, err = ReadLongStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionStart) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionStart); err != nil {
		return err
	}
	if err := WriteOctet(w, m.VersionMajor); err != nil {
		return err
	}
	if err := WriteOctet(w, m.VersionMinor); err != nil {
		return err
	}
	if err := WriteTable(w, m.ServerProperties); err != nil {
		return err
	}
	if err := WriteLongStr(w, m.Mechanisms); err != nil {
		return err
	}
	return WriteLongStr(w, m.Locales)
}

// ConnectionStartOk is sent by the client in response to ConnectionStart.
type ConnectionStartOk struct {
	ClientProperties map[string]interface{} // Actually a table
	Mechanism        string                 // Short string
	Response         string                 // Long string
	Locale           string                 // Short string
}

func (m *ConnectionStartOk) Read(r *bytes.Reader) (err error) {
	if m.ClientProperties, err = ReadTable(r); err != nil {
		return err
	}
	if m.Mechanism, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Response, err = ReadLongStr(r); err != nil {
		return err
	}
	if m.Locale, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionStartOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionStartOk); err != nil {
		return err
	}
	if err := WriteTable(w, m.ClientProperties); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Mechanism); err != nil {
		return err
	}
	if err := WriteLongStr(w, m.Response); err != nil {
		return err
	}
	return WriteShortStr(w, m.Locale)
}

// ConnectionSecure is sent by the server to challenge the client.
type ConnectionSecure struct {
	Challenge string // Long string
}

func (m *ConnectionSecure) Read(r *bytes.Reader) (err error) {
	if m.Challenge, err = ReadLongStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionSecure) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionSecure); err != nil {
		return err
	}
	return WriteLongStr(w, m.Challenge)
}

// ConnectionSecureOk is sent by the client in response to ConnectionSecure.
type ConnectionSecureOk struct {
	Response string // Long string
}

func (m *ConnectionSecureOk) Read(r *bytes.Reader) (err error) {
	if m.Response, err = ReadLongStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionSecureOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionSecureOk); err != nil {
		return err
	}
	return WriteLongStr(w, m.Response)
}

// ConnectionTune is sent by the server to propose connection parameters.
type ConnectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (m *ConnectionTune) Read(r *bytes.Reader) (err error) {
	if m.ChannelMax, err = ReadShort(r); err != nil {
		return err
	}
	if m.FrameMax, err = ReadLong(r); err != nil {
		return err
	}
	if m.Heartbeat, err = ReadShort(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionTune) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionTune); err != nil {
		return err
	}
	if err := WriteShort(w, m.ChannelMax); err != nil {
		return err
	}
	if err := WriteLong(w, m.FrameMax); err != nil {
		return err
	}
	return WriteShort(w, m.Heartbeat)
}

// ConnectionTuneOk is sent by the client to agree on connection parameters.
type ConnectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (m *ConnectionTuneOk) Read(r *bytes.Reader) (err error) {
	if m.ChannelMax, err = ReadShort(r); err != nil {
		return err
	}
	if m.FrameMax, err = ReadLong(r); err != nil {
		return err
	}
	if m.Heartbeat, err = ReadShort(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionTuneOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionTuneOk); err != nil {
		return err
	}
	if err := WriteShort(w, m.ChannelMax); err != nil {
		return err
	}
	if err := WriteLong(w, m.FrameMax); err != nil {
		return err
	}
	return WriteShort(w, m.Heartbeat)
}

// ConnectionOpen is sent by the client to open a virtual host.
type ConnectionOpen struct {
	VirtualHost  string // Short string
	Capabilities string // Short string - deprecated
	Insist       bool   // Bit
}

func (m *ConnectionOpen) Read(r *bytes.Reader) (err error) {
	if m.VirtualHost, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Capabilities, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Insist = (bits & 0x01) != 0
	return nil
}

func (m *ConnectionOpen) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionOpen); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.VirtualHost); err != nil {
		return err
	}
	if err := WriteShortStr(w, ""); err != nil { // Capabilities deprecated
		return err
	}
	var bits byte
	if m.Insist {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

// ConnectionOpenOk is sent by the server to confirm the virtual host is open.
type ConnectionOpenOk struct {
	KnownHosts string // Short string - deprecated
}

func (m *ConnectionOpenOk) Read(r *bytes.Reader) (err error) {
	if m.KnownHosts, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionOpenOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionOpenOk); err != nil {
		return err
	}
	return WriteShortStr(w, "") // KnownHosts deprecated
}

// ConnectionClose is sent by either peer to initiate a connection shutdown.
type ConnectionClose struct {
	ReplyCode uint16
	ReplyText string // Short string
	ClassID   uint16
	MethodID  uint16
}

func (m *ConnectionClose) Read(r *bytes.Reader) (err error) {
	if m.ReplyCode, err = ReadShort(r); err != nil {
		return err
	}
	if m.ReplyText, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.ClassID, err = ReadShort(r); err != nil {
		return err
	}
	if m.MethodID, err = ReadShort(r); err != nil {
		return err
	}
	return nil
}

func (m *ConnectionClose) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionClose); err != nil {
		return err
	}
	if err := WriteShort(w, m.ReplyCode); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.ReplyText); err != nil {
		return err
	}
	if err := WriteShort(w, m.ClassID); err != nil {
		return err
	}
	return WriteShort(w, m.MethodID)
}

// ConnectionCloseOk is sent by either peer to confirm a connection shutdown.
type ConnectionCloseOk struct{}

func (m *ConnectionCloseOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ConnectionCloseOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConnection); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConnectionCloseOk); err != nil {
		return err
	}
	return nil
}

const (
	ClassChannel = 20
)

const (
	MethodChannelOpen    = 10
	MethodChannelOpenOk  = 11
	MethodChannelFlow    = 20
	MethodChannelFlowOk  = 21
	MethodChannelClose   = 40
	MethodChannelCloseOk = 41
)

type ChannelOpen struct {
	Reserved1 string // Short string
}

func (m *ChannelOpen) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ChannelOpen) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassChannel); err != nil {
		return err
	}
	if err := WriteShort(w, MethodChannelOpen); err != nil {
		return err
	}
	return WriteShortStr(w, "")
}

type ChannelOpenOk struct {
	Reserved1 string // Long string
}

func (m *ChannelOpenOk) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadLongStr(r); err != nil {
		return err
	}
	return nil
}

func (m *ChannelOpenOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassChannel); err != nil {
		return err
	}
	if err := WriteShort(w, MethodChannelOpenOk); err != nil {
		return err
	}
	return WriteLongStr(w, "")
}

type ChannelFlow struct {
	Active bool
}

func (m *ChannelFlow) Read(r *bytes.Reader) (err error) {
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Active = (bits & 0x01) != 0
	return nil
}

func (m *ChannelFlow) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassChannel); err != nil {
		return err
	}
	if err := WriteShort(w, MethodChannelFlow); err != nil {
		return err
	}
	var bits byte
	if m.Active {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type ChannelFlowOk struct {
	Active bool
}

func (m *ChannelFlowOk) Read(r *bytes.Reader) (err error) {
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Active = (bits & 0x01) != 0
	return nil
}

func (m *ChannelFlowOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassChannel); err != nil {
		return err
	}
	if err := WriteShort(w, MethodChannelFlowOk); err != nil {
		return err
	}
	var bits byte
	if m.Active {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type ChannelClose struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
}

func (m *ChannelClose) Read(r *bytes.Reader) (err error) {
	if m.ReplyCode, err = ReadShort(r); err != nil {
		return err
	}
	if m.ReplyText, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.ClassID, err = ReadShort(r); err != nil {
		return err
	}
	if m.MethodID, err = ReadShort(r); err != nil {
		return err
	}
	return nil
}

func (m *ChannelClose) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassChannel); err != nil {
		return err
	}
	if err := WriteShort(w, MethodChannelClose); err != nil {
		return err
	}
	if err := WriteShort(w, m.ReplyCode); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.ReplyText); err != nil {
		return err
	}
	if err := WriteShort(w, m.ClassID); err != nil {
		return err
	}
	return WriteShort(w, m.MethodID)
}

type ChannelCloseOk struct{}

func (m *ChannelCloseOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ChannelCloseOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassChannel); err != nil {
		return err
	}
	if err := WriteShort(w, MethodChannelCloseOk); err != nil {
		return err
	}
	return nil
}

const (
	ClassExchange = 40
)

const (
	MethodExchangeDeclare   = 10
	MethodExchangeDeclareOk = 11
	MethodExchangeDelete    = 20
	MethodExchangeDeleteOk  = 21
	MethodExchangeBind      = 30
	MethodExchangeBindOk    = 31
	MethodExchangeUnbind    = 40
	MethodExchangeUnbindOk  = 51
)

type ExchangeDeclare struct {
	Reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]interface{}
}

func (m *ExchangeDeclare) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Type, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Passive = (bits & 0x01) != 0
	m.Durable = (bits & 0x02) != 0
	m.AutoDelete = (bits & 0x04) != 0
	m.Internal = (bits & 0x08) != 0
	m.NoWait = (bits & 0x10) != 0
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *ExchangeDeclare) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeDeclare); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Type); err != nil {
		return err
	}
	var bits byte
	if m.Passive {
		bits |= 0x01
	}
	if m.Durable {
		bits |= 0x02
	}
	if m.AutoDelete {
		bits |= 0x04
	}
	if m.Internal {
		bits |= 0x08
	}
	if m.NoWait {
		bits |= 0x10
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type ExchangeDeclareOk struct{}

func (m *ExchangeDeclareOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ExchangeDeclareOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeDeclareOk); err != nil {
		return err
	}
	return nil
}

type ExchangeDelete struct {
	Reserved1 uint16
	Exchange  string
	IfUnused  bool
	NoWait    bool
}

func (m *ExchangeDelete) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.IfUnused = (bits & 0x01) != 0
	m.NoWait = (bits & 0x02) != 0
	return nil
}

func (m *ExchangeDelete) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeDelete); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	var bits byte
	if m.IfUnused {
		bits |= 0x01
	}
	if m.NoWait {
		bits |= 0x02
	}
	return WriteOctet(w, bits)
}

type ExchangeDeleteOk struct{}

func (m *ExchangeDeleteOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ExchangeDeleteOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeDeleteOk); err != nil {
		return err
	}
	return nil
}

type ExchangeBind struct {
	Reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   map[string]interface{}
}

func (m *ExchangeBind) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Destination, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Source, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoWait = (bits & 0x01) != 0
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *ExchangeBind) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeBind); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Destination); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Source); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.RoutingKey); err != nil {
		return err
	}
	var bits byte
	if m.NoWait {
		bits |= 0x01
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type ExchangeBindOk struct{}

func (m *ExchangeBindOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ExchangeBindOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeBindOk); err != nil {
		return err
	}
	return nil
}

type ExchangeUnbind struct {
	Reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   map[string]interface{}
}

func (m *ExchangeUnbind) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Destination, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Source, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoWait = (bits & 0x01) != 0
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *ExchangeUnbind) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeUnbind); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Destination); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Source); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.RoutingKey); err != nil {
		return err
	}
	var bits byte
	if m.NoWait {
		bits |= 0x01
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type ExchangeUnbindOk struct{}

func (m *ExchangeUnbindOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ExchangeUnbindOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassExchange); err != nil {
		return err
	}
	if err := WriteShort(w, MethodExchangeUnbindOk); err != nil {
		return err
	}
	return nil
}

const (
	ClassQueue = 50
)

const (
	MethodQueueDeclare   = 10
	MethodQueueDeclareOk = 11
	MethodQueueBind      = 20
	MethodQueueBindOk    = 21
	MethodQueuePurge     = 30
	MethodQueuePurgeOk   = 31
	MethodQueueDelete    = 40
	MethodQueueDeleteOk  = 41
	MethodQueueUnbind    = 50
	MethodQueueUnbindOk  = 51
)

type QueueDeclare struct {
	Reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  map[string]interface{}
}

func (m *QueueDeclare) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Passive = (bits & 0x01) != 0
	m.Durable = (bits & 0x02) != 0
	m.Exclusive = (bits & 0x04) != 0
	m.AutoDelete = (bits & 0x08) != 0
	m.NoWait = (bits & 0x10) != 0
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *QueueDeclare) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueDeclare); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	var bits byte
	if m.Passive {
		bits |= 0x01
	}
	if m.Durable {
		bits |= 0x02
	}
	if m.Exclusive {
		bits |= 0x04
	}
	if m.AutoDelete {
		bits |= 0x08
	}
	if m.NoWait {
		bits |= 0x10
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type QueueDeclareOk struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

func (m *QueueDeclareOk) Read(r *bytes.Reader) (err error) {
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.MessageCount, err = ReadLong(r); err != nil {
		return err
	}
	if m.ConsumerCount, err = ReadLong(r); err != nil {
		return err
	}
	return nil
}

func (m *QueueDeclareOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueDeclareOk); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	if err := WriteLong(w, m.MessageCount); err != nil {
		return err
	}
	return WriteLong(w, m.ConsumerCount)
}

type QueueBind struct {
	Reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  map[string]interface{}
}

func (m *QueueBind) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoWait = (bits & 0x01) != 0
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *QueueBind) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueBind); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.RoutingKey); err != nil {
		return err
	}
	var bits byte
	if m.NoWait {
		bits |= 0x01
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type QueueBindOk struct{}

func (m *QueueBindOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *QueueBindOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueBindOk); err != nil {
		return err
	}
	return nil
}

type QueueUnbind struct {
	Reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	Arguments  map[string]interface{}
}

func (m *QueueUnbind) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *QueueUnbind) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueUnbind); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.RoutingKey); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type QueueUnbindOk struct{}

func (m *QueueUnbindOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *QueueUnbindOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueUnbindOk); err != nil {
		return err
	}
	return nil
}

type QueuePurge struct {
	Reserved1 uint16
	Queue     string
	NoWait    bool
}

func (m *QueuePurge) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoWait = (bits & 0x01) != 0
	return nil
}

func (m *QueuePurge) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueuePurge); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	var bits byte
	if m.NoWait {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type QueuePurgeOk struct {
	MessageCount uint32
}

func (m *QueuePurgeOk) Read(r *bytes.Reader) (err error) {
	if m.MessageCount, err = ReadLong(r); err != nil {
		return err
	}
	return nil
}

func (m *QueuePurgeOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueuePurgeOk); err != nil {
		return err
	}
	return WriteLong(w, m.MessageCount)
}

type QueueDelete struct {
	Reserved1 uint16
	Queue     string
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

func (m *QueueDelete) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.IfUnused = (bits & 0x01) != 0
	m.IfEmpty = (bits & 0x02) != 0
	m.NoWait = (bits & 0x04) != 0
	return nil
}

func (m *QueueDelete) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueDelete); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	var bits byte
	if m.IfUnused {
		bits |= 0x01
	}
	if m.IfEmpty {
		bits |= 0x02
	}
	if m.NoWait {
		bits |= 0x04
	}
	return WriteOctet(w, bits)
}

type QueueDeleteOk struct {
	MessageCount uint32
}

func (m *QueueDeleteOk) Read(r *bytes.Reader) (err error) {
	if m.MessageCount, err = ReadLong(r); err != nil {
		return err
	}
	return nil
}

func (m *QueueDeleteOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassQueue); err != nil {
		return err
	}
	if err := WriteShort(w, MethodQueueDeleteOk); err != nil {
		return err
	}
	return WriteLong(w, m.MessageCount)
}

const (
	ClassBasic = 60
)

const (
	MethodBasicQos          = 10
	MethodBasicQosOk        = 11
	MethodBasicConsume      = 20
	MethodBasicConsumeOk    = 21
	MethodBasicCancel       = 30
	MethodBasicCancelOk     = 31
	MethodBasicPublish      = 40
	MethodBasicReturn       = 50
	MethodBasicDeliver      = 60
	MethodBasicGet          = 70
	MethodBasicGetOk        = 71
	MethodBasicGetEmpty     = 72
	MethodBasicAck          = 80
	MethodBasicReject       = 90
	MethodBasicRecoverAsync = 100
	MethodBasicRecover      = 110
	MethodBasicRecoverOk    = 111
	MethodBasicNack         = 120
)

type BasicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

func (m *BasicQos) Read(r *bytes.Reader) (err error) {
	if m.PrefetchSize, err = ReadLong(r); err != nil {
		return err
	}
	if m.PrefetchCount, err = ReadShort(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Global = (bits & 0x01) != 0
	return nil
}

func (m *BasicQos) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicQos); err != nil {
		return err
	}
	if err := WriteLong(w, m.PrefetchSize); err != nil {
		return err
	}
	if err := WriteShort(w, m.PrefetchCount); err != nil {
		return err
	}
	var bits byte
	if m.Global {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicQosOk struct{}

func (m *BasicQosOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *BasicQosOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicQosOk); err != nil {
		return err
	}
	return nil
}

type BasicConsume struct {
	Reserved1   uint16
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   map[string]interface{}
}

func (m *BasicConsume) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.ConsumerTag, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoLocal = (bits & 0x01) != 0
	m.NoAck = (bits & 0x02) != 0
	m.Exclusive = (bits & 0x04) != 0
	m.NoWait = (bits & 0x08) != 0
	if m.Arguments, err = ReadTable(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicConsume) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicConsume); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.ConsumerTag); err != nil {
		return err
	}
	var bits byte
	if m.NoLocal {
		bits |= 0x01
	}
	if m.NoAck {
		bits |= 0x02
	}
	if m.Exclusive {
		bits |= 0x04
	}
	if m.NoWait {
		bits |= 0x08
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	return WriteTable(w, m.Arguments)
}

type BasicConsumeOk struct {
	ConsumerTag string
}

func (m *BasicConsumeOk) Read(r *bytes.Reader) (err error) {
	if m.ConsumerTag, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicConsumeOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicConsumeOk); err != nil {
		return err
	}
	return WriteShortStr(w, m.ConsumerTag)
}

type BasicCancel struct {
	ConsumerTag string
	NoWait      bool
}

func (m *BasicCancel) Read(r *bytes.Reader) (err error) {
	if m.ConsumerTag, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoWait = (bits & 0x01) != 0
	return nil
}

func (m *BasicCancel) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicCancel); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.ConsumerTag); err != nil {
		return err
	}
	var bits byte
	if m.NoWait {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicCancelOk struct {
	ConsumerTag string
}

func (m *BasicCancelOk) Read(r *bytes.Reader) (err error) {
	if m.ConsumerTag, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicCancelOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicCancelOk); err != nil {
		return err
	}
	return WriteShortStr(w, m.ConsumerTag)
}

type BasicPublish struct {
	Reserved1  uint16
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

func (m *BasicPublish) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Mandatory = (bits & 0x01) != 0
	m.Immediate = (bits & 0x02) != 0
	return nil
}

func (m *BasicPublish) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicPublish); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.RoutingKey); err != nil {
		return err
	}
	var bits byte
	if m.Mandatory {
		bits |= 0x01
	}
	if m.Immediate {
		bits |= 0x02
	}
	return WriteOctet(w, bits)
}

type BasicReturn struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
}

func (m *BasicReturn) Read(r *bytes.Reader) (err error) {
	if m.ReplyCode, err = ReadShort(r); err != nil {
		return err
	}
	if m.ReplyText, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicReturn) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicReturn); err != nil {
		return err
	}
	if err := WriteShort(w, m.ReplyCode); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.ReplyText); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	return WriteShortStr(w, m.RoutingKey)
}

type BasicDeliver struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
}

func (m *BasicDeliver) Read(r *bytes.Reader) (err error) {
	if m.ConsumerTag, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.DeliveryTag, err = ReadLongLong(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Redelivered = (bits & 0x01) != 0
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicDeliver) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicDeliver); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.ConsumerTag); err != nil {
		return err
	}
	if err := WriteLongLong(w, m.DeliveryTag); err != nil {
		return err
	}
	var bits byte
	if m.Redelivered {
		bits |= 0x01
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	return WriteShortStr(w, m.RoutingKey)
}

type BasicGet struct {
	Reserved1 uint16
	Queue     string
	NoAck     bool
}

func (m *BasicGet) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShort(r); err != nil {
		return err
	}
	if m.Queue, err = ReadShortStr(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoAck = (bits & 0x01) != 0
	return nil
}

func (m *BasicGet) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicGet); err != nil {
		return err
	}
	if err := WriteShort(w, 0); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Queue); err != nil {
		return err
	}
	var bits byte
	if m.NoAck {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicGetOk struct {
	DeliveryTag  uint64
	Redelivered  bool
	Exchange     string
	RoutingKey   string
	MessageCount uint32
}

func (m *BasicGetOk) Read(r *bytes.Reader) (err error) {
	if m.DeliveryTag, err = ReadLongLong(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Redelivered = (bits & 0x01) != 0
	if m.Exchange, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortStr(r); err != nil {
		return err
	}
	if m.MessageCount, err = ReadLong(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicGetOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicGetOk); err != nil {
		return err
	}
	if err := WriteLongLong(w, m.DeliveryTag); err != nil {
		return err
	}
	var bits byte
	if m.Redelivered {
		bits |= 0x01
	}
	if err := WriteOctet(w, bits); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.Exchange); err != nil {
		return err
	}
	if err := WriteShortStr(w, m.RoutingKey); err != nil {
		return err
	}
	return WriteLong(w, m.MessageCount)
}

type BasicGetEmpty struct {
	Reserved1 string
}

func (m *BasicGetEmpty) Read(r *bytes.Reader) (err error) {
	if m.Reserved1, err = ReadShortStr(r); err != nil {
		return err
	}
	return nil
}

func (m *BasicGetEmpty) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicGetEmpty); err != nil {
		return err
	}
	return WriteShortStr(w, "")
}

type BasicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

func (m *BasicAck) Read(r *bytes.Reader) (err error) {
	if m.DeliveryTag, err = ReadLongLong(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Multiple = (bits & 0x01) != 0
	return nil
}

func (m *BasicAck) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicAck); err != nil {
		return err
	}
	if err := WriteLongLong(w, m.DeliveryTag); err != nil {
		return err
	}
	var bits byte
	if m.Multiple {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

func (m *BasicReject) Read(r *bytes.Reader) (err error) {
	if m.DeliveryTag, err = ReadLongLong(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Requeue = (bits & 0x01) != 0
	return nil
}

func (m *BasicReject) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicReject); err != nil {
		return err
	}
	if err := WriteLongLong(w, m.DeliveryTag); err != nil {
		return err
	}
	var bits byte
	if m.Requeue {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicRecoverAsync struct {
	Requeue bool
}

func (m *BasicRecoverAsync) Read(r *bytes.Reader) (err error) {
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Requeue = (bits & 0x01) != 0
	return nil
}

func (m *BasicRecoverAsync) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicRecoverAsync); err != nil {
		return err
	}
	var bits byte
	if m.Requeue {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicRecover struct {
	Requeue bool
}

func (m *BasicRecover) Read(r *bytes.Reader) (err error) {
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Requeue = (bits & 0x01) != 0
	return nil
}

func (m *BasicRecover) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicRecover); err != nil {
		return err
	}
	var bits byte
	if m.Requeue {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type BasicRecoverOk struct{}

func (m *BasicRecoverOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *BasicRecoverOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicRecoverOk); err != nil {
		return err
	}
	return nil
}

type BasicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

func (m *BasicNack) Read(r *bytes.Reader) (err error) {
	if m.DeliveryTag, err = ReadLongLong(r); err != nil {
		return err
	}
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.Multiple = (bits & 0x01) != 0
	m.Requeue = (bits & 0x02) != 0
	return nil
}

func (m *BasicNack) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassBasic); err != nil {
		return err
	}
	if err := WriteShort(w, MethodBasicNack); err != nil {
		return err
	}
	if err := WriteLongLong(w, m.DeliveryTag); err != nil {
		return err
	}
	var bits byte
	if m.Multiple {
		bits |= 0x01
	}
	if m.Requeue {
		bits |= 0x02
	}
	return WriteOctet(w, bits)
}

const (
	ClassConfirm = 85
)

const (
	MethodConfirmSelect   = 10
	MethodConfirmSelectOk = 11
)

type ConfirmSelect struct {
	NoWait bool
}

func (m *ConfirmSelect) Read(r *bytes.Reader) (err error) {
	var bits byte
	if bits, err = ReadOctet(r); err != nil {
		return err
	}
	m.NoWait = (bits & 0x01) != 0
	return nil
}

func (m *ConfirmSelect) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConfirm); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConfirmSelect); err != nil {
		return err
	}
	var bits byte
	if m.NoWait {
		bits |= 0x01
	}
	return WriteOctet(w, bits)
}

type ConfirmSelectOk struct{}

func (m *ConfirmSelectOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *ConfirmSelectOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassConfirm); err != nil {
		return err
	}
	if err := WriteShort(w, MethodConfirmSelectOk); err != nil {
		return err
	}
	return nil
}

const (
	ClassTx = 90
)

const (
	MethodTxSelect     = 10
	MethodTxSelectOk   = 11
	MethodTxCommit     = 20
	MethodTxCommitOk   = 21
	MethodTxRollback   = 30
	MethodTxRollbackOk = 31
)

type TxSelect struct{}

func (m *TxSelect) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *TxSelect) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassTx); err != nil {
		return err
	}
	if err := WriteShort(w, MethodTxSelect); err != nil {
		return err
	}
	return nil
}

type TxSelectOk struct{}

func (m *TxSelectOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *TxSelectOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassTx); err != nil {
		return err
	}
	if err := WriteShort(w, MethodTxSelectOk); err != nil {
		return err
	}
	return nil
}

type TxCommit struct{}

func (m *TxCommit) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *TxCommit) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassTx); err != nil {
		return err
	}
	if err := WriteShort(w, MethodTxCommit); err != nil {
		return err
	}
	return nil
}

type TxCommitOk struct{}

func (m *TxCommitOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *TxCommitOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassTx); err != nil {
		return err
	}
	if err := WriteShort(w, MethodTxCommitOk); err != nil {
		return err
	}
	return nil
}

type TxRollback struct{}

func (m *TxRollback) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *TxRollback) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassTx); err != nil {
		return err
	}
	if err := WriteShort(w, MethodTxRollback); err != nil {
		return err
	}
	return nil
}

type TxRollbackOk struct{}

func (m *TxRollbackOk) Read(r *bytes.Reader) (err error) {
	return nil
}

func (m *TxRollbackOk) Write(w io.Writer) (err error) {
	if err := WriteShort(w, ClassTx); err != nil {
		return err
	}
	if err := WriteShort(w, MethodTxRollbackOk); err != nil {
		return err
	}
	return nil
}
