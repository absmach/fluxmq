// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"io"
)

// Property flag bits for BasicProperties.
const (
	FlagContentType     uint16 = 1 << 15
	FlagContentEncoding uint16 = 1 << 14
	FlagHeaders         uint16 = 1 << 13
	FlagDeliveryMode    uint16 = 1 << 12
	FlagPriority        uint16 = 1 << 11
	FlagCorrelationID   uint16 = 1 << 10
	FlagReplyTo         uint16 = 1 << 9
	FlagExpiration      uint16 = 1 << 8
	FlagMessageID       uint16 = 1 << 7
	FlagTimestamp       uint16 = 1 << 6
	FlagType            uint16 = 1 << 5
	FlagUserID          uint16 = 1 << 4
	FlagAppID           uint16 = 1 << 3
	FlagClusterID       uint16 = 1 << 2
)

// BasicProperties represents the content header properties for basic class messages.
type BasicProperties struct {
	ContentType     string
	ContentEncoding string
	Headers         map[string]interface{}
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       uint64
	Type            string
	UserID          string
	AppID           string
	ClusterID       string
}

// Flags returns the property flags bitmask for the set fields.
func (p *BasicProperties) Flags() uint16 {
	var flags uint16
	if p.ContentType != "" {
		flags |= FlagContentType
	}
	if p.ContentEncoding != "" {
		flags |= FlagContentEncoding
	}
	if p.Headers != nil {
		flags |= FlagHeaders
	}
	if p.DeliveryMode != 0 {
		flags |= FlagDeliveryMode
	}
	if p.Priority != 0 {
		flags |= FlagPriority
	}
	if p.CorrelationID != "" {
		flags |= FlagCorrelationID
	}
	if p.ReplyTo != "" {
		flags |= FlagReplyTo
	}
	if p.Expiration != "" {
		flags |= FlagExpiration
	}
	if p.MessageID != "" {
		flags |= FlagMessageID
	}
	if p.Timestamp != 0 {
		flags |= FlagTimestamp
	}
	if p.Type != "" {
		flags |= FlagType
	}
	if p.UserID != "" {
		flags |= FlagUserID
	}
	if p.AppID != "" {
		flags |= FlagAppID
	}
	if p.ClusterID != "" {
		flags |= FlagClusterID
	}
	return flags
}

// Read reads BasicProperties from the reader according to the given property flags.
func (p *BasicProperties) Read(r io.Reader, flags uint16) error {
	var err error
	if flags&FlagContentType != 0 {
		if p.ContentType, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagContentEncoding != 0 {
		if p.ContentEncoding, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagHeaders != 0 {
		if p.Headers, err = ReadTable(r); err != nil {
			return err
		}
	}
	if flags&FlagDeliveryMode != 0 {
		if p.DeliveryMode, err = ReadOctet(r); err != nil {
			return err
		}
	}
	if flags&FlagPriority != 0 {
		if p.Priority, err = ReadOctet(r); err != nil {
			return err
		}
	}
	if flags&FlagCorrelationID != 0 {
		if p.CorrelationID, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagReplyTo != 0 {
		if p.ReplyTo, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagExpiration != 0 {
		if p.Expiration, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagMessageID != 0 {
		if p.MessageID, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagTimestamp != 0 {
		if p.Timestamp, err = ReadLongLong(r); err != nil {
			return err
		}
	}
	if flags&FlagType != 0 {
		if p.Type, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagUserID != 0 {
		if p.UserID, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagAppID != 0 {
		if p.AppID, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	if flags&FlagClusterID != 0 {
		if p.ClusterID, err = ReadShortStr(r); err != nil {
			return err
		}
	}
	return nil
}

// Write writes BasicProperties to the writer (properties only, not the flags).
func (p *BasicProperties) Write(w io.Writer) error {
	flags := p.Flags()
	if flags&FlagContentType != 0 {
		if err := WriteShortStr(w, p.ContentType); err != nil {
			return err
		}
	}
	if flags&FlagContentEncoding != 0 {
		if err := WriteShortStr(w, p.ContentEncoding); err != nil {
			return err
		}
	}
	if flags&FlagHeaders != 0 {
		if err := WriteTable(w, p.Headers); err != nil {
			return err
		}
	}
	if flags&FlagDeliveryMode != 0 {
		if err := WriteOctet(w, p.DeliveryMode); err != nil {
			return err
		}
	}
	if flags&FlagPriority != 0 {
		if err := WriteOctet(w, p.Priority); err != nil {
			return err
		}
	}
	if flags&FlagCorrelationID != 0 {
		if err := WriteShortStr(w, p.CorrelationID); err != nil {
			return err
		}
	}
	if flags&FlagReplyTo != 0 {
		if err := WriteShortStr(w, p.ReplyTo); err != nil {
			return err
		}
	}
	if flags&FlagExpiration != 0 {
		if err := WriteShortStr(w, p.Expiration); err != nil {
			return err
		}
	}
	if flags&FlagMessageID != 0 {
		if err := WriteShortStr(w, p.MessageID); err != nil {
			return err
		}
	}
	if flags&FlagTimestamp != 0 {
		if err := WriteLongLong(w, p.Timestamp); err != nil {
			return err
		}
	}
	if flags&FlagType != 0 {
		if err := WriteShortStr(w, p.Type); err != nil {
			return err
		}
	}
	if flags&FlagUserID != 0 {
		if err := WriteShortStr(w, p.UserID); err != nil {
			return err
		}
	}
	if flags&FlagAppID != 0 {
		if err := WriteShortStr(w, p.AppID); err != nil {
			return err
		}
	}
	if flags&FlagClusterID != 0 {
		if err := WriteShortStr(w, p.ClusterID); err != nil {
			return err
		}
	}
	return nil
}

// ContentHeader represents a decoded content header frame.
type ContentHeader struct {
	ClassID    uint16
	Weight     uint16
	BodySize   uint64
	Flags      uint16
	Properties BasicProperties
}

// ReadContentHeader reads a content header from the reader.
func ReadContentHeader(r io.Reader) (*ContentHeader, error) {
	classID, err := ReadShort(r)
	if err != nil {
		return nil, err
	}
	weight, err := ReadShort(r)
	if err != nil {
		return nil, err
	}
	bodySize, err := ReadLongLong(r)
	if err != nil {
		return nil, err
	}
	flags, err := ReadShort(r)
	if err != nil {
		return nil, err
	}
	h := &ContentHeader{
		ClassID:  classID,
		Weight:   weight,
		BodySize: bodySize,
		Flags:    flags,
	}
	if err := h.Properties.Read(r, flags); err != nil {
		return nil, err
	}
	return h, nil
}

// WriteContentHeader writes a content header to the writer.
func (h *ContentHeader) WriteContentHeader(w io.Writer) error {
	if err := WriteShort(w, h.ClassID); err != nil {
		return err
	}
	if err := WriteShort(w, h.Weight); err != nil {
		return err
	}
	if err := WriteLongLong(w, h.BodySize); err != nil {
		return err
	}
	flags := h.Properties.Flags()
	h.Flags = flags
	if err := WriteShort(w, flags); err != nil {
		return err
	}
	return h.Properties.Write(w)
}
