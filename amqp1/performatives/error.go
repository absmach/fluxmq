// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"

	"github.com/absmach/fluxmq/amqp1/types"
)

// AMQP error descriptor
const DescriptorError uint64 = 0x1D

// Standard error condition symbols.
const (
	ErrInternalError         types.Symbol = "amqp:internal-error"
	ErrNotFound              types.Symbol = "amqp:not-found"
	ErrUnauthorizedAccess    types.Symbol = "amqp:unauthorized-access"
	ErrDecodeError           types.Symbol = "amqp:decode-error"
	ErrResourceLimitExceeded types.Symbol = "amqp:resource-limit-exceeded"
	ErrNotAllowed            types.Symbol = "amqp:not-allowed"
	ErrInvalidField          types.Symbol = "amqp:invalid-field"
	ErrNotImplemented        types.Symbol = "amqp:not-implemented"
	ErrResourceLocked        types.Symbol = "amqp:resource-locked"
	ErrPreconditionFailed    types.Symbol = "amqp:precondition-failed"
	ErrResourceDeleted       types.Symbol = "amqp:resource-deleted"
	ErrIllegalState          types.Symbol = "amqp:illegal-state"
	ErrFrameSizeTooSmall     types.Symbol = "amqp:frame-size-too-small"

	// Connection errors
	ErrConnectionForced   types.Symbol = "amqp:connection:forced"
	ErrFramingError       types.Symbol = "amqp:connection:framing-error"
	ErrConnectionRedirect types.Symbol = "amqp:connection:redirect"

	// Session errors
	ErrWindowViolation  types.Symbol = "amqp:session:window-violation"
	ErrErrantLink       types.Symbol = "amqp:session:errant-link"
	ErrHandleInUse      types.Symbol = "amqp:session:handle-in-use"
	ErrUnattachedHandle types.Symbol = "amqp:session:unattached-handle"

	// Link errors
	ErrDetachForced          types.Symbol = "amqp:link:detach-forced"
	ErrTransferLimitExceeded types.Symbol = "amqp:link:transfer-limit-exceeded"
	ErrMessageSizeExceeded   types.Symbol = "amqp:link:message-size-exceeded"
	ErrLinkRedirect          types.Symbol = "amqp:link:redirect"
	ErrStolen                types.Symbol = "amqp:link:stolen"
)

// Error represents an AMQP error (descriptor 0x1D).
type Error struct {
	Condition   types.Symbol
	Description string
	Info        map[types.Symbol]any
}

// Encode serializes the error as a described list.
func (e *Error) Encode() ([]byte, error) {
	var fields bytes.Buffer
	if err := types.WriteSymbol(&fields, e.Condition); err != nil {
		return nil, err
	}
	if e.Description != "" {
		if err := types.WriteString(&fields, e.Description); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}
	if len(e.Info) > 0 {
		if err := writeSymbolAnyMap(&fields, e.Info); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(&fields); err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := types.WriteDescriptor(&buf, DescriptorError); err != nil {
		return nil, err
	}
	if err := types.WriteList(&buf, fields.Bytes(), 3); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeError decodes an AMQP error from list fields.
func DecodeError(fields []any) *Error {
	if len(fields) == 0 {
		return nil
	}
	e := &Error{}
	if len(fields) > 0 && fields[0] != nil {
		e.Condition = fields[0].(types.Symbol)
	}
	if len(fields) > 1 && fields[1] != nil {
		e.Description = fields[1].(string)
	}
	return e
}

func writeSymbolAnyMap(w *bytes.Buffer, m map[types.Symbol]any) error {
	var pairs bytes.Buffer
	for k, v := range m {
		if err := types.WriteSymbol(&pairs, k); err != nil {
			return err
		}
		if err := types.WriteAny(&pairs, v); err != nil {
			return err
		}
	}
	return types.WriteMap(w, pairs.Bytes(), len(m))
}
