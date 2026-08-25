// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"errors"
	"fmt"
	"io"

	"github.com/absmach/fluxmq/amqp1/types"
	"github.com/absmach/fluxmq/internal/bufpool"
)

// AMQP error descriptor.
const DescriptorError uint64 = 0x1D

// ErrMalformedError reports an error performative whose fields do not match the
// AMQP 1.0 error type. It is a peer protocol violation.
var ErrMalformedError = errors.New("malformed amqp error performative")

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

	// Connection errors.
	ErrConnectionForced   types.Symbol = "amqp:connection:forced"
	ErrFramingError       types.Symbol = "amqp:connection:framing-error"
	ErrConnectionRedirect types.Symbol = "amqp:connection:redirect"

	// Session errors.
	ErrWindowViolation  types.Symbol = "amqp:session:window-violation"
	ErrErrantLink       types.Symbol = "amqp:session:errant-link"
	ErrHandleInUse      types.Symbol = "amqp:session:handle-in-use"
	ErrUnattachedHandle types.Symbol = "amqp:session:unattached-handle"

	// Link errors.
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
	fields := bufpool.Get()
	defer bufpool.Put(fields)
	if err := types.WriteSymbol(fields, e.Condition); err != nil {
		return nil, err
	}
	if e.Description != "" {
		if err := types.WriteString(fields, e.Description); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(fields); err != nil {
			return nil, err
		}
	}
	if len(e.Info) > 0 {
		if err := writeSymbolAnyMap(fields, e.Info); err != nil {
			return nil, err
		}
	} else {
		if err := types.WriteNull(fields); err != nil {
			return nil, err
		}
	}

	buf := bufpool.Get()
	defer bufpool.Put(buf)
	if err := types.WriteDescriptor(buf, DescriptorError); err != nil {
		return nil, err
	}
	if err := types.WriteList(buf, fields.Bytes(), 3); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// decodeErrorField decodes a performative field that carries an AMQP error.
//
// The error field itself is optional: a nil value means the peer sent no error,
// which is legal and yields a nil result. Anything else must be a well-formed
// error composite — a described type with the error descriptor whose body is a
// list. A malformed wrapper is a protocol violation and is reported rather than
// skipped, so a peer cannot suppress an error by mis-encoding it.
func decodeErrorField(value any) (*Error, error) {
	if value == nil {
		return nil, nil
	}
	described, ok := value.(*types.Described)
	if !ok {
		return nil, fmt.Errorf("%w: error field is %T, want a described type", ErrMalformedError, value)
	}
	if described.Descriptor != DescriptorError {
		return nil, fmt.Errorf("%w: error field has descriptor 0x%02x, want 0x%02x",
			ErrMalformedError, described.Descriptor, DescriptorError)
	}
	fields, ok := described.Value.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: error body is %T, want a list", ErrMalformedError, described.Value)
	}
	return DecodeError(fields)
}

// DecodeError decodes an AMQP error composite from its list fields.
//
// Every field arrives from a remote peer, so a type mismatch is a protocol
// violation rather than a local bug: it is reported instead of being silently
// coerced or zeroed, and the caller is expected to fail the frame.
//
// condition is a mandatory field of the error type: an absent error field is
// legal, but an error composite that carries no condition, or a null one, is
// not. description and info remain optional.
func DecodeError(fields []any) (*Error, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: condition is required", ErrMalformedError)
	}
	if fields[0] == nil {
		return nil, fmt.Errorf("%w: condition is null", ErrMalformedError)
	}
	e := &Error{}
	{
		condition, ok := fields[0].(types.Symbol)
		if !ok {
			return nil, fmt.Errorf("%w: condition is %T, want symbol", ErrMalformedError, fields[0])
		}
		e.Condition = condition
	}
	if len(fields) > 1 && fields[1] != nil {
		description, ok := fields[1].(string)
		if !ok {
			return nil, fmt.Errorf("%w: description is %T, want string", ErrMalformedError, fields[1])
		}
		e.Description = description
	}
	if len(fields) > 2 && fields[2] != nil {
		info, ok := fields[2].(map[any]any)
		if !ok {
			return nil, fmt.Errorf("%w: info is %T, want map", ErrMalformedError, fields[2])
		}
		e.Info = make(map[types.Symbol]any, len(info))
		for key, value := range info {
			symbol, ok := key.(types.Symbol)
			if !ok {
				return nil, fmt.Errorf("%w: info key is %T, want symbol", ErrMalformedError, key)
			}
			e.Info[symbol] = value
		}
	}
	return e, nil
}

func writeSymbolAnyMap(w io.Writer, m map[types.Symbol]any) error {
	pairs := bufpool.Get()
	defer bufpool.Put(pairs)
	for k, v := range m {
		if err := types.WriteSymbol(pairs, k); err != nil {
			return err
		}
		if err := types.WriteAny(pairs, v); err != nil {
			return err
		}
	}
	return types.WriteMap(w, pairs.Bytes(), len(m))
}
