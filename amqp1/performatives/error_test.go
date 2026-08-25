// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/amqp1/types"
	"github.com/stretchr/testify/require"
)

const testErrDescription = "gone"

func TestErrorInfoRoundTrip(t *testing.T) {
	want := &Error{
		Condition:   ErrResourceLimitExceeded,
		Description: "queue operation failed",
		Info: map[types.Symbol]any{
			"fluxmq:queue-error-code": "resource_exhausted",
			"fluxmq:retryable":        true,
		},
	}

	encoded, err := want.Encode()
	require.NoError(t, err)
	decoded, err := types.ReadType(bytes.NewReader(encoded))
	require.NoError(t, err)
	described, ok := decoded.(*types.Described)
	require.True(t, ok)
	fields, ok := described.Value.([]any)
	require.True(t, ok)
	got, err := DecodeError(fields)
	require.NoError(t, err)

	require.Equal(t, want.Condition, got.Condition)
	require.Equal(t, want.Description, got.Description)
	require.Equal(t, want.Info, got.Info)
}

// Every field of an error performative arrives from a remote peer. A type
// mismatch must be reported, never coerced, zeroed, or allowed to panic.
func TestDecodeErrorRejectsMalformedFields(t *testing.T) {
	tests := []struct {
		name   string
		fields []any
	}{
		{
			name:   "condition/not-symbol",
			fields: []any{"amqp:not-found"},
		},
		{
			name:   "condition/numeric",
			fields: []any{uint32(7)},
		},
		{
			name:   "description/not-string",
			fields: []any{ErrNotFound, uint32(7)},
		},
		{
			name:   "description/symbol",
			fields: []any{ErrNotFound, types.Symbol("oops")},
		},
		{
			name:   "info/not-map",
			fields: []any{ErrNotFound, testErrDescription, "not-a-map"},
		},
		{
			name:   "info/key-not-symbol",
			fields: []any{ErrNotFound, testErrDescription, map[any]any{"plain-string-key": true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got, err := DecodeError(tt.fields)
				require.ErrorIs(t, err, ErrMalformedError)
				require.Nil(t, got)
			})
		})
	}
}

// condition is a mandatory field of the AMQP error type. An absent error field
// is legal (covered by TestDecodeErrorFieldTreatsAbsentErrorAsLegal), but an
// error composite that carries no condition is not.
func TestDecodeErrorRequiresCondition(t *testing.T) {
	tests := []struct {
		name   string
		fields []any
	}{
		{name: "no fields", fields: nil},
		{name: "empty fields", fields: []any{}},
		{name: "null condition", fields: []any{nil}},
		{name: "explicit nulls", fields: []any{nil, nil, nil}},
		{name: "null condition with description", fields: []any{nil, testErrDescription}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeError(tt.fields)
			require.ErrorIs(t, err, ErrMalformedError)
			require.Nil(t, got)
		})
	}
}

// description and info stay optional, so a condition-only error still decodes.
func TestDecodeErrorAcceptsConditionOnly(t *testing.T) {
	tests := []struct {
		name   string
		fields []any
		assert func(*testing.T, *Error)
	}{
		{
			name:   "condition only",
			fields: []any{ErrNotFound},
			assert: func(t *testing.T, e *Error) {
				require.Equal(t, ErrNotFound, e.Condition)
				require.Empty(t, e.Description)
				require.Nil(t, e.Info)
			},
		},
		{
			name:   "condition with null description and info",
			fields: []any{ErrNotFound, nil, nil},
			assert: func(t *testing.T, e *Error) {
				require.Equal(t, ErrNotFound, e.Condition)
				require.Empty(t, e.Description)
				require.Nil(t, e.Info)
			},
		},
		{
			name:   "condition and description",
			fields: []any{ErrResourceLocked, "held elsewhere"},
			assert: func(t *testing.T, e *Error) {
				require.Equal(t, ErrResourceLocked, e.Condition)
				require.Equal(t, "held elsewhere", e.Description)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeError(tt.fields)
			require.NoError(t, err)
			require.NotNil(t, got)
			tt.assert(t, got)
		})
	}
}

// The error field of a performative is optional, but a present one must be a
// well-formed error composite: a peer must not be able to suppress an error by
// mis-encoding its wrapper.
func TestDecodeErrorFieldRejectsMalformedWrappers(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "not a described type", value: "amqp:not-found"},
		{name: "numeric", value: uint32(7)},
		{name: "wrong descriptor", value: &types.Described{Descriptor: DescriptorAccepted, Value: []any{ErrNotFound}}},
		{name: "body not a list", value: &types.Described{Descriptor: DescriptorError, Value: testErrDescription}},
		{name: "body is an empty composite", value: &types.Described{Descriptor: DescriptorError, Value: []any{}}},
		{name: "body has a null condition", value: &types.Described{Descriptor: DescriptorError, Value: []any{nil}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got, err := decodeErrorField(tt.value)
				require.ErrorIs(t, err, ErrMalformedError)
				require.Nil(t, got)
			})
		})
	}
}

func TestDecodeErrorFieldTreatsAbsentErrorAsLegal(t *testing.T) {
	got, err := decodeErrorField(nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

// A malformed error must fail the whole performative rather than decoding into
// one that silently reports no error.
func TestPerformativeDecodersRejectMalformedErrors(t *testing.T) {
	malformed := &types.Described{Descriptor: DescriptorError, Value: []any{nil}}

	t.Run("detach", func(t *testing.T) {
		got, err := DecodeDetach([]any{uint32(1), true, malformed})
		require.ErrorIs(t, err, ErrMalformedError)
		require.Nil(t, got)
	})

	t.Run("end", func(t *testing.T) {
		got, err := DecodeEnd([]any{malformed})
		require.ErrorIs(t, err, ErrMalformedError)
		require.Nil(t, got)
	})

	t.Run("close", func(t *testing.T) {
		got, err := DecodeClose([]any{malformed})
		require.ErrorIs(t, err, ErrMalformedError)
		require.Nil(t, got)
	})

	t.Run("rejected outcome", func(t *testing.T) {
		got, err := DecodeOutcome(&types.Described{Descriptor: DescriptorRejected, Value: []any{malformed}})
		require.ErrorIs(t, err, ErrMalformedError)
		require.Nil(t, got)
	})

	t.Run("rejected outcome with non-list body", func(t *testing.T) {
		got, err := DecodeOutcome(&types.Described{Descriptor: DescriptorRejected, Value: "nope"})
		require.ErrorIs(t, err, ErrMalformedError)
		require.Nil(t, got)
	})
}

// A rejected outcome carrying no error at all remains legal.
func TestDecodeOutcomeAcceptsRejectedWithoutError(t *testing.T) {
	for _, value := range []any{nil, []any{}, []any{nil}} {
		got, err := DecodeOutcome(&types.Described{Descriptor: DescriptorRejected, Value: value})
		require.NoError(t, err)
		rejected, ok := got.(*Rejected)
		require.True(t, ok)
		require.Nil(t, rejected.Error)
	}
}
