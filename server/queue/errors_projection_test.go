// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allErrorCodes, allOwnershipStates, allLeaderStates, and allDurabilityStates
// enumerate the domain taxonomy. Adding a value to queue/errors.go without
// adding it here fails TestTaxonomyEnumerationIsComplete; adding it here without
// mapping it fails TestErrorProjectionTablesAreExhaustive. Between them a new
// value cannot reach a wire as an unmapped zero.
var (
	allErrorCodes = []queuepkg.ErrorCode{
		queuepkg.ErrorCodeCanceled,
		queuepkg.ErrorCodeInvalidArgument,
		queuepkg.ErrorCodeNotFound,
		queuepkg.ErrorCodeAlreadyExists,
		queuepkg.ErrorCodeConflict,
		queuepkg.ErrorCodeFailedPrecondition,
		queuepkg.ErrorCodeResourceExhausted,
		queuepkg.ErrorCodeOutOfRange,
		queuepkg.ErrorCodeUnavailable,
		queuepkg.ErrorCodeDeadlineExceeded,
		queuepkg.ErrorCodeInternal,
	}
	allOwnershipStates = []queuepkg.OwnershipState{
		queuepkg.OwnershipUnspecified,
		queuepkg.OwnershipCaller,
		queuepkg.OwnershipOther,
		queuepkg.OwnershipLost,
	}
	allLeaderStates = []queuepkg.LeaderState{
		queuepkg.LeaderUnspecified,
		queuepkg.LeaderRequired,
		queuepkg.LeaderUnavailable,
		queuepkg.LeaderNotLocal,
	}
	allDurabilityStates = []queuepkg.DurabilityState{
		queuepkg.DurabilityUnspecified,
		queuepkg.DurabilityNotAttempted,
		queuepkg.DurabilityUnconfirmed,
		queuepkg.DurabilityUnsupported,
	}
)

func TestErrorProjectionTablesAreExhaustive(t *testing.T) {
	t.Run("connect codes", func(t *testing.T) {
		require.Len(t, connectCodes, len(allErrorCodes))
		for _, code := range allErrorCodes {
			mapped, ok := connectCodes[code]
			assert.True(t, ok, "error code %q has no Connect projection", code)
			assert.NotEqual(t, connect.Code(0), mapped, "error code %q maps to the zero Connect code", code)
		}
	})

	t.Run("proto error codes", func(t *testing.T) {
		require.Len(t, protoErrorCodes, len(allErrorCodes))
		for _, code := range allErrorCodes {
			mapped, ok := protoErrorCodes[code]
			assert.True(t, ok, "error code %q has no protobuf projection", code)
			assert.NotEqual(t, queuev1.QueueErrorCode_QUEUE_ERROR_CODE_UNSPECIFIED, mapped,
				"error code %q maps to UNSPECIFIED, which is never emitted", code)
		}
	})

	t.Run("proto ownership states", func(t *testing.T) {
		require.Len(t, protoOwnershipStates, len(allOwnershipStates))
		for _, state := range allOwnershipStates {
			_, ok := protoOwnershipStates[state]
			assert.True(t, ok, "ownership state %q has no protobuf projection", state)
		}
	})

	t.Run("proto leader states", func(t *testing.T) {
		require.Len(t, protoLeaderStates, len(allLeaderStates))
		for _, state := range allLeaderStates {
			_, ok := protoLeaderStates[state]
			assert.True(t, ok, "leader state %q has no protobuf projection", state)
		}
	})

	t.Run("proto durability states", func(t *testing.T) {
		require.Len(t, protoDurabilityStates, len(allDurabilityStates))
		for _, state := range allDurabilityStates {
			_, ok := protoDurabilityStates[state]
			assert.True(t, ok, "durability state %q has no protobuf projection", state)
		}
	})
}

// The projections must not collapse two distinct domain codes onto one wire
// value: a client branching on the result would lose the distinction.
func TestErrorProjectionsAreInjective(t *testing.T) {
	t.Run("connect codes", func(t *testing.T) {
		seen := make(map[connect.Code]queuepkg.ErrorCode, len(connectCodes))
		for code, mapped := range connectCodes {
			if previous, clash := seen[mapped]; clash {
				t.Errorf("connect code %v is shared by %q and %q", mapped, previous, code)
			}
			seen[mapped] = code
		}
	})

	t.Run("proto error codes", func(t *testing.T) {
		seen := make(map[queuev1.QueueErrorCode]queuepkg.ErrorCode, len(protoErrorCodes))
		for code, mapped := range protoErrorCodes {
			if previous, clash := seen[mapped]; clash {
				t.Errorf("protobuf code %v is shared by %q and %q", mapped, previous, code)
			}
			seen[mapped] = code
		}
	})
}

// The taxonomy has no success value, so every declared code must project as a
// non-OK Connect code.
func TestEveryErrorCodeProjectsAsFailure(t *testing.T) {
	for _, code := range allErrorCodes {
		assert.NotEqual(t, connect.Code(0), connectCode(code), "error code %q projects as success", code)
	}
}

// TestTaxonomyEnumerationIsComplete parses queue/errors.go and asserts the
// enumerations above list every declared constant. Without it, adding a domain
// constant and forgetting the projection tables would pass silently: the
// exhaustiveness test only compares the tables against these lists.
func TestTaxonomyEnumerationIsComplete(t *testing.T) {
	declared := parseDeclaredConstants(t, "../../queue/errors.go")

	tests := []struct {
		typeName   string
		enumerated []string
	}{
		{"ErrorCode", stringifyAll(allErrorCodes)},
		{"OwnershipState", stringifyAll(allOwnershipStates)},
		{"LeaderState", stringifyAll(allLeaderStates)},
		{"DurabilityState", stringifyAll(allDurabilityStates)},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			names := declared[tt.typeName]
			require.NotEmpty(t, names, "no constants found for type %s", tt.typeName)
			assert.ElementsMatch(t, names, tt.enumerated,
				"queue.%s constants and the enumeration in this file have diverged", tt.typeName)
		})
	}
}

func stringifyAll[T ~string](values []T) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = string(value)
	}
	return out
}

// parseDeclaredConstants returns the literal values of every typed string
// constant in path, keyed by type name.
func parseDeclaredConstants(t *testing.T, path string) map[string][]string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)

	declared := make(map[string][]string)
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || valueSpec.Type == nil {
				continue
			}
			typeName, ok := valueSpec.Type.(*ast.Ident)
			if !ok {
				continue
			}
			for _, value := range valueSpec.Values {
				literal, ok := value.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				unquoted, err := strconv.Unquote(literal.Value)
				require.NoError(t, err)
				declared[typeName.Name] = append(declared[typeName.Name], unquoted)
			}
		}
	}
	return declared
}
