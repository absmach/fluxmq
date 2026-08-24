// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/amqp1/types"
	"github.com/stretchr/testify/require"
)

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
	got := DecodeError(fields)

	require.Equal(t, want.Condition, got.Condition)
	require.Equal(t, want.Description, got.Description)
	require.Equal(t, want.Info, got.Info)
}
