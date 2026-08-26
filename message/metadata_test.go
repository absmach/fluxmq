// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImmutableMetadataOwnsInputsAndCopiesMutableOutputs(t *testing.T) {
	key := []byte("key")
	headerValue := []byte("value")
	headersInput := map[string][]byte{"header": headerValue}
	propertiesInput := map[string]string{testTenantKey: testTenant}
	idsInput := []uint32{1, 2}

	binary := NewBinary(key)
	headers := NewHeaderMap(headersInput)
	properties := NewPropertyMap(propertiesInput)
	ids := NewUint32List(idsInput...)

	key[0] = 'X'
	headerValue[0] = 'X'
	headersInput["new"] = []byte("new")
	propertiesInput[testTenantKey] = "changed"
	idsInput[0] = 99

	require.True(t, binary.Equal([]byte("key")))
	header, ok := headers.Get("header")
	require.True(t, ok)
	require.True(t, header.Equal([]byte("value")))
	_, ok = headers.Get("new")
	require.False(t, ok)
	tenant, ok := properties.Get(testTenantKey)
	require.True(t, ok)
	require.Equal(t, testTenant, tenant)
	require.Equal(t, []uint32{1, 2}, ids.Slice())

	binaryBytes := binary.Bytes()
	headerMap := headers.Map()
	propertyMap := properties.Map()
	idSlice := ids.Slice()
	binaryBytes[0] = 'Y'
	headerMap["header"][0] = 'Y'
	propertyMap[testTenantKey] = "mutated"
	idSlice[0] = 42

	require.True(t, binary.Equal([]byte("key")))
	header, _ = headers.Get("header")
	require.True(t, header.Equal([]byte("value")))
	tenant, _ = properties.Get(testTenantKey)
	require.Equal(t, testTenant, tenant)
	require.Equal(t, uint32(1), ids.At(0))
}

func TestMetadataMutationIsCopyOnWrite(t *testing.T) {
	headers := NewHeaderMap(map[string][]byte{"one": []byte("1")})
	changedHeaders := headers.With("two", []byte("2")).Without("one")
	_, ok := headers.Get("one")
	require.True(t, ok)
	_, ok = headers.Get("two")
	require.False(t, ok)
	_, ok = changedHeaders.Get("one")
	require.False(t, ok)

	properties := NewPropertyMap(map[string]string{"one": "1"})
	changedProperties := properties.With("two", "2").Without("one")
	_, ok = properties.Get("one")
	require.True(t, ok)
	_, ok = properties.Get("two")
	require.False(t, ok)
	_, ok = changedProperties.Get("one")
	require.False(t, ok)

	ids := NewUint32List(1)
	require.Equal(t, []uint32{1}, ids.Slice())
	require.Equal(t, []uint32{1, 2}, ids.Append(2).Slice())
}
