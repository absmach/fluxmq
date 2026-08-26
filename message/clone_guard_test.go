// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// Metadata values are copied directly by Clone. Keep mutable reference types
// behind immutable value wrappers so adding a new exported slice, map, or
// pointer cannot silently reintroduce aliases between readers.
func TestMetadataDoesNotExposeMutableReferenceFields(t *testing.T) {
	for name, value := range map[string]any{
		"PublisherMetadata": PublisherMetadata{},
		"DeliveryMetadata":  DeliveryMetadata{},
		"QueueMetadata":     QueueMetadata{},
	} {
		t.Run(name, func(t *testing.T) {
			typ := reflect.TypeOf(value)
			for i := range typ.NumField() {
				field := typ.Field(i)
				if !field.IsExported() {
					continue
				}
				switch field.Type.Kind() {
				case reflect.Map, reflect.Pointer, reflect.Slice:
					t.Fatalf("%s.%s exposes mutable %s; use an immutable metadata value", name, field.Name, field.Type.Kind())
				}
			}
		})
	}
}

func TestCloneCarriesEveryMetadataField(t *testing.T) {
	envelope := conformanceEnvelope()
	defer Release(envelope)

	clone := envelope.Clone()
	defer Release(clone)

	require.Equal(t, envelope.PublisherMeta, clone.PublisherMeta)
	require.Equal(t, envelope.BrokerMeta, clone.BrokerMeta)
}
