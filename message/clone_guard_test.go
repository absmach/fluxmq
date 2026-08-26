// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Metadata values are copied directly by Clone, so an exported map, slice or
// pointer anywhere under Envelope would be shared between every clone of a
// message — the aliasing the immutable metadata values exist to prevent.
//
// This walks Envelope's metadata recursively rather than naming the structs it
// checks. The hand-written list it replaces covered three of the eight structs
// Clone copies, so a slice added to TransferMetadata or TraceMetadata would
// have reintroduced sharing with nothing failing.
func TestMetadataDoesNotExposeMutableReferenceFields(t *testing.T) {
	envelope := reflect.TypeOf(Envelope{})
	for _, name := range []string{"PublisherMeta", "BrokerMeta"} {
		field, ok := envelope.FieldByName(name)
		require.Truef(t, ok, "Envelope.%s is gone; this guard no longer covers what Clone copies", name)
		t.Run(name, func(t *testing.T) {
			requireImmutableFields(t, field.Type, "Envelope."+name)
		})
	}
}

// requireImmutableFields fails on any exported map, slice or pointer reachable
// from typ, descending into nested metadata structs.
//
// The immutable collections are exempt by design: their mutable storage is
// unexported and no method hands out a reference to it, which is the whole
// point of them. time.Time is exempt for the same reason.
func requireImmutableFields(t *testing.T, typ reflect.Type, path string) {
	t.Helper()

	switch typ {
	case reflect.TypeOf(Binary{}), reflect.TypeOf(HeaderMap{}),
		reflect.TypeOf(PropertyMap{}), reflect.TypeOf(Uint32List{}),
		reflect.TypeOf(time.Time{}):
		return
	}
	// Optional[T] holds its value inline, so its safety is its parameter's.
	if strings.HasPrefix(typ.Name(), "Optional[") {
		field, ok := typ.FieldByName("value")
		require.Truef(t, ok, "%s: Optional lost its value field", path)
		requireImmutableFields(t, field.Type, path+".value")
		return
	}
	if typ.Kind() != reflect.Struct {
		return
	}

	for i := range typ.NumField() {
		field := typ.Field(i)
		fieldPath := path + "." + field.Name
		if !field.IsExported() {
			continue
		}
		switch field.Type.Kind() {
		case reflect.Map, reflect.Pointer, reflect.Slice:
			t.Fatalf("%s exposes mutable %s; use an immutable metadata value", fieldPath, field.Type.Kind())
		case reflect.Struct:
			requireImmutableFields(t, field.Type, fieldPath)
		}
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
