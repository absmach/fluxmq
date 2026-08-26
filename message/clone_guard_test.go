// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Clone skips a namespace whose guard reports it empty. The guards enumerate
// fields by hand, so a field added to the struct and not to its guard is
// silently dropped by every clone of a message that carries only that field —
// which is how a publisher's message-id stopped surviving delivery. This walks
// the structs by reflection so the next added field fails here instead.
func TestCloneCarriesEverySingleUserField(t *testing.T) {
	typ := reflect.TypeOf(PublisherMetadata{})

	for i := range typ.NumField() {
		field := typ.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			envelope := Acquire()
			defer Release(envelope)

			target := reflect.ValueOf(&envelope.PublisherMeta).Elem().FieldByName(field.Name)
			require.True(t, target.CanSet(), "field %s is not settable", field.Name)
			target.Set(nonZeroValue(t, field.Type))

			clone := envelope.Clone()
			defer Release(clone)

			got := reflect.ValueOf(&clone.PublisherMeta).Elem().FieldByName(field.Name)
			require.Falsef(t, got.IsZero(),
				"Clone dropped UserMetadata.%s: hasUserMetadata does not test it", field.Name)
		})
	}
}

// The same guard, for the other namespace Clone gates on a hand-written helper.
// Source, Transfer and Trace are gated on `!= (T{})` instead, which the compiler
// keeps correct as long as they stay comparable — the two below are the ones
// that enumerate fields and can therefore fall behind the struct.
func TestCloneCarriesEverySingleDeliveryField(t *testing.T) {
	typ := reflect.TypeOf(DeliveryMetadata{})

	for i := range typ.NumField() {
		field := typ.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			envelope := Acquire()
			defer Release(envelope)

			target := reflect.ValueOf(&envelope.BrokerMeta.Delivery).Elem().FieldByName(field.Name)
			require.True(t, target.CanSet(), "field %s is not settable", field.Name)
			target.Set(nonZeroValue(t, field.Type))

			clone := envelope.Clone()
			defer Release(clone)

			got := reflect.ValueOf(&clone.BrokerMeta.Delivery).Elem().FieldByName(field.Name)
			require.Falsef(t, got.IsZero(),
				"Clone dropped DeliveryMetadata.%s: hasDeliveryMetadata does not test it", field.Name)
		})
	}
}

// Source, Transfer and Trace are gated on a struct comparison rather than a
// helper, which only stays correct while they stay comparable. A slice or map
// field would make the guard fail to compile — this states that the compiler is
// the guard, so nobody replaces it with a hand-written helper without noticing
// what they are giving up.
func TestComparableNamespacesGateCloneByStructEquality(t *testing.T) {
	for name, value := range map[string]any{
		"SourceMetadata":   SourceMetadata{},
		"TransferMetadata": TransferMetadata{},
		"TraceMetadata":    TraceMetadata{},
	} {
		t.Run(name, func(t *testing.T) {
			require.True(t, reflect.TypeOf(value).Comparable(),
				"%s is gated by `!= (T{})` in Clone, which requires it to stay comparable", name)
		})
	}
}

func nonZeroValue(t *testing.T, typ reflect.Type) reflect.Value {
	t.Helper()

	if typ == reflect.TypeOf(time.Time{}) {
		return reflect.ValueOf(time.Unix(1700000000, 0).UTC())
	}

	switch typ.Kind() {
	case reflect.String:
		return reflect.ValueOf("set").Convert(typ)
	case reflect.Slice:
		value := reflect.MakeSlice(typ, 1, 1)
		value.Index(0).Set(nonZeroValue(t, typ.Elem()))
		return value
	case reflect.Map:
		value := reflect.MakeMap(typ)
		value.SetMapIndex(nonZeroValue(t, typ.Key()), nonZeroValue(t, typ.Elem()))
		return value
	case reflect.Pointer:
		value := reflect.New(typ.Elem())
		value.Elem().Set(nonZeroValue(t, typ.Elem()))
		return value
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return reflect.ValueOf(uint64(7)).Convert(typ)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if typ == reflect.TypeOf(time.Duration(0)) {
			return reflect.ValueOf(time.Second).Convert(typ)
		}
		return reflect.ValueOf(int64(7)).Convert(typ)
	case reflect.Bool:
		return reflect.ValueOf(true).Convert(typ)
	default:
		t.Fatalf("no non-zero value for %s (kind %s)", typ, typ.Kind())
		return reflect.Value{}
	}
}
