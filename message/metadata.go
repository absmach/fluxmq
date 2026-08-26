// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import "bytes"

// Binary is an immutable byte string. It owns the bytes supplied at
// construction and never exposes its backing slice, so envelopes can share it
// across clones without allowing one consumer to mutate another's metadata.
type Binary struct {
	value []byte
}

// NewBinary copies value into an immutable byte string.
func NewBinary(value []byte) Binary {
	return Binary{value: bytes.Clone(value)}
}

// Len returns the number of bytes in b.
func (b Binary) Len() int { return len(b.value) }

// Bytes returns an independent mutable copy of b.
func (b Binary) Bytes() []byte { return bytes.Clone(b.value) }

// AppendTo appends b to dst without exposing b's immutable storage.
func (b Binary) AppendTo(dst []byte) []byte { return append(dst, b.value...) }

// Equal reports whether b contains value.
func (b Binary) Equal(value []byte) bool { return bytes.Equal(b.value, value) }

// HeaderMap is an immutable collection of binary header values. With and
// Without implement copy-on-write: readers and envelope clones share the map,
// while only a writer allocating a changed value pays for a copy.
type HeaderMap struct {
	values map[string]Binary
}

// NewHeaderMap copies keys and values from headers.
func NewHeaderMap(headers map[string][]byte) HeaderMap {
	if len(headers) == 0 {
		return HeaderMap{}
	}
	values := make(map[string]Binary, len(headers))
	for key, value := range headers {
		values[key] = NewBinary(value)
	}
	return HeaderMap{values: values}
}

// Len returns the number of headers.
func (h HeaderMap) Len() int { return len(h.values) }

// Get returns an immutable header value.
func (h HeaderMap) Get(key string) (Binary, bool) {
	value, ok := h.values[key]
	return value, ok
}

// Range visits every header until visit returns false.
func (h HeaderMap) Range(visit func(string, Binary) bool) {
	for key, value := range h.values {
		if !visit(key, value) {
			return
		}
	}
}

// Map returns a mutable deep copy of the headers.
func (h HeaderMap) Map() map[string][]byte {
	if len(h.values) == 0 {
		return nil
	}
	result := make(map[string][]byte, len(h.values))
	for key, value := range h.values {
		result[key] = value.Bytes()
	}
	return result
}

// With returns headers with key set to a copied value.
func (h HeaderMap) With(key string, value []byte) HeaderMap {
	values := make(map[string]Binary, len(h.values)+1)
	for existingKey, existingValue := range h.values {
		values[existingKey] = existingValue
	}
	values[key] = NewBinary(value)
	return HeaderMap{values: values}
}

// Without returns headers without key.
func (h HeaderMap) Without(key string) HeaderMap {
	if _, ok := h.values[key]; !ok {
		return h
	}
	if len(h.values) == 1 {
		return HeaderMap{}
	}
	values := make(map[string]Binary, len(h.values)-1)
	for existingKey, value := range h.values {
		if existingKey != key {
			values[existingKey] = value
		}
	}
	return HeaderMap{values: values}
}

// PropertyMap is an immutable collection of string properties. With and
// Without implement copy-on-write on mutation.
type PropertyMap struct {
	values map[string]string
}

// NewPropertyMap copies properties.
func NewPropertyMap(properties map[string]string) PropertyMap {
	if len(properties) == 0 {
		return PropertyMap{}
	}
	values := make(map[string]string, len(properties))
	for key, value := range properties {
		values[key] = value
	}
	return PropertyMap{values: values}
}

// Len returns the number of properties.
func (p PropertyMap) Len() int { return len(p.values) }

// Get returns one property.
func (p PropertyMap) Get(key string) (string, bool) {
	value, ok := p.values[key]
	return value, ok
}

// Range visits every property until visit returns false.
func (p PropertyMap) Range(visit func(string, string) bool) {
	for key, value := range p.values {
		if !visit(key, value) {
			return
		}
	}
}

// Map returns an independent mutable copy of the properties.
func (p PropertyMap) Map() map[string]string {
	if len(p.values) == 0 {
		return nil
	}
	result := make(map[string]string, len(p.values))
	for key, value := range p.values {
		result[key] = value
	}
	return result
}

// With returns properties with key set to value.
func (p PropertyMap) With(key, value string) PropertyMap {
	values := make(map[string]string, len(p.values)+1)
	for existingKey, existingValue := range p.values {
		values[existingKey] = existingValue
	}
	values[key] = value
	return PropertyMap{values: values}
}

// Without returns properties without key.
func (p PropertyMap) Without(key string) PropertyMap {
	if _, ok := p.values[key]; !ok {
		return p
	}
	if len(p.values) == 1 {
		return PropertyMap{}
	}
	values := make(map[string]string, len(p.values)-1)
	for existingKey, value := range p.values {
		if existingKey != key {
			values[existingKey] = value
		}
	}
	return PropertyMap{values: values}
}

// WithoutReserved returns p unchanged when it contains no broker-owned key.
// Otherwise it copies only publisher-owned entries.
func (p PropertyMap) WithoutReserved() PropertyMap {
	for key := range p.values {
		if IsReservedProperty(key) {
			values := make(map[string]string, len(p.values)-1)
			for candidate, value := range p.values {
				if !IsReservedProperty(candidate) {
					values[candidate] = value
				}
			}
			if len(values) == 0 {
				return PropertyMap{}
			}
			return PropertyMap{values: values}
		}
	}
	return p
}

// Uint32List is an immutable uint32 sequence.
type Uint32List struct {
	values []uint32
}

// NewUint32List copies values.
func NewUint32List(values ...uint32) Uint32List {
	if len(values) == 0 {
		return Uint32List{}
	}
	return Uint32List{values: append([]uint32(nil), values...)}
}

// Len returns the number of values.
func (l Uint32List) Len() int { return len(l.values) }

// At returns the value at index. It panics when index is out of range, like a
// slice access.
func (l Uint32List) At(index int) uint32 { return l.values[index] }

// Range visits values in order until visit returns false.
func (l Uint32List) Range(visit func(uint32) bool) {
	for _, value := range l.values {
		if !visit(value) {
			return
		}
	}
}

// Slice returns an independent mutable copy of the values.
func (l Uint32List) Slice() []uint32 { return append([]uint32(nil), l.values...) }

// Append returns a new list containing values after the existing sequence.
func (l Uint32List) Append(values ...uint32) Uint32List {
	if len(values) == 0 {
		return l
	}
	result := make([]uint32, 0, len(l.values)+len(values))
	result = append(result, l.values...)
	result = append(result, values...)
	return Uint32List{values: result}
}

// Optional is an immutable explicitly-present value. It replaces pointer
// optionals whose pointees could otherwise be mutated through a shallow clone.
type Optional[T any] struct {
	value T
	set   bool
}

// Some constructs a present value.
func Some[T any](value T) Optional[T] { return Optional[T]{value: value, set: true} }

// Value returns the contained value and whether it is present.
func (o Optional[T]) Value() (T, bool) { return o.value, o.set }

// IsSet reports whether o contains a value.
func (o Optional[T]) IsSet() bool { return o.set }
