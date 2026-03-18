// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"maps"
	"reflect"
	"strings"
)

// ReloadClass classifies how a config field change should be handled.
type ReloadClass int

const (
	// RestartRequired means the field cannot be changed at runtime.
	RestartRequired ReloadClass = iota
	// RuntimeSafe means the field can be changed at runtime without restart.
	RuntimeSafe
)

func (c ReloadClass) String() string {
	switch c {
	case RuntimeSafe:
		return "runtime_safe"
	case RestartRequired:
		return "restart_required"
	default:
		return "unknown"
	}
}

// FieldChange describes a single config field that changed between two configs.
type FieldChange struct {
	Path     string
	OldValue any
	NewValue any
	Class    ReloadClass
	Reason   string // human-readable reason for restart-required fields
}

// DiffResult holds the result of comparing two configs.
type DiffResult struct {
	RuntimeSafe     []FieldChange
	RestartRequired []FieldChange
}

// HasChanges reports whether any fields changed.
func (d DiffResult) HasChanges() bool {
	return len(d.RuntimeSafe) > 0 || len(d.RestartRequired) > 0
}

// runtimeSafeFields contains fields that can be changed at runtime.
var runtimeSafeFields = map[string]struct{}{
	// Log — safe to change at runtime.
	"Log.Level":  {},
	"Log.Format": {},

	// Rate limits — safe to change via atomic swap of rate limiter.
	"RateLimit.Enabled":                    {},
	"RateLimit.Connection.Enabled":         {},
	"RateLimit.Connection.Rate":            {},
	"RateLimit.Connection.Burst":           {},
	"RateLimit.Connection.CleanupInterval": {},
	"RateLimit.Message.Enabled":            {},
	"RateLimit.Message.Rate":               {},
	"RateLimit.Message.Burst":              {},
	"RateLimit.Subscribe.Enabled":          {},
	"RateLimit.Subscribe.Rate":             {},
	"RateLimit.Subscribe.Burst":            {},

	// Broker tuning (implemented): MaxQoS.
	"Broker.MaxQoS": {},
}

// fieldClassification maps each leaf config field path to its reload
// classification. Fields default to RestartRequired unless listed above.
var fieldClassification = buildFieldClassification()

func buildFieldClassification() map[string]ReloadClass {
	classification := make(map[string]ReloadClass)

	for _, path := range LeafFieldPaths() {
		classification[path] = RestartRequired
	}
	for path := range runtimeSafeFields {
		classification[path] = RuntimeSafe
	}

	return classification
}

// restartReasons provides human-readable reasons for restart-required fields.
var restartReasons = map[string]string{
	"Server":       "listener addresses and TLS config require restart",
	"Storage":      "storage backend cannot be changed at runtime",
	"Cluster":      "cluster topology requires restart",
	"Session":      "session config affects existing connections",
	"Webhook":      "webhook worker pool requires drain/restart",
	"QueueManager": "queue manager config requires restart",
	"Queues":       "queue definitions require restart",
	"Auth":         "auth config affects connection-level behavior",
	"Broker":       "this broker setting requires restart",
}

// ClassifyField returns the reload classification for a config field path.
// Unknown fields default to RestartRequired.
func ClassifyField(path string) ReloadClass {
	if class, ok := fieldClassification[path]; ok {
		return class
	}
	return RestartRequired
}

// AllClassifiedFields returns a copy of the field classification registry.
func AllClassifiedFields() map[string]ReloadClass {
	result := make(map[string]ReloadClass, len(fieldClassification))
	maps.Copy(result, fieldClassification)
	return result
}

// Diff compares two configs and returns classified field changes.
func Diff(old, new *Config) DiffResult {
	var result DiffResult
	diffStructs(reflect.ValueOf(old).Elem(), reflect.ValueOf(new).Elem(), "", &result)
	return result
}

func diffStructs(old, new reflect.Value, prefix string, result *DiffResult) {
	t := old.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}

		oldField := old.Field(i)
		newField := new.Field(i)

		if field.Type.Kind() == reflect.Struct && isProjectStruct(field.Type) {
			diffStructs(oldField, newField, path, result)
			continue
		}

		if !reflect.DeepEqual(oldField.Interface(), newField.Interface()) {
			addChange(path, oldField.Interface(), newField.Interface(), result)
		}
	}
}

func addChange(path string, oldVal, newVal any, result *DiffResult) {
	class := ClassifyField(path)
	change := FieldChange{
		Path:     path,
		OldValue: oldVal,
		NewValue: newVal,
		Class:    class,
	}

	if class == RestartRequired {
		change.Reason = reasonForPath(path)
		result.RestartRequired = append(result.RestartRequired, change)
	} else {
		result.RuntimeSafe = append(result.RuntimeSafe, change)
	}
}

// reasonForPath returns a restart reason by matching the top-level config section.
func reasonForPath(path string) string {
	for prefix, reason := range restartReasons {
		if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
			if len(path) == len(prefix) || path[len(prefix)] == '.' {
				return reason
			}
		}
	}
	return fmt.Sprintf("field %q requires restart", path)
}

// LeafFieldPaths returns all leaf field paths in the Config struct.
// Used by tests to verify the classification registry is complete.
func LeafFieldPaths() []string {
	var paths []string
	collectLeafPaths(reflect.TypeFor[Config](), "", &paths)
	return paths
}

func collectLeafPaths(t reflect.Type, prefix string, paths *[]string) {
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}

		ft := field.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		if ft.Kind() == reflect.Struct && isProjectStruct(ft) {
			collectLeafPaths(ft, path, paths)
		} else {
			*paths = append(*paths, path)
		}
	}
}

const projectModule = "github.com/absmach/fluxmq"

// isProjectStruct returns true for struct types defined within the fluxmq module.
// These are recursed into during diff/leaf enumeration. External types (e.g.,
// time.Duration) are treated as leaf values.
func isProjectStruct(t reflect.Type) bool {
	pkg := t.PkgPath()
	if pkg == "" {
		return false
	}
	return strings.HasPrefix(pkg, projectModule)
}
