// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package consumer provides consumer group management with routing key filtering
// and work stealing support for the log-based queue model.
package consumer

import (
	"regexp"
	"strings"
)

// Filter represents a compiled routing key filter.
// It matches routing keys against a subscription pattern.
type Filter struct {
	pattern   string         // Original pattern (e.g., "images/+/png")
	regex     *regexp.Regexp // Compiled regex for matching
	isExact   bool           // True if pattern has no wildcards
	matchAll  bool           // True if pattern matches everything (# or empty)
}

// NewFilter creates a new filter from a subscription pattern.
// Patterns support MQTT-style wildcards:
//   - + matches exactly one level
//   - # matches zero or more levels (must be last)
//
// Examples:
//   - "images" matches only "images"
//   - "images/+" matches "images/png", "images/jpg" but not "images" or "images/a/b"
//   - "images/#" matches "images", "images/png", "images/a/b/c"
//   - "+/png" matches "images/png", "photos/png" but not "png" or "a/b/png"
//   - "#" matches everything
func NewFilter(pattern string) *Filter {
	f := &Filter{
		pattern: pattern,
	}

	// Empty pattern or "#" matches everything
	if pattern == "" || pattern == "#" {
		f.matchAll = true
		return f
	}

	// Check if pattern has wildcards
	if !strings.Contains(pattern, "+") && !strings.Contains(pattern, "#") {
		f.isExact = true
		return f
	}

	// Compile pattern to regex
	f.regex = compilePattern(pattern)
	return f
}

// Matches returns true if the routing key matches the filter pattern.
func (f *Filter) Matches(routingKey string) bool {
	if f.matchAll {
		return true
	}

	if f.isExact {
		return routingKey == f.pattern
	}

	if f.regex != nil {
		return f.regex.MatchString(routingKey)
	}

	return false
}

// Pattern returns the original pattern string.
func (f *Filter) Pattern() string {
	return f.pattern
}

// IsWildcard returns true if the filter contains wildcards.
func (f *Filter) IsWildcard() bool {
	return !f.isExact
}

// compilePattern converts an MQTT-style pattern to a regex.
func compilePattern(pattern string) *regexp.Regexp {
	// Escape regex special characters except our wildcards
	escaped := regexp.QuoteMeta(pattern)

	// Replace wildcards with regex equivalents
	// \+ -> [^/]+ (one level, no slashes) - + is escaped by QuoteMeta
	// # -> .* (any levels) - # is NOT escaped by QuoteMeta, so match directly
	escaped = strings.ReplaceAll(escaped, `\+`, `[^/]+`)
	escaped = strings.ReplaceAll(escaped, `#`, `.*`)

	// Anchor the pattern
	regexStr := "^" + escaped + "$"

	// Compile (should never fail for valid patterns)
	regex, err := regexp.Compile(regexStr)
	if err != nil {
		// Fallback to exact match on compile error
		return nil
	}

	return regex
}

// ParseQueueSubscription parses a queue subscription topic into queue root and filter pattern.
// Examples:
//   - "$queue/tasks" -> root="$queue/tasks", pattern=""
//   - "$queue/tasks/images" -> root="$queue/tasks", pattern="images"
//   - "$queue/tasks/images/+" -> root="$queue/tasks", pattern="images/+"
//   - "$queue/tasks/#" -> root="$queue/tasks", pattern="#"
func ParseQueueSubscription(topic string) (queueRoot string, filterPattern string) {
	if !strings.HasPrefix(topic, "$queue/") {
		return "", ""
	}

	// Remove $queue/ prefix
	rest := strings.TrimPrefix(topic, "$queue/")
	if rest == "" {
		return "", ""
	}

	// Split into queue name and routing pattern
	parts := strings.SplitN(rest, "/", 2)
	queueRoot = "$queue/" + parts[0]

	if len(parts) > 1 {
		filterPattern = parts[1]
	}

	return queueRoot, filterPattern
}

// ExtractRoutingKey extracts the routing key portion from a publish topic.
// The queue root is used to determine where the routing key starts.
// Examples:
//   - topic="$queue/tasks/images/png", root="$queue/tasks" -> "images/png"
//   - topic="$queue/tasks", root="$queue/tasks" -> ""
func ExtractRoutingKey(topic, queueRoot string) string {
	if !strings.HasPrefix(topic, queueRoot) {
		return ""
	}

	// Remove queue root prefix
	rest := strings.TrimPrefix(topic, queueRoot)
	// Remove leading slash
	rest = strings.TrimPrefix(rest, "/")

	return rest
}

// MatchesSubscription checks if a publish topic matches a subscription pattern.
// This handles both exact matches and wildcard patterns.
func MatchesSubscription(publishTopic, subscriptionPattern string) bool {
	// Parse subscription
	subRoot, subFilter := ParseQueueSubscription(subscriptionPattern)
	if subRoot == "" {
		return false
	}

	// Parse publish topic
	pubRoot, pubRoutingKey := ParseQueueSubscription(publishTopic)
	if pubRoot == "" {
		return false
	}

	// Queue roots must match
	if subRoot != pubRoot {
		return false
	}

	// Create filter and check routing key
	filter := NewFilter(subFilter)
	return filter.Matches(pubRoutingKey)
}

// FilterSet manages multiple filters for a consumer that may have multiple subscriptions.
type FilterSet struct {
	filters []*Filter
}

// NewFilterSet creates a new filter set.
func NewFilterSet() *FilterSet {
	return &FilterSet{
		filters: make([]*Filter, 0),
	}
}

// Add adds a filter pattern to the set.
func (fs *FilterSet) Add(pattern string) {
	fs.filters = append(fs.filters, NewFilter(pattern))
}

// Matches returns true if any filter in the set matches the routing key.
func (fs *FilterSet) Matches(routingKey string) bool {
	for _, f := range fs.filters {
		if f.Matches(routingKey) {
			return true
		}
	}
	return false
}

// Clear removes all filters from the set.
func (fs *FilterSet) Clear() {
	fs.filters = fs.filters[:0]
}

// Len returns the number of filters in the set.
func (fs *FilterSet) Len() int {
	return len(fs.filters)
}
