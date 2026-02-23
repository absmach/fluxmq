// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// Common validation errors.
var ErrInvalidTopicName = errors.New("invalid topic name: contains wildcards or illegal characters")
var ErrInvalidTopicFilter = errors.New("invalid topic filter: malformed wildcard or illegal characters")

// Validator is an interface for packets that can validate themselves.
type Validator interface {
	Validate() error
}

// ValidateTopicName checks if the topic name is valid for PUBLISH (no wildcards).
func ValidateTopicName(topic string) error {
	if topic == "" {
		return ErrInvalidTopicName
	}
	// "The Topic Name ... MUST NOT contain wildcard characters"
	if strings.Contains(topic, "+") || strings.Contains(topic, "#") {
		return ErrInvalidTopicName
	}
	// Must be valid UTF-8
	if !utf8.ValidString(topic) {
		return ErrInvalidTopicName
	}
	// Check for null character
	if strings.Contains(topic, "\u0000") {
		return ErrInvalidTopicName
	}
	return nil
}

// ValidateTopicFilter checks if the topic filter is valid for SUBSCRIBE.
// Supports MQTT wildcards and shared subscriptions ($share/{group}/{filter}).
func ValidateTopicFilter(filter string) error {
	if filter == "" {
		return ErrInvalidTopicFilter
	}
	if !utf8.ValidString(filter) || strings.Contains(filter, "\u0000") {
		return ErrInvalidTopicFilter
	}

	// Shared subscription format: $share/{group}/{filter}
	shareName, topicFilter, isShared := ParseShared(filter)
	if IsShared(filter) && !isShared {
		return ErrInvalidTopicFilter
	}
	if isShared {
		if shareName == "" || strings.ContainsAny(shareName, "+#") {
			return ErrInvalidTopicFilter
		}
		if topicFilter == "" {
			return ErrInvalidTopicFilter
		}
		filter = topicFilter
	}

	levels := strings.Split(filter, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") {
			// '#' must occupy the entire final level.
			if level != "#" || i != len(levels)-1 {
				return ErrInvalidTopicFilter
			}
		}
		if strings.Contains(level, "+") {
			// '+' must occupy the entire level.
			if level != "+" {
				return ErrInvalidTopicFilter
			}
		}
	}

	return nil
}
