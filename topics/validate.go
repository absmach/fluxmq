package topics

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// Common validation errors.
var ErrInvalidTopicName = errors.New("invalid topic name: contains wildcards or illegal characters")

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
