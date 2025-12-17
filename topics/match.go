// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import "strings"

// TopicMatch checks if the topic matches the given filter according to MQTT wildcard rules.
// Rules:
// - filter can contain '+' (single level wildcard) and '#' (multi-level wildcard at end).
// - topic must not contain wildcards.
// - '$' prefix topics are special (no wildcards starting with $ match them unless explicit).
func TopicMatch(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	if filter == topic {
		return true
	}

	filterLevels := strings.Split(filter, "/")
	topicLevels := strings.Split(topic, "/")

	// Special check for '$' topics - wildcards cannot match $ topics unless filter also starts with $
	if strings.HasPrefix(topic, "$") {
		if len(filter) == 0 || filter[0] != '$' {
			return false
		}
		if filterLevels[0] == "+" || filterLevels[0] == "#" {
			return false
		}
	}

	for i, fLevel := range filterLevels {
		if fLevel == "#" {
			// Multi-level wildcard matches everything from this point
			return true
		}

		if i >= len(topicLevels) {
			// Filter has more levels than topic (and it's not #)
			return false
		}

		tLevel := topicLevels[i]

		if fLevel == "+" {
			// Matches any single level
			continue
		}

		if fLevel != tLevel {
			return false
		}
	}

	// If we consumed all filter levels (none were #), we must have consumed all topic levels
	return len(filterLevels) == len(topicLevels)
}
