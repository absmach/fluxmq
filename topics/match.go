// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import "strings"

// TopicMatch checks if the topic matches the given filter according to MQTT wildcard rules.
// Rules:
// - filter can contain '+' (single level wildcard) and '#' (multi-level wildcard at end).
// - topic must not contain wildcards.
// - '$' prefix topics are special (no wildcards starting with $ match them unless explicit).
// It walks levels with strings.Cut rather than splitting both sides, so it
// allocates nothing. Queue topic matching calls it once per registered pattern
// on the publish path of every protocol, where two slices per call became two
// per pattern per message.
func TopicMatch(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	if filter == topic {
		return true
	}

	// Special check for '$' topics - wildcards cannot match $ topics unless filter also starts with $
	if topic[0] == '$' {
		if filter[0] != '$' {
			return false
		}
		if firstLevel, _, _ := strings.Cut(filter, "/"); firstLevel == "+" || firstLevel == "#" {
			return false
		}
	}

	remainingTopic := topic
	topicExhausted := false
	for {
		filterLevel, filterRest, filterHasMore := strings.Cut(filter, "/")

		if filterLevel == "#" {
			// Multi-level wildcard matches everything from this point
			return true
		}
		if topicExhausted {
			// Filter has more levels than topic (and it's not #)
			return false
		}

		topicLevel, topicRest, topicHasMore := strings.Cut(remainingTopic, "/")

		// '+' matches any single level.
		if filterLevel != "+" && filterLevel != topicLevel {
			return false
		}

		if !filterHasMore {
			// All filter levels consumed (none were #), so the topic must be
			// consumed too.
			return !topicHasMore
		}

		filter = filterRest
		if topicHasMore {
			remainingTopic = topicRest
			continue
		}
		topicExhausted = true
	}
}
