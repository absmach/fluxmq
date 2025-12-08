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

	// Special check for '$' topics
	if strings.HasPrefix(topic, "$") {
		if len(filter) == 0 || filter[0] != '$' {
			return false // Must explicitly match starting '$'
		}
		// Even if filter starts with $, wildcards cannot match the first level if it starts with $?
		// Spec says: "Wildcard characters can be used in Topic Filters, but they MUST NOT be used as the first character of a Topic Filter when the Topic Name starts with a $ character."
		// Wait, actually "The Server MUST NOT match a subscription filter ... to a Topic Name starting with a $ character ... unless the filter also starts with a $ character and the first wildcard is later".
		if filterLevels[0] == "+" || filterLevels[0] == "#" {
			return false
		}
	}

	for i, fLevel := range filterLevels {
		// Multi-level wildcard MUST be the last level
		if fLevel == "#" {
			// # matches parent and all children
			return true
		}

		if i >= len(topicLevels) {
			// Filter is longer than topic
			// E.g. filter "a/+" matches "a" -> No, "a/+" matches "a/b".
			// But "a/#" matches "a".
			// If we are here, fLevel is not # (handled above)
			// So mismatch
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
