package cluster

import "strings"

// topicMatchesFilter checks if a topic matches an MQTT subscription filter.
// Supports wildcards: + (single-level) and # (multi-level).
func topicMatchesFilter(topic, filter string) bool {
	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")

	return matchLevels(topicLevels, filterLevels, 0, 0)
}

func matchLevels(topic, filter []string, ti, fi int) bool {
	// Both exhausted - match
	if ti == len(topic) && fi == len(filter) {
		return true
	}

	// Filter exhausted but topic remains - no match
	if fi == len(filter) {
		return false
	}

	// Multi-level wildcard '#' matches everything remaining
	if filter[fi] == "#" {
		return true
	}

	// Topic exhausted but filter remains (and it's not #) - no match
	if ti == len(topic) {
		return false
	}

	// Single-level wildcard '+' or exact match
	if filter[fi] == "+" || filter[fi] == topic[ti] {
		return matchLevels(topic, filter, ti+1, fi+1)
	}

	return false
}
