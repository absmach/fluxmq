// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import "strings"

// ParseShared parses a shared subscription filter.
// Format: $share/{ShareName}/{TopicFilter}
// Returns: shareName, topicFilter, isShared
//
// Examples:
//   - "$share/group1/sensors/#" -> ("group1", "sensors/#", true)
//   - "sensors/#" -> ("", "sensors/#", false)
func ParseShared(filter string) (shareName, topicFilter string, isShared bool) {
	if !strings.HasPrefix(filter, "$share/") {
		return "", filter, false
	}

	// Remove "$share/" prefix
	rest := filter[7:]

	// Split on first '/' to separate share name from topic filter
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		// Invalid shared subscription format
		return "", filter, false
	}

	return parts[0], parts[1], true
}

// IsShared returns true if the filter is a shared subscription.
func IsShared(filter string) bool {
	return strings.HasPrefix(filter, "$share/")
}

// ShareGroup represents a group of subscribers sharing a subscription.
type ShareGroup struct {
	Name        string   // Share group name
	TopicFilter string   // The topic filter being shared
	Subscribers []string // List of client IDs in this group
	lastIndex   int      // For round-robin distribution
}

// NextSubscriber returns the next subscriber in round-robin fashion.
// Returns empty string if no subscribers.
func (g *ShareGroup) NextSubscriber() string {
	clientID, _, _ := g.Select()
	return clientID
}

// Select returns the next subscriber in round-robin fashion along with the
// rotation it was taken from, and advances the cursor by one. A caller that
// cannot deliver to the returned member passes the rotation to SubscriberAt to
// walk the rest of the group.
func (g *ShareGroup) Select() (clientID string, rotation int, ok bool) {
	if len(g.Subscribers) == 0 {
		return "", 0, false
	}

	rotation = g.lastIndex
	g.lastIndex = (g.lastIndex + 1) % len(g.Subscribers)
	return g.Subscribers[rotation], rotation, true
}

// SubscriberAt returns the member offset positions after rotation, wrapping
// around the group. ok is false once offset reaches the group size, so a caller
// stepping offset up from 1 tries every remaining member exactly once and then
// stops.
//
// Membership can change between the Select that produced rotation and this
// call. A member that joined or left in the meantime is picked up or skipped;
// for a fallback walk that is harmless, and bounding the walk by the current
// size is what keeps it terminating.
func (g *ShareGroup) SubscriberAt(rotation, offset int) (string, bool) {
	size := len(g.Subscribers)
	if size == 0 || offset < 0 || offset >= size {
		return "", false
	}

	return g.Subscribers[(rotation+offset)%size], true
}

// AddSubscriber adds a subscriber to the group if not already present.
// Returns true if the subscriber was added.
func (g *ShareGroup) AddSubscriber(clientID string) bool {
	// Check if already exists
	for _, sub := range g.Subscribers {
		if sub == clientID {
			return false
		}
	}

	g.Subscribers = append(g.Subscribers, clientID)
	return true
}

// RemoveSubscriber removes a subscriber from the group.
// Returns true if the subscriber was found and removed.
func (g *ShareGroup) RemoveSubscriber(clientID string) bool {
	for i, sub := range g.Subscribers {
		if sub == clientID {
			// Remove by swapping with last element and truncating
			g.Subscribers[i] = g.Subscribers[len(g.Subscribers)-1]
			g.Subscribers = g.Subscribers[:len(g.Subscribers)-1]

			// Adjust lastIndex if needed
			if g.lastIndex >= len(g.Subscribers) && len(g.Subscribers) > 0 {
				g.lastIndex = 0
			}

			return true
		}
	}
	return false
}

// IsEmpty returns true if the group has no subscribers.
func (g *ShareGroup) IsEmpty() bool {
	return len(g.Subscribers) == 0
}
