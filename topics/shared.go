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

// ShareSubscriber is one member of a share group, with the QoS that member
// subscribed at.
//
// The QoS belongs to the member rather than to the group: joining a group does
// not change what a client asked for, and delivering above it would hand a
// client packets it never agreed to acknowledge.
type ShareSubscriber struct {
	ClientID string
	QoS      byte
}

// ShareGroup represents a group of subscribers sharing a subscription.
type ShareGroup struct {
	Name        string            // Share group name
	TopicFilter string            // The topic filter being shared
	Subscribers []ShareSubscriber // Members of this group
	lastIndex   int               // For round-robin distribution
}

// NextSubscriber returns the next subscriber in round-robin fashion.
// Returns empty string if no subscribers.
func (g *ShareGroup) NextSubscriber() string {
	if len(g.Subscribers) == 0 {
		return ""
	}

	subscriber := g.Subscribers[g.lastIndex]
	g.lastIndex = (g.lastIndex + 1) % len(g.Subscribers)
	return subscriber.ClientID
}

// SubscriberAt returns the member offset positions after rotation, wrapping
// around the group. ok is false once offset reaches the group size, so a caller
// stepping offset up from 1 tries every remaining member exactly once and then
// stops.
//
// Membership can change between the rotation being chosen and this call. A
// member that joined or left in the meantime is picked up or skipped; for a
// fallback walk that is harmless, and bounding the walk by the current size is
// what keeps it terminating.
func (g *ShareGroup) SubscriberAt(rotation, offset int) (ShareSubscriber, bool) {
	size := len(g.Subscribers)
	if size == 0 || offset < 0 || offset >= size {
		return ShareSubscriber{}, false
	}

	return g.Subscribers[(rotation+offset)%size], true
}

// AddSubscriber adds a subscriber to the group, or updates the QoS of one
// already in it — re-subscribing at a different QoS is how a client changes
// what it agreed to receive. Returns true if the subscriber was added.
func (g *ShareGroup) AddSubscriber(clientID string, qos byte) bool {
	for i := range g.Subscribers {
		if g.Subscribers[i].ClientID == clientID {
			g.Subscribers[i].QoS = qos
			return false
		}
	}

	g.Subscribers = append(g.Subscribers, ShareSubscriber{ClientID: clientID, QoS: qos})
	return true
}

// RemoveSubscriber removes a subscriber from the group.
// Returns true if the subscriber was found and removed.
func (g *ShareGroup) RemoveSubscriber(clientID string) bool {
	for i, sub := range g.Subscribers {
		if sub.ClientID == clientID {
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
