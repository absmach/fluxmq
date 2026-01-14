// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "sync"

// topicAliasManager manages bidirectional topic alias mappings for MQTT 5.0.
// It handles both outbound aliases (client -> server) and inbound aliases (server -> client).
type topicAliasManager struct {
	// Outbound (client sending to server)
	outboundTopicToAlias map[string]uint16 // topic -> alias
	outboundAliasToTopic map[uint16]string // alias -> topic
	outboundNextAlias    uint16            // next alias to assign
	outboundMaximum      uint16            // maximum aliases server accepts
	outboundMu           sync.RWMutex

	// Inbound (server sending to client)
	inboundAliasToTopic map[uint16]string // alias -> topic
	inboundMaximum      uint16            // maximum aliases client accepts
	inboundMu           sync.RWMutex
}

// newTopicAliasManager creates a new topic alias manager.
// clientMax is the maximum aliases the client accepts from the server.
// serverMax is the maximum aliases the server accepts from the client (from CONNACK).
func newTopicAliasManager(clientMax, serverMax uint16) *topicAliasManager {
	return &topicAliasManager{
		outboundTopicToAlias: make(map[string]uint16),
		outboundAliasToTopic: make(map[uint16]string),
		outboundNextAlias:    1,
		outboundMaximum:      serverMax,
		inboundAliasToTopic:  make(map[uint16]string),
		inboundMaximum:       clientMax,
	}
}

// getOrAssignOutbound gets an existing alias for a topic or assigns a new one.
// Returns (alias, isNew, ok).
// - If topic already has an alias: (alias, false, true)
// - If topic can be assigned new alias: (newAlias, true, true)
// - If aliases disabled or limit reached: (0, false, false)
func (m *topicAliasManager) getOrAssignOutbound(topic string) (uint16, bool, bool) {
	if m.outboundMaximum == 0 {
		return 0, false, false // Aliases disabled
	}

	m.outboundMu.Lock()
	defer m.outboundMu.Unlock()

	// Check if topic already has an alias
	if alias, exists := m.outboundTopicToAlias[topic]; exists {
		return alias, false, true
	}

	// Assign new alias if under limit
	if m.outboundNextAlias <= m.outboundMaximum {
		alias := m.outboundNextAlias
		m.outboundNextAlias++
		m.outboundTopicToAlias[topic] = alias
		m.outboundAliasToTopic[alias] = topic
		return alias, true, true
	}

	// Limit reached, cannot assign more aliases
	return 0, false, false
}

// registerInbound registers an inbound alias mapping from the server.
// Called when receiving a PUBLISH with both topic and alias.
func (m *topicAliasManager) registerInbound(alias uint16, topic string) bool {
	if m.inboundMaximum == 0 || alias == 0 || alias > m.inboundMaximum {
		return false
	}

	m.inboundMu.Lock()
	defer m.inboundMu.Unlock()
	m.inboundAliasToTopic[alias] = topic
	return true
}

// resolveInbound resolves an inbound alias to a topic.
// Called when receiving a PUBLISH with only an alias (no topic).
func (m *topicAliasManager) resolveInbound(alias uint16) (string, bool) {
	m.inboundMu.RLock()
	defer m.inboundMu.RUnlock()
	topic, ok := m.inboundAliasToTopic[alias]
	return topic, ok
}

// reset clears all alias mappings.
// Called on disconnect.
func (m *topicAliasManager) reset() {
	m.outboundMu.Lock()
	m.outboundTopicToAlias = make(map[string]uint16)
	m.outboundAliasToTopic = make(map[uint16]string)
	m.outboundNextAlias = 1
	m.outboundMu.Unlock()

	m.inboundMu.Lock()
	m.inboundAliasToTopic = make(map[uint16]string)
	m.inboundMu.Unlock()
}

// updateServerMaximum updates the server's maximum alias limit.
// Called after receiving CONNACK.
func (m *topicAliasManager) updateServerMaximum(serverMax uint16) {
	m.outboundMu.Lock()
	defer m.outboundMu.Unlock()

	m.outboundMaximum = serverMax

	// If new limit is lower, clear all aliases beyond the new limit
	if serverMax < m.outboundNextAlias {
		// Rebuild maps to remove aliases beyond new limit
		newTopicToAlias := make(map[string]uint16)
		newAliasToTopic := make(map[uint16]string)

		for topic, alias := range m.outboundTopicToAlias {
			if alias <= serverMax {
				newTopicToAlias[topic] = alias
				newAliasToTopic[alias] = topic
			}
		}

		m.outboundTopicToAlias = newTopicToAlias
		m.outboundAliasToTopic = newAliasToTopic

		// Reset next alias to be after the highest remaining alias
		maxAlias := uint16(0)
		for alias := range m.outboundAliasToTopic {
			if alias > maxAlias {
				maxAlias = alias
			}
		}
		m.outboundNextAlias = maxAlias + 1
	}
}

// stats returns statistics about alias usage.
func (m *topicAliasManager) stats() (outboundCount, inboundCount int) {
	m.outboundMu.RLock()
	outboundCount = len(m.outboundTopicToAlias)
	m.outboundMu.RUnlock()

	m.inboundMu.RLock()
	inboundCount = len(m.inboundAliasToTopic)
	m.inboundMu.RUnlock()

	return outboundCount, inboundCount
}
