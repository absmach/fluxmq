// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"
)

func TestTopicAliasManager_OutboundAssignment(t *testing.T) {
	tam := newTopicAliasManager(10, 5) // client accepts 10, server accepts 5

	// First assignment
	alias1, isNew1, ok1 := tam.getOrAssignOutbound("sensors/temperature")
	if !ok1 {
		t.Fatal("Expected successful assignment")
	}
	if !isNew1 {
		t.Error("Expected new alias")
	}
	if alias1 != 1 {
		t.Errorf("Expected alias 1, got %d", alias1)
	}

	// Second assignment (different topic)
	alias2, isNew2, ok2 := tam.getOrAssignOutbound("sensors/humidity")
	if !ok2 {
		t.Fatal("Expected successful assignment")
	}
	if !isNew2 {
		t.Error("Expected new alias")
	}
	if alias2 != 2 {
		t.Errorf("Expected alias 2, got %d", alias2)
	}

	// Reuse existing alias
	alias3, isNew3, ok3 := tam.getOrAssignOutbound("sensors/temperature")
	if !ok3 {
		t.Fatal("Expected successful reuse")
	}
	if isNew3 {
		t.Error("Expected existing alias")
	}
	if alias3 != 1 {
		t.Errorf("Expected alias 1, got %d", alias3)
	}
}

func TestTopicAliasManager_OutboundLimit(t *testing.T) {
	tam := newTopicAliasManager(10, 3) // server accepts max 3

	// Assign 3 aliases (up to limit)
	for i := 1; i <= 3; i++ {
		topic := "topic" + string(rune('0'+i))
		alias, isNew, ok := tam.getOrAssignOutbound(topic)
		if !ok {
			t.Fatalf("Assignment %d failed", i)
		}
		if !isNew {
			t.Errorf("Assignment %d should be new", i)
		}
		if alias != uint16(i) {
			t.Errorf("Expected alias %d, got %d", i, alias)
		}
	}

	// Try to assign beyond limit
	_, _, ok := tam.getOrAssignOutbound("topic4")
	if ok {
		t.Error("Expected failure when exceeding limit")
	}

	// Reusing existing alias should still work
	alias, isNew, ok := tam.getOrAssignOutbound("topic1")
	if !ok {
		t.Fatal("Expected successful reuse")
	}
	if isNew {
		t.Error("Expected existing alias")
	}
	if alias != 1 {
		t.Errorf("Expected alias 1, got %d", alias)
	}
}

func TestTopicAliasManager_Disabled(t *testing.T) {
	tam := newTopicAliasManager(10, 0) // server doesn't support aliases

	_, _, ok := tam.getOrAssignOutbound("test/topic")
	if ok {
		t.Error("Expected failure when aliases disabled")
	}
}

func TestTopicAliasManager_InboundRegister(t *testing.T) {
	tam := newTopicAliasManager(5, 10)

	// Register inbound alias
	ok := tam.registerInbound(1, "device/status")
	if !ok {
		t.Fatal("Expected successful registration")
	}

	// Resolve the alias
	topic, ok := tam.resolveInbound(1)
	if !ok {
		t.Fatal("Expected successful resolution")
	}
	if topic != "device/status" {
		t.Errorf("Expected 'device/status', got '%s'", topic)
	}

	// Update existing alias
	ok = tam.registerInbound(1, "device/updated")
	if !ok {
		t.Fatal("Expected successful update")
	}

	topic, ok = tam.resolveInbound(1)
	if !ok {
		t.Fatal("Expected successful resolution")
	}
	if topic != "device/updated" {
		t.Errorf("Expected 'device/updated', got '%s'", topic)
	}
}

func TestTopicAliasManager_InboundLimit(t *testing.T) {
	tam := newTopicAliasManager(3, 10) // client accepts max 3

	// Valid alias within limit
	ok := tam.registerInbound(3, "topic3")
	if !ok {
		t.Error("Expected success for alias within limit")
	}

	// Invalid: beyond limit
	ok = tam.registerInbound(4, "topic4")
	if ok {
		t.Error("Expected failure for alias beyond limit")
	}

	// Invalid: zero alias
	ok = tam.registerInbound(0, "topic0")
	if ok {
		t.Error("Expected failure for zero alias")
	}
}

func TestTopicAliasManager_InboundDisabled(t *testing.T) {
	tam := newTopicAliasManager(0, 10) // client doesn't accept aliases

	ok := tam.registerInbound(1, "test/topic")
	if ok {
		t.Error("Expected failure when inbound aliases disabled")
	}
}

func TestTopicAliasManager_InboundUnknownAlias(t *testing.T) {
	tam := newTopicAliasManager(5, 10)

	// Try to resolve unregistered alias
	_, ok := tam.resolveInbound(99)
	if ok {
		t.Error("Expected failure for unknown alias")
	}
}

func TestTopicAliasManager_Reset(t *testing.T) {
	tam := newTopicAliasManager(10, 10)

	// Assign outbound aliases
	tam.getOrAssignOutbound("topic1")
	tam.getOrAssignOutbound("topic2")

	// Register inbound alias
	tam.registerInbound(1, "device/status")

	// Verify they exist
	_, _, ok1 := tam.getOrAssignOutbound("topic1")
	if !ok1 {
		t.Error("Outbound alias should exist")
	}
	_, ok2 := tam.resolveInbound(1)
	if !ok2 {
		t.Error("Inbound alias should exist")
	}

	// Reset
	tam.reset()

	// Verify all cleared
	alias, isNew, ok := tam.getOrAssignOutbound("topic1")
	if !ok {
		t.Fatal("Should be able to assign after reset")
	}
	if !isNew {
		t.Error("Should be treated as new after reset")
	}
	if alias != 1 {
		t.Error("Should start from alias 1 after reset")
	}

	_, ok = tam.resolveInbound(1)
	if ok {
		t.Error("Inbound aliases should be cleared after reset")
	}
}

func TestTopicAliasManager_UpdateServerMaximum(t *testing.T) {
	tam := newTopicAliasManager(10, 10)

	// Assign 5 aliases
	tam.getOrAssignOutbound("topic1")
	tam.getOrAssignOutbound("topic2")
	tam.getOrAssignOutbound("topic3")
	tam.getOrAssignOutbound("topic4")
	tam.getOrAssignOutbound("topic5")

	// Lower the server limit to 3
	tam.updateServerMaximum(3)

	// Aliases 1-3 should still exist
	alias1, isNew1, ok1 := tam.getOrAssignOutbound("topic1")
	if !ok1 || isNew1 || alias1 != 1 {
		t.Error("Alias 1 should still exist")
	}

	alias2, isNew2, ok2 := tam.getOrAssignOutbound("topic2")
	if !ok2 || isNew2 || alias2 != 2 {
		t.Error("Alias 2 should still exist")
	}

	alias3, isNew3, ok3 := tam.getOrAssignOutbound("topic3")
	if !ok3 || isNew3 || alias3 != 3 {
		t.Error("Alias 3 should still exist")
	}

	// Aliases 4-5 should be cleared and cannot be reassigned (beyond limit)
	_, _, ok4 := tam.getOrAssignOutbound("topic4")
	if ok4 {
		t.Error("Topic4 should not be assignable after limit reduced to 3")
	}

	// topic5 also cannot be assigned
	_, _, ok5 := tam.getOrAssignOutbound("topic5")
	if ok5 {
		t.Error("Topic5 should not be assignable after limit reduced to 3")
	}

	// Can't assign new topics beyond limit
	_, _, ok := tam.getOrAssignOutbound("topic6")
	if ok {
		t.Error("Should not be able to assign beyond new limit")
	}
}

func TestTopicAliasManager_Stats(t *testing.T) {
	tam := newTopicAliasManager(10, 10)

	// Initially empty
	outCount, inCount := tam.stats()
	if outCount != 0 || inCount != 0 {
		t.Error("Stats should be zero initially")
	}

	// Add some aliases
	tam.getOrAssignOutbound("topic1")
	tam.getOrAssignOutbound("topic2")
	tam.registerInbound(1, "device1")
	tam.registerInbound(2, "device2")
	tam.registerInbound(3, "device3")

	outCount, inCount = tam.stats()
	if outCount != 2 {
		t.Errorf("Expected 2 outbound aliases, got %d", outCount)
	}
	if inCount != 3 {
		t.Errorf("Expected 3 inbound aliases, got %d", inCount)
	}

	// After reset
	tam.reset()
	outCount, inCount = tam.stats()
	if outCount != 0 || inCount != 0 {
		t.Error("Stats should be zero after reset")
	}
}

func TestTopicAliasManager_Concurrency(t *testing.T) {
	tam := newTopicAliasManager(100, 100)

	// Concurrent outbound assignments
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				topic := "topic" + string(rune('0'+id))
				tam.getOrAssignOutbound(topic)
			}
			done <- true
		}(i)
	}

	// Concurrent inbound registrations
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				tam.registerInbound(uint16(id+1), "device"+string(rune('0'+id)))
				tam.resolveInbound(uint16(id + 1))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Just verify no panics occurred
	outCount, inCount := tam.stats()
	if outCount == 0 && inCount == 0 {
		t.Error("Expected some aliases to be created")
	}
}
