// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestNormalizeTopicScenarioConfigPublishJitter(t *testing.T) {
	cfg := topicScenarioConfig{
		Name:                 "jitter-parse",
		Pattern:              "fanin",
		Flow:                 "mqtt-mqtt",
		Publishers:           1,
		MessagesPerPublisher: 1,
		PublishInterval:      "100ms",
		PublishJitter:        "25ms",
		Subscribers:          1,
	}

	out, err := normalizeTopicScenarioConfig(cfg, "")
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if out.resolvedPublishJitter != 25*time.Millisecond {
		t.Fatalf("resolved publish jitter = %v, want %v", out.resolvedPublishJitter, 25*time.Millisecond)
	}
}

func TestNormalizeTopicScenarioConfigInvalidPublishJitter(t *testing.T) {
	cfg := topicScenarioConfig{
		Name:                 "jitter-invalid",
		Pattern:              "fanin",
		Flow:                 "mqtt-mqtt",
		Publishers:           1,
		MessagesPerPublisher: 1,
		PublishJitter:        "not-a-duration",
		Subscribers:          1,
	}

	_, err := normalizeTopicScenarioConfig(cfg, "")
	if err == nil {
		t.Fatal("expected error for invalid publish_jitter")
	}
	if !strings.Contains(err.Error(), "invalid publish_jitter") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolvedPublishJitter(t *testing.T) {
	flagValue := 10 * time.Millisecond
	cfgValue := 25 * time.Millisecond
	if got := resolvedPublishJitter(flagValue, cfgValue); got != cfgValue {
		t.Fatalf("resolved from config = %v, want %v", got, cfgValue)
	}
	if got := resolvedPublishJitter(flagValue, 0); got != flagValue {
		t.Fatalf("resolved from flag = %v, want %v", got, flagValue)
	}
}

func TestInitialPublishDelayRange(t *testing.T) {
	jitter := 2 * time.Second
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		got := initialPublishDelay(jitter, rng)
		if got < 0 || got > jitter {
			t.Fatalf("initial delay out of range: %v", got)
		}
	}
}

func TestJitteredPublishIntervalRange(t *testing.T) {
	base := 5 * time.Second
	jitter := 2 * time.Second
	min := base - jitter
	max := base + jitter
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		got := jitteredPublishInterval(base, jitter, rng)
		if got < min || got > max {
			t.Fatalf("jittered interval out of range: %v (want between %v and %v)", got, min, max)
		}
	}
}

func TestJitteredPublishIntervalFloorsToZero(t *testing.T) {
	base := 10 * time.Millisecond
	jitter := 200 * time.Millisecond
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 1000; i++ {
		got := jitteredPublishInterval(base, jitter, rng)
		if got < 0 {
			t.Fatalf("jittered interval should never be negative: %v", got)
		}
	}
}

func TestShuffledAddrsDeterministicAndPermutation(t *testing.T) {
	in := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}
	orig := strings.Join(in, ",")

	a := shuffledAddrs(in, 123, mqttPublisherSeedSalt)
	b := shuffledAddrs(in, 123, mqttPublisherSeedSalt)
	if strings.Join(a, ",") != strings.Join(b, ",") {
		t.Fatalf("shuffle should be deterministic for same runID and salt: %v vs %v", a, b)
	}
	if strings.Join(in, ",") != orig {
		t.Fatalf("input addrs mutated: got %v want %s", in, orig)
	}

	c := shuffledAddrs(in, 124, mqttPublisherSeedSalt)
	if strings.Join(a, ",") == strings.Join(c, ",") {
		t.Fatalf("expected different order for different runIDs: %v", a)
	}

	want := make(map[string]int, len(in))
	have := make(map[string]int, len(in))
	for _, addr := range in {
		want[addr]++
	}
	for _, addr := range a {
		have[addr]++
	}
	if len(a) != len(in) {
		t.Fatalf("shuffled length mismatch: got %d want %d", len(a), len(in))
	}
	for addr, count := range want {
		if have[addr] != count {
			t.Fatalf("shuffled addrs not a permutation, addr=%s got=%d want=%d", addr, have[addr], count)
		}
	}
}

func TestAddrByOffsetBalancedAfterShuffle(t *testing.T) {
	addrs := shuffledAddrs([]string{"node1", "node2", "node3"}, 999, amqpPublisherSeedSalt)
	counts := map[string]int{
		"node1": 0,
		"node2": 0,
		"node3": 0,
	}

	const clients = 100
	for i := 0; i < clients; i++ {
		counts[addrByOffset(addrs, i, publisherNodeOffset)]++
	}

	min, max := clients, 0
	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	if max-min > 1 {
		t.Fatalf("unbalanced assignment after shuffle: counts=%v", counts)
	}
}

func TestShuffledMQTTEndpointsDeterministicAndMixed(t *testing.T) {
	v3 := []string{"n1:1883", "n2:1885", "n3:1887"}
	v5 := []string{"n1:1884", "n2:1886", "n3:1888"}

	a := shuffledMQTTEndpoints(v3, v5, 1234, mqttPublisherSeedSalt)
	b := shuffledMQTTEndpoints(v3, v5, 1234, mqttPublisherSeedSalt)
	if len(a) != len(v3)+len(v5) {
		t.Fatalf("unexpected endpoint count: got %d want %d", len(a), len(v3)+len(v5))
	}
	if len(b) != len(a) {
		t.Fatalf("unexpected deterministic length mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("expected deterministic ordering for same seed, mismatch at %d: %v vs %v", i, a[i], b[i])
		}
	}

	var v3Count, v5Count int
	for _, ep := range a {
		switch ep.ProtocolVersion {
		case 4:
			v3Count++
		case 5:
			v5Count++
		default:
			t.Fatalf("unexpected protocol version: %d", ep.ProtocolVersion)
		}
	}
	if v3Count != len(v3) || v5Count != len(v5) {
		t.Fatalf("unexpected protocol split: v3=%d v5=%d", v3Count, v5Count)
	}
}
