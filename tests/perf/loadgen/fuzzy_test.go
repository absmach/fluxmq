// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeFuzzyScenarioConfigValid(t *testing.T) {
	cfg := fuzzyScenarioConfig{
		Name:                  "fuzzy-valid",
		Mode:                  "fuzzy",
		Duration:              "90s",
		DrainTimeout:          "20s",
		ChurnInterval:         "1500ms",
		MaxPublishers:         100,
		MaxSubscribers:        120,
		MaxTotalConnections:   140,
		MQTTPublisherPercent:  65,
		MQTTSubscriberPercent: 40,
		TopicProfiles: []fuzzyTopicProfileConfig{
			{
				Name:            "hot",
				Topic:           "perf/fuzzy/hot/{run_id}",
				MaxPublishers:   90,
				MaxSubscribers:  20,
				PublishInterval: "40ms",
				PublishJitter:   "20ms",
				Weight:          4,
			},
			{
				Name:            "cold",
				Topic:           "perf/fuzzy/cold/{run_id}",
				MaxPublishers:   10,
				MaxSubscribers:  100,
				PublishInterval: "1s",
				PublishJitter:   "150ms",
				Weight:          1,
			},
		},
	}

	out, err := normalizeFuzzyScenarioConfig(cfg, "tests/perf/configs/fuzzy_valid.json")
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if out.resolvedDuration != 90*time.Second {
		t.Fatalf("duration = %v, want 90s", out.resolvedDuration)
	}
	if out.resolvedDrainTimeout != 20*time.Second {
		t.Fatalf("drain timeout = %v, want 20s", out.resolvedDrainTimeout)
	}
	if out.resolvedChurnInterval != 1500*time.Millisecond {
		t.Fatalf("churn interval = %v, want 1500ms", out.resolvedChurnInterval)
	}
	if out.resolvedMQTTPublisherPercent != 65 {
		t.Fatalf("mqtt publisher percent = %d, want 65", out.resolvedMQTTPublisherPercent)
	}
	if out.resolvedMQTTSubscriberPercent != 40 {
		t.Fatalf("mqtt subscriber percent = %d, want 40", out.resolvedMQTTSubscriberPercent)
	}
	if out.resolvedMaxTotalConnections != 140 {
		t.Fatalf("max total connections = %d, want 140", out.resolvedMaxTotalConnections)
	}
	if len(out.TopicProfiles) != 2 {
		t.Fatalf("topic profiles = %d, want 2", len(out.TopicProfiles))
	}
	if out.TopicProfiles[0].resolvedPublishInterval != 40*time.Millisecond {
		t.Fatalf("hot interval = %v, want 40ms", out.TopicProfiles[0].resolvedPublishInterval)
	}
	if out.TopicProfiles[1].resolvedPublishJitter != 150*time.Millisecond {
		t.Fatalf("cold jitter = %v, want 150ms", out.TopicProfiles[1].resolvedPublishJitter)
	}
}

func TestNormalizeFuzzyScenarioConfigInvalidDuration(t *testing.T) {
	cfg := fuzzyScenarioConfig{
		Name:           "fuzzy-invalid-duration",
		Mode:           "fuzzy",
		Duration:       "bad",
		MaxPublishers:  10,
		MaxSubscribers: 10,
		TopicProfiles: []fuzzyTopicProfileConfig{
			{
				Topic:           "perf/fuzzy/one",
				MaxPublishers:   5,
				MaxSubscribers:  5,
				PublishInterval: "100ms",
			},
		},
	}

	_, err := normalizeFuzzyScenarioConfig(cfg, "")
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
	if !strings.Contains(err.Error(), "invalid duration") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeFuzzyScenarioConfigRejectsMissingProfiles(t *testing.T) {
	cfg := fuzzyScenarioConfig{
		Name:           "fuzzy-no-profiles",
		Mode:           "fuzzy",
		Duration:       "60s",
		MaxPublishers:  10,
		MaxSubscribers: 10,
	}

	_, err := normalizeFuzzyScenarioConfig(cfg, "")
	if err == nil {
		t.Fatal("expected error for missing topic profiles")
	}
	if !strings.Contains(err.Error(), "at least one topic profile") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeFuzzyScenarioConfigClampsMaxTotalConnections(t *testing.T) {
	cfg := fuzzyScenarioConfig{
		Name:                "fuzzy-max-total-clamp",
		Mode:                "fuzzy",
		Duration:            "60s",
		MaxPublishers:       40,
		MaxSubscribers:      50,
		MaxTotalConnections: 500,
		TopicProfiles: []fuzzyTopicProfileConfig{
			{
				Topic:           "perf/fuzzy/one",
				MaxPublishers:   40,
				MaxSubscribers:  50,
				PublishInterval: "100ms",
			},
		},
	}

	out, err := normalizeFuzzyScenarioConfig(cfg, "")
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	want := 90 // max_publishers + max_subscribers
	if out.resolvedMaxTotalConnections != want {
		t.Fatalf("max total connections = %d, want %d", out.resolvedMaxTotalConnections, want)
	}
}

func TestNormalizeFuzzyScenarioConfigDefaultsMaxTotalAndChurn(t *testing.T) {
	cfg := fuzzyScenarioConfig{
		Name:           "fuzzy-defaults",
		Mode:           "fuzzy",
		Duration:       "45s",
		MaxPublishers:  12,
		MaxSubscribers: 18,
		TopicProfiles: []fuzzyTopicProfileConfig{
			{
				Topic:           "perf/fuzzy/defaults",
				MaxPublishers:   12,
				MaxSubscribers:  18,
				PublishInterval: "80ms",
			},
		},
	}

	out, err := normalizeFuzzyScenarioConfig(cfg, "")
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if out.resolvedChurnInterval != 750*time.Millisecond {
		t.Fatalf("default churn interval = %v, want 750ms", out.resolvedChurnInterval)
	}
	if out.resolvedMaxTotalConnections != 30 {
		t.Fatalf("default max total connections = %d, want 30", out.resolvedMaxTotalConnections)
	}
}
