// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	msgclient "github.com/absmach/fluxmq/client"
	"github.com/absmach/fluxmq/topics"
)

const (
	defaultFuzzyConfigPath = "tests/perf/configs/fuzzy_mixed_bridge.json"
)

type fuzzyTopicProfileConfig struct {
	Name            string `json:"name"`
	Topic           string `json:"topic"`
	MaxPublishers   int    `json:"max_publishers"`
	MaxSubscribers  int    `json:"max_subscribers"`
	PublishInterval string `json:"publish_interval,omitempty"`
	PublishJitter   string `json:"publish_jitter,omitempty"`
	Weight          int    `json:"weight,omitempty"`

	resolvedPublishInterval time.Duration
	resolvedPublishJitter   time.Duration
}

type fuzzyScenarioConfig struct {
	Name                  string                    `json:"name"`
	Description           string                    `json:"description,omitempty"`
	Mode                  string                    `json:"mode,omitempty"`
	Duration              string                    `json:"duration"`
	DrainTimeout          string                    `json:"drain_timeout,omitempty"`
	ChurnInterval         string                    `json:"churn_interval,omitempty"`
	MaxPublishers         int                       `json:"max_publishers"`
	MaxSubscribers        int                       `json:"max_subscribers"`
	MaxTotalConnections   int                       `json:"max_total_connections,omitempty"`
	QoS                   *int                      `json:"qos,omitempty"`
	MQTTPublisherPercent  int                       `json:"mqtt_publisher_percent,omitempty"`
	MQTTSubscriberPercent int                       `json:"mqtt_subscriber_percent,omitempty"`
	TopicProfiles         []fuzzyTopicProfileConfig `json:"topic_profiles"`

	resolvedDuration              time.Duration
	resolvedDrainTimeout          time.Duration
	resolvedChurnInterval         time.Duration
	resolvedMaxTotalConnections   int
	resolvedQoS                   byte
	resolvedMQTTPublisherPercent  int
	resolvedMQTTSubscriberPercent int
}

type fuzzyPublisher struct {
	id         int
	proto      string
	profileIdx int
	topic      string
	interval   time.Duration
	jitter     time.Duration
	client     *msgclient.Client
	cancel     context.CancelFunc
	done       chan struct{}
}

type fuzzySubscriber struct {
	id         int
	proto      string
	profileIdx int
	filter     string
	client     *msgclient.Client
}

func loadFuzzyScenarioConfig(path string) (fuzzyScenarioConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fuzzyScenarioConfig{}, fmt.Errorf("failed to read fuzzy config %s: %w", path, err)
	}

	var cfg fuzzyScenarioConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fuzzyScenarioConfig{}, fmt.Errorf("failed to parse fuzzy config %s: %w", path, err)
	}

	return normalizeFuzzyScenarioConfig(cfg, path)
}

func normalizeFuzzyScenarioConfig(cfg fuzzyScenarioConfig, path string) (fuzzyScenarioConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		base := filepath.Base(path)
		cfg.Name = strings.TrimSuffix(base, filepath.Ext(base))
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = "fuzzy"
	}
	if mode != "fuzzy" {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid mode %q (use fuzzy)", cfg.Name, cfg.Mode)
	}
	cfg.Mode = mode

	durRaw := strings.TrimSpace(cfg.Duration)
	if durRaw == "" {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must set duration", cfg.Name)
	}
	dur, err := time.ParseDuration(durRaw)
	if err != nil {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid duration %q: %w", cfg.Name, cfg.Duration, err)
	}
	if dur <= 0 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must set duration > 0", cfg.Name)
	}
	cfg.resolvedDuration = dur

	if cfg.MaxPublishers <= 0 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must set max_publishers > 0", cfg.Name)
	}
	if cfg.MaxSubscribers <= 0 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must set max_subscribers > 0", cfg.Name)
	}

	churnInterval := 750 * time.Millisecond
	if strings.TrimSpace(cfg.ChurnInterval) != "" {
		d, err := time.ParseDuration(cfg.ChurnInterval)
		if err != nil {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid churn_interval %q: %w", cfg.Name, cfg.ChurnInterval, err)
		}
		if d <= 0 {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must set churn_interval > 0", cfg.Name)
		}
		churnInterval = d
	}
	cfg.resolvedChurnInterval = churnInterval

	maxPossibleConnections := cfg.MaxPublishers + cfg.MaxSubscribers
	if cfg.MaxTotalConnections <= 0 {
		cfg.MaxTotalConnections = maxPossibleConnections
	}
	if cfg.MaxTotalConnections > maxPossibleConnections {
		cfg.MaxTotalConnections = maxPossibleConnections
	}
	if cfg.MaxTotalConnections <= 0 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must resolve max_total_connections > 0", cfg.Name)
	}
	cfg.resolvedMaxTotalConnections = cfg.MaxTotalConnections

	drainTimeout := 30 * time.Second
	if strings.TrimSpace(cfg.DrainTimeout) != "" {
		d, err := time.ParseDuration(cfg.DrainTimeout)
		if err != nil {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid drain_timeout %q: %w", cfg.Name, cfg.DrainTimeout, err)
		}
		if d <= 0 {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must set drain_timeout > 0", cfg.Name)
		}
		drainTimeout = d
	}
	cfg.resolvedDrainTimeout = drainTimeout

	qos := 1
	if cfg.QoS != nil {
		if *cfg.QoS < 0 || *cfg.QoS > 2 {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid qos=%d (use 0|1|2)", cfg.Name, *cfg.QoS)
		}
		qos = *cfg.QoS
	}
	cfg.resolvedQoS = byte(qos)

	if len(cfg.TopicProfiles) == 0 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q must define at least one topic profile", cfg.Name)
	}
	for i := range cfg.TopicProfiles {
		p := &cfg.TopicProfiles[i]
		p.Name = strings.TrimSpace(p.Name)
		if p.Name == "" {
			p.Name = fmt.Sprintf("profile-%d", i+1)
		}

		p.Topic = strings.TrimSpace(p.Topic)
		p.Topic = strings.TrimSuffix(p.Topic, "/")
		if p.Topic == "" {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q must set topic", cfg.Name, p.Name)
		}
		if p.MaxPublishers <= 0 {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q must set max_publishers > 0", cfg.Name, p.Name)
		}
		if p.MaxSubscribers <= 0 {
			return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q must set max_subscribers > 0", cfg.Name, p.Name)
		}
		if p.MaxPublishers > cfg.MaxPublishers {
			p.MaxPublishers = cfg.MaxPublishers
		}
		if p.MaxSubscribers > cfg.MaxSubscribers {
			p.MaxSubscribers = cfg.MaxSubscribers
		}
		if p.Weight <= 0 {
			p.Weight = 1
		}

		if strings.TrimSpace(p.PublishInterval) == "" {
			p.resolvedPublishInterval = 100 * time.Millisecond
		} else {
			d, err := time.ParseDuration(p.PublishInterval)
			if err != nil {
				return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q has invalid publish_interval %q: %w", cfg.Name, p.Name, p.PublishInterval, err)
			}
			if d < 0 {
				return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q has invalid publish_interval %q", cfg.Name, p.Name, p.PublishInterval)
			}
			p.resolvedPublishInterval = d
		}

		if strings.TrimSpace(p.PublishJitter) != "" {
			d, err := time.ParseDuration(p.PublishJitter)
			if err != nil {
				return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q has invalid publish_jitter %q: %w", cfg.Name, p.Name, p.PublishJitter, err)
			}
			if d < 0 {
				return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q profile %q has invalid publish_jitter %q", cfg.Name, p.Name, p.PublishJitter)
			}
			p.resolvedPublishJitter = d
		}
	}

	if cfg.MQTTPublisherPercent < 0 || cfg.MQTTPublisherPercent > 100 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid mqtt_publisher_percent=%d (use 0..100)", cfg.Name, cfg.MQTTPublisherPercent)
	}
	if cfg.MQTTSubscriberPercent < 0 || cfg.MQTTSubscriberPercent > 100 {
		return fuzzyScenarioConfig{}, fmt.Errorf("fuzzy config %q has invalid mqtt_subscriber_percent=%d (use 0..100)", cfg.Name, cfg.MQTTSubscriberPercent)
	}
	if cfg.MQTTPublisherPercent == 0 && cfg.MQTTSubscriberPercent == 0 {
		cfg.MQTTPublisherPercent = 50
		cfg.MQTTSubscriberPercent = 50
	}
	if cfg.MQTTPublisherPercent == 0 {
		cfg.MQTTPublisherPercent = 50
	}
	if cfg.MQTTSubscriberPercent == 0 {
		cfg.MQTTSubscriberPercent = 50
	}
	cfg.resolvedMQTTPublisherPercent = cfg.MQTTPublisherPercent
	cfg.resolvedMQTTSubscriberPercent = cfg.MQTTSubscriberPercent

	return cfg, nil
}

func runFuzzyScenario(ctx context.Context, cfg runConfig, sc fuzzyScenarioConfig) (scenarioResult, error) {
	if cfg.Publishers > 0 && cfg.Publishers < sc.MaxPublishers {
		sc.MaxPublishers = cfg.Publishers
	}
	if cfg.Subscribers > 0 && cfg.Subscribers < sc.MaxSubscribers {
		sc.MaxSubscribers = cfg.Subscribers
	}
	if cfg.MaxTotalConnections > 0 && cfg.MaxTotalConnections < sc.resolvedMaxTotalConnections {
		sc.resolvedMaxTotalConnections = cfg.MaxTotalConnections
	}
	if cfg.Duration > 0 {
		sc.resolvedDuration = cfg.Duration
	}
	if cfg.ChurnInterval > 0 {
		sc.resolvedChurnInterval = cfg.ChurnInterval
	}
	if cfg.DrainTimeout > 0 {
		sc.resolvedDrainTimeout = cfg.DrainTimeout
	}

	maxPossibleConnections := sc.MaxPublishers + sc.MaxSubscribers
	if sc.resolvedMaxTotalConnections > maxPossibleConnections {
		sc.resolvedMaxTotalConnections = maxPossibleConnections
	}
	if sc.resolvedMaxTotalConnections <= 0 {
		return scenarioResult{}, fmt.Errorf("fuzzy scenario %q resolved max_total_connections <= 0", sc.Name)
	}

	res := scenarioResult{
		Scenario:     sc.Name,
		Description:  sc.Description,
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	runID := time.Now().UnixNano()
	mqttPublisherEndpoints := shuffledMQTTEndpoints(cfg.MQTTV3Addrs, cfg.MQTTV5Addrs, runID, mqttPublisherSeedSalt)
	mqttSubscriberEndpoints := shuffledMQTTEndpoints(cfg.MQTTV3Addrs, cfg.MQTTV5Addrs, runID, mqttSubscriberSeedSalt)
	amqpPublisherAddrs := shuffledAddrs(cfg.AMQPAddrs, runID, amqpPublisherSeedSalt)
	amqpSubscriberAddrs := shuffledAddrs(cfg.AMQPAddrs, runID, amqpSubscriberSeedSalt)

	for i := range sc.TopicProfiles {
		sc.TopicProfiles[i].Topic = strings.ReplaceAll(sc.TopicProfiles[i].Topic, "{run_id}", fmt.Sprintf("%d", runID))
	}

	var sent atomic.Int64
	var received atomic.Int64
	var expected atomic.Int64
	var errCount atomic.Int64
	var publishErrCount atomic.Int64
	seen := newDeduper()

	stopReport := startPeriodicStatusReport(sc.Name, func() int64 { return sent.Load() }, func() int64 { return received.Load() })
	defer stopReport()

	runCtx, cancelRun := context.WithTimeout(ctx, sc.resolvedDuration)
	defer cancelRun()

	var stateMu sync.RWMutex
	publishers := map[int]*fuzzyPublisher{}
	subscribers := map[int]*fuzzySubscriber{}
	activePublisherByProfile := make([]int, len(sc.TopicProfiles))
	activeSubscriberByProfile := make([]int, len(sc.TopicProfiles))
	nextPublisherID := 1
	nextSubscriberID := 1
	peakPublishers := 0
	peakSubscribers := 0

	var mqttPubCounter int
	var mqttSubCounter int
	var amqpPubCounter int
	var amqpSubCounter int

	rng := rand.New(rand.NewSource(runID + 991))

	chooseProto := func(mqttPercent int) string {
		if rng.Intn(100) < mqttPercent {
			return protoMQTT
		}
		return protoAMQP
	}

	pickProfile := func(active []int, isPublisher bool) int {
		totalWeight := 0
		weights := make([]int, len(sc.TopicProfiles))
		for i, p := range sc.TopicProfiles {
			available := p.MaxSubscribers - active[i]
			if isPublisher {
				available = p.MaxPublishers - active[i]
			}
			if available <= 0 {
				continue
			}
			w := p.Weight * available
			if w <= 0 {
				w = 1
			}
			weights[i] = w
			totalWeight += w
		}
		if totalWeight == 0 {
			return -1
		}
		pick := rng.Intn(totalWeight)
		for i, w := range weights {
			if w == 0 {
				continue
			}
			if pick < w {
				return i
			}
			pick -= w
		}
		return -1
	}

	publisherWait := sync.WaitGroup{}

	countExpectedForTopic := func(topic string) int64 {
		stateMu.RLock()
		defer stateMu.RUnlock()

		var matches int64
		for _, sub := range subscribers {
			if topics.TopicMatch(sub.filter, topic) {
				matches++
			}
		}
		return matches
	}

	connectPublisher := func(protoOverride string) bool {
		stateMu.RLock()
		curPublishers := len(publishers)
		curSubscribers := len(subscribers)
		activeSnapshot := append([]int(nil), activePublisherByProfile...)
		stateMu.RUnlock()
		if curPublishers >= sc.MaxPublishers {
			return false
		}
		if curPublishers+curSubscribers >= sc.resolvedMaxTotalConnections {
			return false
		}
		profileIdx := pickProfile(activeSnapshot, true)
		if profileIdx < 0 {
			return false
		}

		profile := sc.TopicProfiles[profileIdx]
		proto := protoOverride
		if proto == "" {
			proto = chooseProto(sc.resolvedMQTTPublisherPercent)
		}

		id := nextPublisherID
		nextPublisherID++

		var client *msgclient.Client
		var err error
		switch proto {
		case protoMQTT:
			endpoint := mqttEndpointByOffset(mqttPublisherEndpoints, mqttPubCounter, publisherNodeOffset)
			mqttPubCounter++
			client, err = connectMQTTClient(runCtx, endpoint.Addr, endpoint.ProtocolVersion, fmt.Sprintf("%s-fz-pub-%d-%d", sc.Name, runID, id))
		case protoAMQP:
			addr := addrByOffset(amqpPublisherAddrs, amqpPubCounter, publisherNodeOffset)
			amqpPubCounter++
			client, err = connectAMQPClient(runCtx, addr)
		default:
			return false
		}
		if err != nil {
			errCount.Add(1)
			return false
		}

		pubCtx, cancel := context.WithCancel(runCtx)
		pub := &fuzzyPublisher{
			id:         id,
			proto:      proto,
			profileIdx: profileIdx,
			topic:      profile.Topic,
			interval:   profile.resolvedPublishInterval,
			jitter:     profile.resolvedPublishJitter,
			client:     client,
			cancel:     cancel,
			done:       make(chan struct{}),
		}

		stateMu.Lock()
		publishers[id] = pub
		activePublisherByProfile[profileIdx]++
		if len(publishers) > peakPublishers {
			peakPublishers = len(publishers)
		}
		stateMu.Unlock()

		publisherWait.Add(1)
		go func(p *fuzzyPublisher) {
			defer publisherWait.Done()
			defer close(p.done)

			localRNG := newPublishRNG(runID, p.id+9000)
			if !sleepWithContext(pubCtx, initialPublishDelay(p.jitter, localRNG)) {
				return
			}

			seq := 0
			for {
				select {
				case <-pubCtx.Done():
					return
				default:
				}

				msgID := fmt.Sprintf("fuzzy-%d-%d-%d", runID, p.id, seq)
				seq++
				payload := makePayload(msgID, cfg.PayloadBytes)

				matches := countExpectedForTopic(p.topic)
				var publishErr error
				switch p.proto {
				case protoMQTT:
					publishErr = p.client.Publish(pubCtx, p.topic, payload, msgclient.WithQoS(sc.resolvedQoS))
				case protoAMQP:
					publishErr = p.client.Publish(pubCtx, p.topic, payload)
				default:
					publishErr = fmt.Errorf("unsupported publisher protocol %q", p.proto)
				}

				if publishErr != nil {
					publishErrCount.Add(1)
					errCount.Add(1)
				} else {
					sent.Add(1)
					expected.Add(matches)
				}

				if !sleepWithContext(pubCtx, jitteredPublishInterval(p.interval, p.jitter, localRNG)) {
					return
				}
			}
		}(pub)

		return true
	}

	makeSubscriberFilter := func(profile fuzzyTopicProfileConfig) string {
		if rng.Intn(4) != 0 {
			return profile.Topic
		}
		if idx := strings.LastIndex(profile.Topic, "/"); idx > 0 {
			return profile.Topic[:idx] + "/#"
		}
		return profile.Topic
	}

	connectSubscriber := func(protoOverride string) bool {
		stateMu.RLock()
		curSubscribers := len(subscribers)
		curPublishers := len(publishers)
		activeSnapshot := append([]int(nil), activeSubscriberByProfile...)
		stateMu.RUnlock()
		if curSubscribers >= sc.MaxSubscribers {
			return false
		}
		if curPublishers+curSubscribers >= sc.resolvedMaxTotalConnections {
			return false
		}
		profileIdx := pickProfile(activeSnapshot, false)
		if profileIdx < 0 {
			return false
		}

		profile := sc.TopicProfiles[profileIdx]
		proto := protoOverride
		if proto == "" {
			proto = chooseProto(sc.resolvedMQTTSubscriberPercent)
		}
		filter := makeSubscriberFilter(profile)

		id := nextSubscriberID
		nextSubscriberID++

		var client *msgclient.Client
		var err error
		switch proto {
		case protoMQTT:
			endpoint := mqttEndpointByOffset(mqttSubscriberEndpoints, mqttSubCounter, subscriberNodeOffset)
			mqttSubCounter++
			client, err = connectMQTTClient(runCtx, endpoint.Addr, endpoint.ProtocolVersion, fmt.Sprintf("%s-fz-sub-%d-%d", sc.Name, runID, id))
			if err == nil {
				err = client.Subscribe(runCtx, filter, func(msg *msgclient.Message) {
					if !isCountablePayload(msg.Payload) {
						return
					}
					msgID := extractMsgID(msg.Payload)
					if seen.add(fmt.Sprintf("%d:%s", id, msgID)) {
						received.Add(1)
					}
				}, msgclient.WithQoS(sc.resolvedQoS))
			}
		case protoAMQP:
			addr := addrByOffset(amqpSubscriberAddrs, amqpSubCounter, subscriberNodeOffset)
			amqpSubCounter++
			client, err = connectAMQPClient(runCtx, addr)
			if err == nil {
				err = client.Subscribe(runCtx, filter, func(msg *msgclient.Message) {
					if isCountablePayload(msg.Payload) {
						msgID := extractMsgID(msg.Payload)
						if seen.add(fmt.Sprintf("%d:%s", id, msgID)) {
							received.Add(1)
						}
					}
					if ackErr := msg.Ack(); ackErr != nil {
						errCount.Add(1)
					}
				}, msgclient.WithAutoAck(false))
			}
		default:
			return false
		}
		if err != nil {
			if client != nil {
				_ = client.Close(runCtx)
			}
			errCount.Add(1)
			return false
		}

		stateMu.Lock()
		subscribers[id] = &fuzzySubscriber{
			id:         id,
			proto:      proto,
			profileIdx: profileIdx,
			filter:     filter,
			client:     client,
		}
		activeSubscriberByProfile[profileIdx]++
		if len(subscribers) > peakSubscribers {
			peakSubscribers = len(subscribers)
		}
		stateMu.Unlock()
		return true
	}

	disconnectPublisherByID := func(ctx context.Context, id int) {
		var pub *fuzzyPublisher
		stateMu.Lock()
		if p, ok := publishers[id]; ok {
			pub = p
			delete(publishers, id)
			if activePublisherByProfile[p.profileIdx] > 0 {
				activePublisherByProfile[p.profileIdx]--
			}
		}
		stateMu.Unlock()

		if pub == nil {
			return
		}
		pub.cancel()
		_ = pub.client.Close(ctx)
		select {
		case <-pub.done:
		case <-time.After(3 * time.Second):
		}
	}

	disconnectSubscriberByID := func(ctx context.Context, id int) {
		var sub *fuzzySubscriber
		stateMu.Lock()
		if s, ok := subscribers[id]; ok {
			sub = s
			delete(subscribers, id)
			if activeSubscriberByProfile[s.profileIdx] > 0 {
				activeSubscriberByProfile[s.profileIdx]--
			}
		}
		stateMu.Unlock()

		if sub == nil {
			return
		}
		_ = sub.client.Close(ctx)
	}

	pickRandomPublisherID := func() (int, bool) {
		stateMu.RLock()
		defer stateMu.RUnlock()
		if len(publishers) == 0 {
			return 0, false
		}
		keys := make([]int, 0, len(publishers))
		for id := range publishers {
			keys = append(keys, id)
		}
		return keys[rng.Intn(len(keys))], true
	}

	pickRandomSubscriberID := func() (int, bool) {
		stateMu.RLock()
		defer stateMu.RUnlock()
		if len(subscribers) == 0 {
			return 0, false
		}
		keys := make([]int, 0, len(subscribers))
		for id := range subscribers {
			keys = append(keys, id)
		}
		return keys[rng.Intn(len(keys))], true
	}

	currentConnections := func() (int, int) {
		stateMu.RLock()
		defer stateMu.RUnlock()
		return len(publishers), len(subscribers)
	}

	allocateTargetMix := func(total int) (int, int) {
		if total < 1 {
			total = 1
		}
		if total > sc.resolvedMaxTotalConnections {
			total = sc.resolvedMaxTotalConnections
		}

		minPublishers := 0
		minSubscribers := 0
		if total >= 2 && sc.MaxPublishers > 0 && sc.MaxSubscribers > 0 {
			minPublishers = 1
			minSubscribers = 1
		}
		if minPublishers > sc.MaxPublishers {
			minPublishers = sc.MaxPublishers
		}
		if minSubscribers > sc.MaxSubscribers {
			minSubscribers = sc.MaxSubscribers
		}

		remaining := total - minPublishers - minSubscribers
		if remaining < 0 {
			remaining = 0
		}

		pubExtraCap := sc.MaxPublishers - minPublishers
		if pubExtraCap < 0 {
			pubExtraCap = 0
		}
		subExtraCap := sc.MaxSubscribers - minSubscribers
		if subExtraCap < 0 {
			subExtraCap = 0
		}

		pubExtra := 0
		if remaining > 0 && pubExtraCap > 0 {
			pubExtra = rng.Intn(minInt(remaining, pubExtraCap) + 1)
		}
		subExtra := remaining - pubExtra
		if subExtra > subExtraCap {
			overflow := subExtra - subExtraCap
			subExtra = subExtraCap
			pubExtra = minInt(pubExtra+overflow, pubExtraCap)
		}

		targetPublishers := minPublishers + pubExtra
		targetSubscribers := minSubscribers + subExtra

		for targetPublishers+targetSubscribers < total {
			if targetPublishers < sc.MaxPublishers {
				targetPublishers++
				continue
			}
			if targetSubscribers < sc.MaxSubscribers {
				targetSubscribers++
				continue
			}
			break
		}

		for targetPublishers+targetSubscribers > total {
			if targetPublishers > minPublishers {
				targetPublishers--
				continue
			}
			if targetSubscribers > minSubscribers {
				targetSubscribers--
				continue
			}
			break
		}

		return targetPublishers, targetSubscribers
	}

	enforceTargetConnections := func(targetPublishers, targetSubscribers, pubStep, subStep int) {
		curPublishers, curSubscribers := currentConnections()
		if curPublishers > targetPublishers {
			toDrop := minInt(pubStep, curPublishers-targetPublishers)
			for i := 0; i < toDrop; i++ {
				if id, ok := pickRandomPublisherID(); ok {
					disconnectPublisherByID(ctx, id)
				}
			}
		}
		if curSubscribers > targetSubscribers {
			toDrop := minInt(subStep, curSubscribers-targetSubscribers)
			for i := 0; i < toDrop; i++ {
				if id, ok := pickRandomSubscriberID(); ok {
					disconnectSubscriberByID(ctx, id)
				}
			}
		}

		curPublishers, curSubscribers = currentConnections()
		if curPublishers < targetPublishers {
			toAdd := minInt(pubStep, targetPublishers-curPublishers)
			for i := 0; i < toAdd; i++ {
				_ = connectPublisher("")
			}
		}
		if curSubscribers < targetSubscribers {
			toAdd := minInt(subStep, targetSubscribers-curSubscribers)
			for i := 0; i < toAdd; i++ {
				_ = connectSubscriber("")
			}
		}
	}

	// Seed both protocol families so bridge paths are exercised from the start.
	_ = connectPublisher(protoMQTT)
	_ = connectPublisher(protoAMQP)
	_ = connectSubscriber(protoMQTT)
	_ = connectSubscriber(protoAMQP)

	bootstrapTotal := maxInt(1, sc.resolvedMaxTotalConnections*4/5)
	bootstrapPublishers, bootstrapSubscribers := allocateTargetMix(bootstrapTotal)

	churnTicker := time.NewTicker(sc.resolvedChurnInterval)
	defer churnTicker.Stop()

	pubStep := maxInt(1, sc.MaxPublishers/8)
	subStep := maxInt(1, sc.MaxSubscribers/8)
	swapOpsPerTick := maxInt(1, minInt(4, sc.resolvedMaxTotalConnections/100+1))

	enforceTargetConnections(bootstrapPublishers, bootstrapSubscribers, pubStep, subStep)

	for {
		select {
		case <-runCtx.Done():
			goto drain
		case <-churnTicker.C:
			targetFloor := maxInt(1, int(float64(sc.resolvedMaxTotalConnections)*0.7))
			if targetFloor > sc.resolvedMaxTotalConnections {
				targetFloor = sc.resolvedMaxTotalConnections
			}
			targetTotal := targetFloor
			if sc.resolvedMaxTotalConnections > targetFloor {
				targetTotal += rng.Intn(sc.resolvedMaxTotalConnections - targetFloor + 1)
			}

			targetPublishers, targetSubscribers := allocateTargetMix(targetTotal)
			enforceTargetConnections(targetPublishers, targetSubscribers, pubStep, subStep)

			// Force additional swap churn on every tick so reconnect/disconnect happens frequently.
			for i := 0; i < swapOpsPerTick; i++ {
				if id, ok := pickRandomPublisherID(); ok {
					disconnectPublisherByID(ctx, id)
					_ = connectPublisher("")
				}
				if id, ok := pickRandomSubscriberID(); ok {
					disconnectSubscriberByID(ctx, id)
					_ = connectSubscriber("")
				}
			}
		}
	}

drain:
	stateMu.RLock()
	publisherIDs := make([]int, 0, len(publishers))
	for id := range publishers {
		publisherIDs = append(publisherIDs, id)
	}
	stateMu.RUnlock()
	for _, id := range publisherIDs {
		disconnectPublisherByID(ctx, id)
	}
	publisherWait.Wait()

	_ = waitForAtLeast(&received, expected.Load(), sc.resolvedDrainTimeout)

	stateMu.RLock()
	subscriberIDs := make([]int, 0, len(subscribers))
	for id := range subscribers {
		subscriberIDs = append(subscriberIDs, id)
	}
	stateMu.RUnlock()
	for _, id := range subscriberIDs {
		disconnectSubscriberByID(ctx, id)
	}

	res.Published = sent.Load()
	res.Expected = expected.Load()
	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.PublishErrors = publishErrCount.Load()
	res.Publishers = peakPublishers
	res.Subscribers = peakSubscribers
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.MinRatio
	res.Notes = fmt.Sprintf(
		"fuzzy mode duration=%s churn=%s max_total_connections=%d max_publishers=%d max_subscribers=%d peak_publishers=%d peak_subscribers=%d profiles=%d qos=%d publish_errors=%d",
		sc.resolvedDuration,
		sc.resolvedChurnInterval,
		sc.resolvedMaxTotalConnections,
		sc.MaxPublishers,
		sc.MaxSubscribers,
		peakPublishers,
		peakSubscribers,
		len(sc.TopicProfiles),
		sc.resolvedQoS,
		res.PublishErrors,
	)
	if !res.Pass {
		res.Notes = res.Notes + fmt.Sprintf("; delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.MinRatio)
	}

	return res, nil
}
