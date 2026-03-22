// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	msgclient "github.com/absmach/fluxmq/client"
	amqpclient "github.com/absmach/fluxmq/client/amqp"
	mqttclient "github.com/absmach/fluxmq/client/mqtt"
	"github.com/absmach/fluxmq/topics"
)

const (
	patternFanIn  = "fanin"
	patternFanOut = "fanout"
	protoMQTT     = "mqtt"
	protoAMQP     = "amqp"
)

type runConfig struct {
	Scenario             string
	PayloadLabel         string
	PayloadBytes         int
	PayloadFromCLI       bool
	MQTTV3Addrs          []string
	MQTTV5Addrs          []string
	AMQPAddrs            []string
	MinRatio             float64
	DrainTimeout         time.Duration
	Publishers           int
	Subscribers          int
	MessagesPerPublisher int
	PublishInterval      time.Duration
	PublishJitter        time.Duration
}

type topicScenarioConfig struct {
	Name                 string   `json:"name"`
	Description          string   `json:"description,omitempty"`
	Pattern              string   `json:"pattern"`
	Flow                 string   `json:"flow,omitempty"`
	PublisherProtocol    string   `json:"publisher_protocol,omitempty"`
	SubscriberProtocol   string   `json:"subscriber_protocol,omitempty"`
	Publishers           int      `json:"publishers"`
	MessagesPerPublisher int      `json:"messages_per_publisher"`
	PublishInterval      string   `json:"publish_interval,omitempty"`
	PublishJitter        string   `json:"publish_jitter,omitempty"`
	Topic                string   `json:"topic,omitempty"`
	TopicCount           int      `json:"topic_count,omitempty"`
	Subscribers          int      `json:"subscribers"`
	QoS                  *int     `json:"qos,omitempty"`
	WildcardSubscribers  int      `json:"wildcard_subscribers,omitempty"`
	WildcardPatterns     []string `json:"wildcard_patterns,omitempty"`
	DrainTimeout         string   `json:"drain_timeout,omitempty"`
	PayloadBytes         int      `json:"payload_bytes,omitempty"`

	resolvedPublishInterval time.Duration
	resolvedPublishJitter   time.Duration
	resolvedDrainTimeout    time.Duration
	resolvedPublisherProto  string
	resolvedSubscriberProto string
	resolvedQoS             byte
}

type scenarioResult struct {
	Timestamp      string  `json:"timestamp"`
	Scenario       string  `json:"scenario"`
	Description    string  `json:"description"`
	PayloadLabel   string  `json:"payload_label"`
	PayloadBytes   int     `json:"payload_bytes"`
	Publishers     int     `json:"publishers"`
	Subscribers    int     `json:"subscribers"`
	Published      int64   `json:"published"`
	Expected       int64   `json:"expected"`
	Received       int64   `json:"received"`
	DeliveryRatio  float64 `json:"delivery_ratio"`
	Errors         int64   `json:"errors"`
	PublishErrors  int64   `json:"publish_errors,omitempty"`
	PublishRateMPS float64 `json:"publish_rate_mps"`
	ReceiveRateMPS float64 `json:"receive_rate_mps"`
	DurationMS     int64   `json:"duration_ms"`
	Pass           bool    `json:"pass"`
	Notes          string  `json:"notes,omitempty"`
}

type deduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

type mqttEndpoint struct {
	Addr            string
	ProtocolVersion byte
}

const (
	publisherNodeOffset          = 0
	subscriberNodeOffset         = 1
	mqttPublisherSeedSalt  int64 = 1009
	mqttSubscriberSeedSalt int64 = 2027
	amqpPublisherSeedSalt  int64 = 3037
	amqpSubscriberSeedSalt int64 = 4051
)

func newDeduper() *deduper {
	return &deduper{seen: make(map[string]struct{})}
}

func (d *deduper) add(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return false
	}
	d.seen[key] = struct{}{}
	return true
}

func startPeriodicStatusReport(scenario string, sentFn, receivedFn func() int64) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	start := time.Now()

	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var prevSent int64
		var prevReceived int64

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				sent := sentFn()
				received := receivedFn()
				sentDelta := sent - prevSent
				receivedDelta := received - prevReceived
				if sentDelta < 0 {
					sentDelta = 0
				}
				if receivedDelta < 0 {
					receivedDelta = 0
				}

				fmt.Printf("progress scenario=%s elapsed=%s sent=%d received=%d mps_sent_10s=%.2f mps_recv_10s=%.2f\n",
					scenario,
					time.Since(start).Truncate(time.Millisecond),
					sent,
					received,
					float64(sentDelta)/10.0,
					float64(receivedDelta)/10.0,
				)

				prevSent = sent
				prevReceived = received
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

func addrByOffset(addrs []string, idx, offset int) string {
	return addrs[(idx+offset)%len(addrs)]
}

func shuffledAddrs(addrs []string, runID, salt int64) []string {
	out := append([]string(nil), addrs...)
	if len(out) < 2 {
		return out
	}
	rng := rand.New(rand.NewSource(runID + salt*7919))
	rng.Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})
	return out
}

func shuffledMQTTEndpoints(v3Addrs, v5Addrs []string, runID, salt int64) []mqttEndpoint {
	out := make([]mqttEndpoint, 0, len(v3Addrs)+len(v5Addrs))
	for _, addr := range v3Addrs {
		out = append(out, mqttEndpoint{Addr: addr, ProtocolVersion: 4})
	}
	for _, addr := range v5Addrs {
		out = append(out, mqttEndpoint{Addr: addr, ProtocolVersion: 5})
	}
	if len(out) < 2 {
		return out
	}
	rng := rand.New(rand.NewSource(runID + salt*7919))
	rng.Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})
	return out
}

func mqttEndpointByOffset(endpoints []mqttEndpoint, idx, offset int) mqttEndpoint {
	return endpoints[(idx+offset)%len(endpoints)]
}

func main() {
	scenarioConfigFlag := flag.String("scenario-config", "", "Path to JSON config for fanin/fanout scenario")
	payloadFlag := flag.String("payload", "small", "Payload preset: small|medium|large")
	payloadBytesFlag := flag.Int("payload-bytes", 0, "Payload size in bytes (overrides -payload)")
	mqttV3Flag := flag.String("mqtt-v3-addrs", "127.0.0.1:1883,127.0.0.1:1885,127.0.0.1:1887", "Comma-separated MQTT v3 broker addresses")
	mqttV5Flag := flag.String("mqtt-v5-addrs", "127.0.0.1:1884,127.0.0.1:1886,127.0.0.1:1888", "Comma-separated MQTT v5 broker addresses")
	amqpFlag := flag.String("amqp-addrs", "127.0.0.1:5682,127.0.0.1:5683,127.0.0.1:5684", "Comma-separated AMQP 0.9.1 broker addresses")
	publishersFlag := flag.Int("publishers", 0, "Override concurrent publishers")
	subscribersFlag := flag.Int("subscribers", 0, "Override concurrent subscribers")
	messagesPerPublisherFlag := flag.Int("messages-per-publisher", 0, "Override messages each publisher sends")
	publishIntervalFlag := flag.Duration("publish-interval", 0, "Pause between each publish per publisher (e.g. 5ms)")
	publishJitterFlag := flag.Duration("publish-jitter", 0, "Per-publisher random jitter for publish cadence")
	minRatioFlag := flag.Float64("min-ratio", 0.95, "Minimum delivery ratio")
	drainTimeoutFlag := flag.Duration("drain-timeout", 45*time.Second, "Max wait time for message drain after publishers finish")
	jsonOutFlag := flag.String("json-out", "", "Optional file to append one JSON line result")
	listFlag := flag.Bool("list-scenarios", false, "Print built-in scenario config paths")
	flag.Parse()

	scenarios := []string{
		"tests/perf/configs/fanin_mqtt_mqtt.json",
		"tests/perf/configs/fanin_mqtt_amqp.json",
		"tests/perf/configs/fanin_amqp_mqtt.json",
		"tests/perf/configs/fanin_amqp_amqp.json",
		"tests/perf/configs/fanout_mqtt_mqtt.json",
		"tests/perf/configs/fanout_mqtt_amqp.json",
		"tests/perf/configs/fanout_amqp_mqtt.json",
		"tests/perf/configs/fanout_amqp_amqp.json",
	}

	if *listFlag {
		fmt.Println("Built-in fanin/fanout scenario configs (use -scenario-config <path>):")
		for _, sc := range scenarios {
			fmt.Println("  " + sc)
		}
		return
	}

	if *scenarioConfigFlag == "" {
		exitErr(fmt.Errorf("-scenario-config is required (use -list-scenarios to see options)"))
	}

	payloadLabel := strings.ToLower(*payloadFlag)
	payloadBytes := 0
	payloadFromCLI := *payloadBytesFlag > 0
	if payloadFromCLI {
		payloadBytes = *payloadBytesFlag
		payloadLabel = fmt.Sprintf("%dB", payloadBytes)
	} else {
		var err error
		payloadBytes, err = payloadSizeFromLabel(*payloadFlag)
		if err != nil {
			exitErr(err)
		}
	}

	mqttV3Addrs := parseAddrList(*mqttV3Flag)
	if len(mqttV3Addrs) == 0 {
		exitErr(errors.New("no MQTT v3 addresses configured"))
	}
	mqttV5Addrs := parseAddrList(*mqttV5Flag)
	if len(mqttV5Addrs) == 0 {
		exitErr(errors.New("no MQTT v5 addresses configured"))
	}

	amqpAddrs := parseAddrList(*amqpFlag)
	if len(amqpAddrs) == 0 {
		exitErr(errors.New("no AMQP addresses configured"))
	}

	cfg := runConfig{
		Scenario:             *scenarioConfigFlag,
		PayloadLabel:         payloadLabel,
		PayloadBytes:         payloadBytes,
		PayloadFromCLI:       payloadFromCLI,
		MQTTV3Addrs:          mqttV3Addrs,
		MQTTV5Addrs:          mqttV5Addrs,
		AMQPAddrs:            amqpAddrs,
		MinRatio:             *minRatioFlag,
		DrainTimeout:         *drainTimeoutFlag,
		Publishers:           *publishersFlag,
		Subscribers:          *subscribersFlag,
		MessagesPerPublisher: *messagesPerPublisherFlag,
		PublishInterval:      *publishIntervalFlag,
		PublishJitter:        *publishJitterFlag,
	}

	start := time.Now()
	ctx := context.Background()

	cfgTopic, err := loadTopicScenarioConfig(*scenarioConfigFlag)
	if err != nil {
		exitErr(err)
	}

	res, err := runConfiguredTopicScenario(ctx, cfg, cfgTopic)

	res.DurationMS = time.Since(start).Milliseconds()
	res.Timestamp = time.Now().UTC().Format(time.RFC3339)
	if err != nil {
		res.Pass = false
		if res.Notes == "" {
			res.Notes = err.Error()
		} else {
			res.Notes = res.Notes + "; " + err.Error()
		}
	}

	if res.Expected > 0 {
		res.DeliveryRatio = float64(res.Received) / float64(res.Expected)
	}
	if res.DurationMS > 0 {
		sec := float64(res.DurationMS) / 1000.0
		res.PublishRateMPS = float64(res.Published) / sec
		res.ReceiveRateMPS = float64(res.Received) / sec
	}

	line, mErr := json.Marshal(res)
	if mErr != nil {
		exitErr(fmt.Errorf("failed to marshal result: %w", mErr))
	}

	fmt.Printf("scenario=%s desc=%q msg_size=%d sent=%d expected=%d received=%d publishers=%d subscribers=%d mps_sent=%.2f mps_recv=%.2f ratio=%.4f pass=%v errors=%d publish_errors=%d duration_ms=%d\n",
		res.Scenario, res.Description, res.PayloadBytes, res.Published, res.Expected, res.Received,
		res.Publishers, res.Subscribers, res.PublishRateMPS, res.ReceiveRateMPS,
		res.DeliveryRatio, res.Pass, res.Errors, res.PublishErrors, res.DurationMS)
	fmt.Println(string(line))

	if *jsonOutFlag != "" {
		if err := appendJSONLine(*jsonOutFlag, line); err != nil {
			exitErr(err)
		}
	}

	if !res.Pass {
		os.Exit(1)
	}
}

func loadTopicScenarioConfig(path string) (topicScenarioConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return topicScenarioConfig{}, fmt.Errorf("failed to read scenario config %s: %w", path, err)
	}

	var cfg topicScenarioConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return topicScenarioConfig{}, fmt.Errorf("failed to parse scenario config %s: %w", path, err)
	}

	return normalizeTopicScenarioConfig(cfg, path)
}

func normalizeTopicScenarioConfig(cfg topicScenarioConfig, path string) (topicScenarioConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		base := filepath.Base(path)
		cfg.Name = strings.TrimSuffix(base, filepath.Ext(base))
	}

	cfg.Pattern = strings.ToLower(strings.TrimSpace(cfg.Pattern))
	if cfg.Pattern != patternFanIn && cfg.Pattern != patternFanOut {
		return topicScenarioConfig{}, fmt.Errorf("config %q has invalid pattern %q (use fanin|fanout)", cfg.Name, cfg.Pattern)
	}

	flow := strings.ToLower(strings.TrimSpace(cfg.Flow))
	pubProto := strings.ToLower(strings.TrimSpace(cfg.PublisherProtocol))
	subProto := strings.ToLower(strings.TrimSpace(cfg.SubscriberProtocol))
	if flow != "" {
		parts := strings.Split(flow, "-")
		if len(parts) != 2 {
			return topicScenarioConfig{}, fmt.Errorf("config %q has invalid flow %q (use mqtt-mqtt|mqtt-amqp|amqp-mqtt|amqp-amqp)", cfg.Name, cfg.Flow)
		}
		if pubProto == "" {
			pubProto = parts[0]
		} else if pubProto != parts[0] {
			return topicScenarioConfig{}, fmt.Errorf("config %q publisher_protocol=%q conflicts with flow=%q", cfg.Name, cfg.PublisherProtocol, cfg.Flow)
		}
		if subProto == "" {
			subProto = parts[1]
		} else if subProto != parts[1] {
			return topicScenarioConfig{}, fmt.Errorf("config %q subscriber_protocol=%q conflicts with flow=%q", cfg.Name, cfg.SubscriberProtocol, cfg.Flow)
		}
	}
	if pubProto == "" || subProto == "" {
		return topicScenarioConfig{}, fmt.Errorf("config %q must define flow or both publisher_protocol/subscriber_protocol", cfg.Name)
	}
	if pubProto != protoMQTT && pubProto != protoAMQP {
		return topicScenarioConfig{}, fmt.Errorf("config %q has invalid publisher protocol %q (use mqtt|amqp)", cfg.Name, pubProto)
	}
	if subProto != protoMQTT && subProto != protoAMQP {
		return topicScenarioConfig{}, fmt.Errorf("config %q has invalid subscriber protocol %q (use mqtt|amqp)", cfg.Name, subProto)
	}

	if cfg.Publishers <= 0 {
		return topicScenarioConfig{}, fmt.Errorf("config %q must set publishers > 0", cfg.Name)
	}
	if cfg.MessagesPerPublisher <= 0 {
		return topicScenarioConfig{}, fmt.Errorf("config %q must set messages_per_publisher > 0", cfg.Name)
	}
	if cfg.Subscribers <= 0 {
		return topicScenarioConfig{}, fmt.Errorf("config %q must set subscribers > 0", cfg.Name)
	}
	if cfg.WildcardSubscribers < 0 {
		return topicScenarioConfig{}, fmt.Errorf("config %q has invalid wildcard_subscribers=%d", cfg.Name, cfg.WildcardSubscribers)
	}
	if cfg.WildcardSubscribers > cfg.Subscribers {
		cfg.WildcardSubscribers = cfg.Subscribers
	}

	if cfg.Pattern == patternFanIn {
		cfg.TopicCount = 1
	} else if cfg.TopicCount <= 0 {
		cfg.TopicCount = maxInt(4, minInt(64, cfg.Subscribers))
	}

	cfg.Topic = strings.TrimSpace(cfg.Topic)
	if cfg.Topic == "" {
		cfg.Topic = fmt.Sprintf("perf/topic/config/%s", strings.ReplaceAll(cfg.Name, " ", "-"))
	}
	cfg.Topic = strings.TrimSuffix(cfg.Topic, "/")
	if cfg.Topic == "" {
		return topicScenarioConfig{}, fmt.Errorf("config %q resolved to empty topic", cfg.Name)
	}

	if cfg.PublishInterval != "" {
		d, err := time.ParseDuration(cfg.PublishInterval)
		if err != nil {
			return topicScenarioConfig{}, fmt.Errorf("config %q has invalid publish_interval %q: %w", cfg.Name, cfg.PublishInterval, err)
		}
		cfg.resolvedPublishInterval = d
	}
	if cfg.PublishJitter != "" {
		d, err := time.ParseDuration(cfg.PublishJitter)
		if err != nil {
			return topicScenarioConfig{}, fmt.Errorf("config %q has invalid publish_jitter %q: %w", cfg.Name, cfg.PublishJitter, err)
		}
		cfg.resolvedPublishJitter = d
	}

	if cfg.DrainTimeout != "" {
		d, err := time.ParseDuration(cfg.DrainTimeout)
		if err != nil {
			return topicScenarioConfig{}, fmt.Errorf("config %q has invalid drain_timeout %q: %w", cfg.Name, cfg.DrainTimeout, err)
		}
		cfg.resolvedDrainTimeout = d
	}

	qos := 1
	if cfg.QoS != nil {
		if *cfg.QoS < 0 || *cfg.QoS > 2 {
			return topicScenarioConfig{}, fmt.Errorf("config %q has invalid qos=%d (use 0|1|2)", cfg.Name, *cfg.QoS)
		}
		qos = *cfg.QoS
	}

	cfg.resolvedPublisherProto = pubProto
	cfg.resolvedSubscriberProto = subProto
	cfg.resolvedQoS = byte(qos)
	cfg.Flow = pubProto + "-" + subProto
	return cfg, nil
}

func runConfiguredTopicScenario(ctx context.Context, cfg runConfig, sc topicScenarioConfig) (scenarioResult, error) {
	if cfg.Publishers > 0 {
		sc.Publishers = cfg.Publishers
	}
	if cfg.MessagesPerPublisher > 0 {
		sc.MessagesPerPublisher = cfg.MessagesPerPublisher
	}
	if cfg.Subscribers > 0 {
		sc.Subscribers = cfg.Subscribers
		if sc.WildcardSubscribers > sc.Subscribers {
			sc.WildcardSubscribers = sc.Subscribers
		}
	}
	if cfg.PublishInterval > 0 {
		sc.resolvedPublishInterval = cfg.PublishInterval
	}
	if cfg.PublishJitter > 0 {
		sc.resolvedPublishJitter = cfg.PublishJitter
	}
	if sc.resolvedDrainTimeout > 0 {
		cfg.DrainTimeout = sc.resolvedDrainTimeout
	}
	if !cfg.PayloadFromCLI && sc.PayloadBytes > 0 {
		cfg.PayloadBytes = sc.PayloadBytes
		cfg.PayloadLabel = fmt.Sprintf("%dB", sc.PayloadBytes)
	}

	res := scenarioResult{
		Scenario:     sc.Name,
		Description:  sc.Description,
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
		Publishers:   sc.Publishers,
		Subscribers:  sc.Subscribers,
	}

	runID := time.Now().UnixNano()
	mqttPublisherEndpoints := shuffledMQTTEndpoints(cfg.MQTTV3Addrs, cfg.MQTTV5Addrs, runID, mqttPublisherSeedSalt)
	mqttSubscriberEndpoints := shuffledMQTTEndpoints(cfg.MQTTV3Addrs, cfg.MQTTV5Addrs, runID, mqttSubscriberSeedSalt)
	amqpPublisherAddrs := shuffledAddrs(cfg.AMQPAddrs, runID, amqpPublisherSeedSalt)
	amqpSubscriberAddrs := shuffledAddrs(cfg.AMQPAddrs, runID, amqpSubscriberSeedSalt)

	baseTopic := strings.ReplaceAll(sc.Topic, "{run_id}", fmt.Sprintf("%d", runID))
	if baseTopic == "" {
		baseTopic = fmt.Sprintf("perf/topic/config/%s/%d", strings.ReplaceAll(sc.Name, " ", "-"), runID)
	}
	topicSet := buildConfiguredTopicSet(sc.Pattern, baseTopic, sc.TopicCount)
	filters := buildConfiguredSubscriberFilters(sc, baseTopic, topicSet, runID)
	matchCounts := buildTopicMatchCounts(topicSet, filters)

	var sent atomic.Int64
	var received atomic.Int64
	var attemptedExpected atomic.Int64
	var failedExpected atomic.Int64
	var errCount atomic.Int64
	var publishErrCount atomic.Int64
	seen := newDeduper()

	stopReport := startPeriodicStatusReport(sc.Name, func() int64 { return sent.Load() }, func() int64 { return received.Load() })
	defer stopReport()

	switch sc.resolvedSubscriberProto {
	case protoMQTT:
		subs, err := connectMQTTSubscribers(ctx, mqttSubscriberEndpoints, sc.Name, runID, sc.Subscribers)
		if err != nil {
			return res, err
		}
		defer disconnectMQTTClients(ctx, subs)

		for i, sub := range subs {
			filter := filters[i]
			subID := i
			if err := sub.Subscribe(ctx, filter, func(msg *msgclient.Message) {
				if !isCountablePayload(msg.Payload) {
					return
				}
				msgID := extractMsgID(msg.Payload)
				if seen.add(fmt.Sprintf("%d:%s", subID, msgID)) {
					received.Add(1)
				}
			}, msgclient.WithQoS(sc.resolvedQoS)); err != nil {
				errCount.Add(1)
				return res, fmt.Errorf("subscriber %d failed to subscribe %q: %w", i, filter, err)
			}
		}
	case protoAMQP:
		subs, err := connectAMQPTopicSubscribersWithFilters(ctx, amqpSubscriberAddrs, sc.Name, runID, filters, func(subID int, msg *msgclient.Message) {
			if isCountablePayload(msg.Payload) {
				msgID := extractMsgID(msg.Payload)
				if seen.add(fmt.Sprintf("%d:%s", subID, msgID)) {
					received.Add(1)
				}
			}
			if err := msg.Ack(); err != nil {
				errCount.Add(1)
			}
		})
		if err != nil {
			return res, err
		}
		defer closeAMQPClients(ctx, subs)
	default:
		return res, fmt.Errorf("unsupported subscriber protocol %q", sc.resolvedSubscriberProto)
	}

	time.Sleep(500 * time.Millisecond)

	topicFor := func(pubIdx, msgIdx int) string {
		if sc.Pattern == patternFanIn {
			return topicSet[0]
		}
		return topicSet[(pubIdx+msgIdx)%len(topicSet)]
	}
	onAttempt := func(topic string) {
		attemptedExpected.Add(matchCounts[topic])
	}
	onPublishError := func(topic string) {
		failedExpected.Add(matchCounts[topic])
		publishErrCount.Add(1)
	}

	var published int64
	switch sc.resolvedPublisherProto {
	case protoMQTT:
		published = runMQTTPublishers(ctx, mqttPublishParams{
			Endpoints:            mqttPublisherEndpoints,
			Scenario:             sc.Name,
			RunID:                runID,
			Publishers:           sc.Publishers,
			MessagesPerPublisher: sc.MessagesPerPublisher,
			PayloadSize:          cfg.PayloadBytes,
			PublishInterval:      resolvedPublishInterval(cfg.PublishInterval, sc.resolvedPublishInterval),
			PublishJitter:        resolvedPublishJitter(cfg.PublishJitter, sc.resolvedPublishJitter),
			TopicFor:             topicFor,
			OnAttempt:            onAttempt,
			OnPublishError:       onPublishError,
			ErrCount:             &errCount,
			PublishedCounter:     &sent,
			QoS:                  sc.resolvedQoS,
		})
	case protoAMQP:
		published = runAMQPTopicPublishers(ctx, amqpTopicPublishParams{
			Addrs:                amqpPublisherAddrs,
			Scenario:             sc.Name,
			RunID:                runID,
			Publishers:           sc.Publishers,
			MessagesPerPublisher: sc.MessagesPerPublisher,
			PayloadSize:          cfg.PayloadBytes,
			PublishInterval:      resolvedPublishInterval(cfg.PublishInterval, sc.resolvedPublishInterval),
			PublishJitter:        resolvedPublishJitter(cfg.PublishJitter, sc.resolvedPublishJitter),
			TopicFor:             topicFor,
			OnAttempt:            onAttempt,
			OnPublishError:       onPublishError,
			ErrCount:             &errCount,
			PublishedCounter:     &sent,
		})
	default:
		return res, fmt.Errorf("unsupported publisher protocol %q", sc.resolvedPublisherProto)
	}

	expected := attemptedExpected.Load() - failedExpected.Load()
	if expected < 0 {
		expected = 0
	}
	_ = waitForAtLeast(&received, expected, cfg.DrainTimeout)

	res.Published = published
	res.Expected = expected
	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.PublishErrors = publishErrCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.MinRatio
	res.Notes = fmt.Sprintf("config pattern=%s flow=%s topics=%d wildcard_subscribers=%d qos=%d publish_errors=%d", sc.Pattern, sc.Flow, len(topicSet), sc.WildcardSubscribers, sc.resolvedQoS, res.PublishErrors)
	if sc.resolvedPublisherProto != protoMQTT && sc.resolvedSubscriberProto != protoMQTT {
		res.Notes = res.Notes + "; qos applies to MQTT only"
	}
	if !res.Pass {
		res.Notes = res.Notes + fmt.Sprintf("; delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.MinRatio)
	}

	return res, nil
}

func resolvedPublishInterval(flagValue, cfgValue time.Duration) time.Duration {
	if cfgValue > 0 {
		return cfgValue
	}
	return flagValue
}

func resolvedPublishJitter(flagValue, cfgValue time.Duration) time.Duration {
	if cfgValue > 0 {
		return cfgValue
	}
	return flagValue
}

func buildConfiguredTopicSet(pattern, baseTopic string, topicCount int) []string {
	if pattern == patternFanIn {
		return []string{baseTopic}
	}
	out := make([]string, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		out = append(out, fmt.Sprintf("%s/t/%03d", baseTopic, i))
	}
	return out
}

func buildConfiguredSubscriberFilters(sc topicScenarioConfig, baseTopic string, topicSet []string, seed int64) []string {
	filters := make([]string, sc.Subscribers)
	for i := 0; i < sc.Subscribers; i++ {
		if sc.Pattern == patternFanIn {
			filters[i] = topicSet[0]
		} else {
			filters[i] = topicSet[i%len(topicSet)]
		}
	}

	if sc.WildcardSubscribers <= 0 {
		return filters
	}

	patterns := sc.WildcardPatterns
	if len(patterns) == 0 {
		if sc.Pattern == patternFanIn {
			patterns = []string{
				"{topic}",
				"{base}/#",
			}
		} else {
			patterns = []string{
				"{base}/t/+",
				"{base}/#",
			}
		}
	}

	rng := rand.New(rand.NewSource(seed))
	perm := rng.Perm(sc.Subscribers)
	for i := 0; i < sc.WildcardSubscribers; i++ {
		idx := perm[i]
		p := patterns[i%len(patterns)]
		p = strings.ReplaceAll(p, "{base}", baseTopic)
		p = strings.ReplaceAll(p, "{topic}", filters[idx])
		filters[idx] = p
	}
	return filters
}

func buildTopicMatchCounts(topicSet, filters []string) map[string]int64 {
	matchCounts := make(map[string]int64, len(topicSet))
	for _, topic := range topicSet {
		var count int64
		for _, filter := range filters {
			if topics.TopicMatch(filter, topic) {
				count++
			}
		}
		matchCounts[topic] = count
	}
	return matchCounts
}

func connectMQTTSubscribers(ctx context.Context, endpoints []mqttEndpoint, scenario string, runID int64, count int) ([]*msgclient.Client, error) {
	clients := make([]*msgclient.Client, 0, count)
	for i := 0; i < count; i++ {
		endpoint := mqttEndpointByOffset(endpoints, i, subscriberNodeOffset)
		clientID := fmt.Sprintf("%s-sub-%d-%d", scenario, runID, i)
		client, err := connectMQTTClient(ctx, endpoint.Addr, endpoint.ProtocolVersion, clientID)
		if err != nil {
			disconnectMQTTClients(ctx, clients)
			return nil, fmt.Errorf("failed to connect MQTT subscriber %s to %s (v%d): %w", clientID, endpoint.Addr, endpoint.ProtocolVersion, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func connectAMQPTopicSubscribersWithFilters(ctx context.Context, addrs []string, scenario string, runID int64, filters []string, handler func(subID int, msg *msgclient.Message)) ([]*msgclient.Client, error) {
	clients := make([]*msgclient.Client, 0, len(filters))
	for i, filter := range filters {
		addr := addrByOffset(addrs, i, subscriberNodeOffset)
		cid := i
		client, err := connectAMQPClient(ctx, addr)
		if err != nil {
			closeAMQPClients(ctx, clients)
			return nil, fmt.Errorf("failed to connect AMQP subscriber %d to %s: %w", i, addr, err)
		}
		if err := client.Subscribe(ctx, filter, func(msg *msgclient.Message) {
			handler(cid, msg)
		}, msgclient.WithAutoAck(false)); err != nil {
			_ = client.Close(ctx)
			closeAMQPClients(ctx, clients)
			return nil, fmt.Errorf("failed AMQP subscribe for scenario=%s run=%d filter=%q: %w", scenario, runID, filter, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

type mqttPublishParams struct {
	Endpoints            []mqttEndpoint
	Scenario             string
	RunID                int64
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	PublishJitter        time.Duration
	TopicFor             func(pubIdx, msgIdx int) string
	OnAttempt            func(topic string)
	OnPublishError       func(topic string)
	ErrCount             *atomic.Int64
	PublishedCounter     *atomic.Int64
	QoS                  byte
}

func newPublishRNG(runID int64, publisherIdx int) *rand.Rand {
	seed := runID + int64(publisherIdx+1)*104729
	return rand.New(rand.NewSource(seed))
}

func initialPublishDelay(jitter time.Duration, rng *rand.Rand) time.Duration {
	if jitter <= 0 || rng == nil {
		return 0
	}
	return time.Duration(rng.Int63n(int64(jitter) + 1))
}

func jitteredPublishInterval(base, jitter time.Duration, rng *rand.Rand) time.Duration {
	if base < 0 {
		base = 0
	}
	if jitter <= 0 || rng == nil {
		return base
	}
	delta := time.Duration(rng.Int63n(int64(jitter)*2+1)) - jitter
	next := base + delta
	if next < 0 {
		return 0
	}
	return next
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func runMQTTPublishers(ctx context.Context, params mqttPublishParams) int64 {
	pubClients := make([]*msgclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		endpoint := mqttEndpointByOffset(params.Endpoints, i, publisherNodeOffset)
		clientID := fmt.Sprintf("%s-pub-%d-%d", params.Scenario, params.RunID, i)
		client, err := connectMQTTClient(ctx, endpoint.Addr, endpoint.ProtocolVersion, clientID)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer disconnectMQTTClients(ctx, pubClients)

	counter := params.PublishedCounter
	if counter == nil {
		counter = &atomic.Int64{}
	}
	qos := params.QoS
	if qos > 2 {
		qos = 1
	}
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *msgclient.Client) {
			defer wg.Done()
			rng := newPublishRNG(params.RunID, idx)
			if !sleepWithContext(ctx, initialPublishDelay(params.PublishJitter, rng)) {
				return
			}

			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				topic := params.TopicFor(idx, msgIdx)
				if params.OnAttempt != nil {
					params.OnAttempt(topic)
				}
				msgID := fmt.Sprintf("%s-%d-%d", params.Scenario, idx, msgIdx)
				payload := makePayload(msgID, params.PayloadSize)
				err := client.Publish(ctx, topic, payload, msgclient.WithQoS(qos))
				if err != nil {
					if params.OnPublishError != nil {
						params.OnPublishError(topic)
					}
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				counter.Add(1)
				if !sleepWithContext(ctx, jitteredPublishInterval(params.PublishInterval, params.PublishJitter, rng)) {
					return
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return counter.Load()
}

type amqpTopicPublishParams struct {
	Addrs                []string
	Scenario             string
	RunID                int64
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	PublishJitter        time.Duration
	TopicFor             func(pubIdx, msgIdx int) string
	OnAttempt            func(topic string)
	OnPublishError       func(topic string)
	ErrCount             *atomic.Int64
	PublishedCounter     *atomic.Int64
}

func runAMQPTopicPublishers(ctx context.Context, params amqpTopicPublishParams) int64 {
	pubClients := make([]*msgclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		addr := addrByOffset(params.Addrs, i, publisherNodeOffset)
		client, err := connectAMQPClient(ctx, addr)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer closeAMQPClients(ctx, pubClients)

	counter := params.PublishedCounter
	if counter == nil {
		counter = &atomic.Int64{}
	}
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *msgclient.Client) {
			defer wg.Done()
			rng := newPublishRNG(params.RunID, idx)
			if !sleepWithContext(ctx, initialPublishDelay(params.PublishJitter, rng)) {
				return
			}

			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				topic := params.TopicFor(idx, msgIdx)
				if params.OnAttempt != nil {
					params.OnAttempt(topic)
				}
				msgID := fmt.Sprintf("%s-at-%d-%d", params.Scenario, idx, msgIdx)
				payload := makePayload(msgID, params.PayloadSize)
				err := client.Publish(ctx, topic, payload)
				if err != nil {
					if params.OnPublishError != nil {
						params.OnPublishError(topic)
					}
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				counter.Add(1)
				if !sleepWithContext(ctx, jitteredPublishInterval(params.PublishInterval, params.PublishJitter, rng)) {
					return
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return counter.Load()
}

func connectMQTTClient(ctx context.Context, addr string, protocolVersion byte, clientID string) (*msgclient.Client, error) {
	if protocolVersion != 4 && protocolVersion != 5 {
		return nil, fmt.Errorf("unsupported MQTT protocol version %d", protocolVersion)
	}
	opts := mqttclient.NewOptions().
		SetServers(addr).
		SetClientID(clientID).
		SetProtocolVersion(protocolVersion).
		SetCleanSession(true).
		SetKeepAlive(20 * time.Second).
		SetConnectTimeout(8 * time.Second).
		SetAckTimeout(12 * time.Second).
		SetAutoReconnect(true).
		SetMessageChanSize(4096)

	c, err := msgclient.NewMQTT(opts)
	if err != nil {
		return nil, err
	}
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func connectAMQPClient(ctx context.Context, addr string) (*msgclient.Client, error) {
	opts := amqpclient.NewOptions().
		SetAddress(addr).
		SetCredentials("guest", "guest").
		SetPrefetch(512, 0).
		SetAutoReconnect(true)

	c, err := msgclient.NewAMQP(opts)
	if err != nil {
		return nil, err
	}
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func disconnectMQTTClients(ctx context.Context, clients []*msgclient.Client) {
	for _, c := range clients {
		if c == nil {
			continue
		}
		_ = c.Close(ctx)
	}
}

func closeAMQPClients(ctx context.Context, clients []*msgclient.Client) {
	for _, c := range clients {
		if c == nil {
			continue
		}
		_ = c.Close(ctx)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseAddrList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func waitForAtLeast(counter *atomic.Int64, target int64, timeout time.Duration) bool {
	if target <= 0 {
		return true
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= target {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return counter.Load() >= target
}

func makePayload(msgID string, size int) []byte {
	prefix := msgID + "|"
	if size <= len(prefix) {
		return []byte(prefix)
	}
	buf := make([]byte, size)
	copy(buf, prefix)
	for i := len(prefix); i < size; i++ {
		buf[i] = byte('a' + (i % 26))
	}
	return buf
}

func extractMsgID(payload []byte) string {
	idx := bytes.IndexByte(payload, '|')
	if idx <= 0 {
		if len(payload) == 0 {
			return ""
		}
		if len(payload) > 64 {
			return string(payload[:64])
		}
		return string(payload)
	}
	return string(payload[:idx])
}

func isCountablePayload(payload []byte) bool {
	return bytes.IndexByte(payload, '|') > 0
}

func ratio(received, expected int64) float64 {
	if expected <= 0 {
		if received > 0 {
			return 1
		}
		return 0
	}
	return float64(received) / float64(expected)
}

func appendJSONLine(path string, line []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open json output %s: %w", path, err)
	}
	defer f.Close()

	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("failed to write json line: %w", err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	return nil
}

func payloadSizeFromLabel(label string) (int, error) {
	switch strings.ToLower(label) {
	case "small":
		return 128, nil
	case "medium":
		return 2048, nil
	case "large":
		return 32768, nil
	default:
		return 0, fmt.Errorf("invalid payload label %q (use small|medium|large)", label)
	}
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
