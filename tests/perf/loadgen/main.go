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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqttclient "github.com/absmach/fluxmq/client"
	amqpclient "github.com/absmach/fluxmq/client/amqp"
	"github.com/absmach/fluxmq/topics"
)

var allScenarios = []string{
	"mqtt-fanin",
	"mqtt-fanout",
	"amqp-fanin",
	"amqp-fanout",
	"mqtt-substorm",
	"queue-fanin",
	"queue-fanout",
	"mqtt-consumer-groups",
	"mqtt-shared-subscriptions",
	"mqtt-last-will",
	"amqp091-consumer-groups",
	"amqp091-stream-cursor",
	"amqp091-retention-policies",
	"bridge-queue-mqtt-amqp",
	"bridge-queue-amqp-mqtt",
}

type runConfig struct {
	Scenario             string
	PayloadLabel         string
	PayloadBytes         int
	MQTTAddrs            []string
	AMQPAddrs            []string
	MinRatio             float64
	QueueMinRatio        float64
	DrainTimeout         time.Duration
	Publishers           int
	Subscribers          int
	ConsumerGroups       int
	ConsumersPerGroup    int
	MessagesPerPublisher int
	PublishInterval      time.Duration
}

type scenarioResult struct {
	Timestamp         string  `json:"timestamp"`
	Scenario          string  `json:"scenario"`
	Description       string  `json:"description"`
	PayloadLabel      string  `json:"payload_label"`
	PayloadBytes      int     `json:"payload_bytes"`
	Publishers        int     `json:"publishers"`
	Subscribers       int     `json:"subscribers"`
	ConsumerGroups    int     `json:"consumer_groups"`
	ConsumersPerGroup int     `json:"consumers_per_group"`
	Published         int64   `json:"published"`
	Expected          int64   `json:"expected"`
	Received          int64   `json:"received"`
	DeliveryRatio     float64 `json:"delivery_ratio"`
	Errors            int64   `json:"errors"`
	SubscribeOps      int64   `json:"subscribe_ops,omitempty"`
	UnsubscribeOps    int64   `json:"unsubscribe_ops,omitempty"`
	PublishRateMPS    float64 `json:"publish_rate_mps"`
	ReceiveRateMPS    float64 `json:"receive_rate_mps"`
	DurationMS        int64   `json:"duration_ms"`
	Pass              bool    `json:"pass"`
	Notes             string  `json:"notes,omitempty"`
}

type deduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

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

func main() {
	scenarioFlag := flag.String("scenario", "", "Scenario to run")
	payloadFlag := flag.String("payload", "small", "Payload preset: small|medium|large")
	payloadBytesFlag := flag.Int("payload-bytes", 0, "Payload size in bytes (overrides -payload)")
	mqttFlag := flag.String("mqtt-addrs", "127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885", "Comma-separated MQTT broker addresses")
	amqpFlag := flag.String("amqp-addrs", "127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684", "Comma-separated AMQP 0.9.1 broker addresses")
	publishersFlag := flag.Int("publishers", 0, "Override concurrent publishers")
	subscribersFlag := flag.Int("subscribers", 0, "Override concurrent subscribers/consumers")
	consumerGroupsFlag := flag.Int("consumer-groups", 0, "Override queue consumer groups for fanout scenarios")
	consumersPerGroupFlag := flag.Int("consumers-per-group", 0, "Override consumers per consumer group")
	messagesPerPublisherFlag := flag.Int("messages-per-publisher", 0, "Override messages each publisher sends")
	publishIntervalFlag := flag.Duration("publish-interval", 0, "Pause between each publish per publisher (e.g. 5ms)")
	minRatioFlag := flag.Float64("min-ratio", 0.95, "Minimum delivery ratio for non-queue topic scenarios")
	queueMinRatioFlag := flag.Float64("queue-min-ratio", 0.99, "Minimum delivery ratio for queue/bridge-queue scenarios")
	drainTimeoutFlag := flag.Duration("drain-timeout", 45*time.Second, "Max wait time for message drain after publishers finish")
	jsonOutFlag := flag.String("json-out", "", "Optional file to append one JSON line result")
	listFlag := flag.Bool("list-scenarios", false, "Print supported scenarios and exit")
	flag.Parse()

	if *listFlag {
		for _, sc := range allScenarios {
			fmt.Println(sc)
		}
		return
	}

	if *scenarioFlag == "" {
		exitErr(errors.New("-scenario is required (use -list-scenarios to inspect options)"))
	}

	payloadLabel := strings.ToLower(*payloadFlag)
	payloadBytes := 0
	if *payloadBytesFlag > 0 {
		payloadBytes = *payloadBytesFlag
		payloadLabel = fmt.Sprintf("%dB", payloadBytes)
	} else {
		var err error
		payloadBytes, err = payloadSizeFromLabel(*payloadFlag)
		if err != nil {
			exitErr(err)
		}
	}

	mqttAddrs := parseAddrList(*mqttFlag)
	if len(mqttAddrs) == 0 {
		exitErr(errors.New("no MQTT addresses configured"))
	}

	amqpAddrs := parseAddrList(*amqpFlag)
	if len(amqpAddrs) == 0 {
		exitErr(errors.New("no AMQP addresses configured"))
	}

	cfg := runConfig{
		Scenario:             *scenarioFlag,
		PayloadLabel:         payloadLabel,
		PayloadBytes:         payloadBytes,
		MQTTAddrs:            mqttAddrs,
		AMQPAddrs:            amqpAddrs,
		MinRatio:             *minRatioFlag,
		QueueMinRatio:        *queueMinRatioFlag,
		DrainTimeout:         *drainTimeoutFlag,
		Publishers:           *publishersFlag,
		Subscribers:          *subscribersFlag,
		ConsumerGroups:       *consumerGroupsFlag,
		ConsumersPerGroup:    *consumersPerGroupFlag,
		MessagesPerPublisher: *messagesPerPublisherFlag,
		PublishInterval:      *publishIntervalFlag,
	}

	start := time.Now()
	ctx := context.Background()

	res, err := runScenario(ctx, cfg)
	if res.Description == "" {
		res.Description = scenarioDescription(cfg.Scenario)
	}
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

	fmt.Printf("scenario=%s desc=%q msg_size=%d sent=%d expected=%d received=%d publishers=%d consumers=%d groups=%d mps_sent=%.2f mps_recv=%.2f ratio=%.4f pass=%v errors=%d duration_ms=%d\n",
		res.Scenario, res.Description, res.PayloadBytes, res.Published, res.Expected, res.Received,
		res.Publishers, res.Subscribers, res.ConsumerGroups, res.PublishRateMPS, res.ReceiveRateMPS,
		res.DeliveryRatio, res.Pass, res.Errors, res.DurationMS)
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

func runScenario(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	switch cfg.Scenario {
	case "mqtt-fanin":
		return runMQTTTopicScenario(ctx, cfg, "mqtt-fanin", workloadMQTTFanin(cfg))
	case "mqtt-fanout":
		return runMQTTTopicScenario(ctx, cfg, "mqtt-fanout", workloadMQTTFanout(cfg))
	case "amqp-fanin":
		return runAMQPTopicScenario(ctx, cfg, "amqp-fanin", workloadAMQPFanin(cfg))
	case "amqp-fanout":
		return runAMQPTopicScenario(ctx, cfg, "amqp-fanout", workloadAMQPFanout(cfg))
	case "mqtt-substorm":
		return runMQTTSubscriptionStorm(ctx, cfg)
	case "queue-fanin":
		return runQueueFanin(ctx, cfg)
	case "queue-fanout":
		return runQueueFanout(ctx, cfg)
	case "mqtt-consumer-groups":
		return runMQTTConsumerGroups(ctx, cfg, "mqtt-consumer-groups")
	case "mqtt-shared-subscriptions":
		return runMQTTSharedSubscriptions(ctx, cfg)
	case "mqtt-last-will":
		return runMQTTLastWill(ctx, cfg)
	case "amqp091-consumer-groups":
		return runAMQP091ConsumerGroups(ctx, cfg)
	case "amqp091-stream-cursor":
		return runAMQP091StreamCursor(ctx, cfg)
	case "amqp091-retention-policies":
		return runAMQP091RetentionPolicies(ctx, cfg)
	case "bridge-queue-mqtt-amqp":
		return runBridgeQueueMQTTToAMQP(ctx, cfg)
	case "bridge-queue-amqp-mqtt":
		return runBridgeQueueAMQPToMQTT(ctx, cfg)
	default:
		return scenarioResult{}, fmt.Errorf("unsupported scenario %q; valid: %s", cfg.Scenario, strings.Join(allScenarios, ", "))
	}
}

type topicWorkload struct {
	Publishers           int
	Subscribers          int
	MessagesPerPublisher int
}

func workloadMQTTFanin(cfg runConfig) topicWorkload {
	w := topicWorkload{Publishers: 120, Subscribers: 40, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 20, 5, 1)}
	applyTopicOverrides(&w, cfg)
	return w
}

func workloadMQTTFanout(cfg runConfig) topicWorkload {
	w := topicWorkload{Publishers: 20, Subscribers: 180, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 30, 8, 1)}
	applyTopicOverrides(&w, cfg)
	return w
}

func workloadAMQPFanin(cfg runConfig) topicWorkload {
	w := topicWorkload{Publishers: 120, Subscribers: 40, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 20, 5, 1)}
	applyTopicOverrides(&w, cfg)
	return w
}

func workloadAMQPFanout(cfg runConfig) topicWorkload {
	w := topicWorkload{Publishers: 20, Subscribers: 180, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 30, 8, 1)}
	applyTopicOverrides(&w, cfg)
	return w
}

func runMQTTTopicScenario(ctx context.Context, cfg runConfig, scenarioName string, workload topicWorkload) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     scenarioName,
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
		Publishers:   workload.Publishers,
		Subscribers:  workload.Subscribers,
	}

	runID := time.Now().UnixNano()
	topic := fmt.Sprintf("perf/topic/%s/%d", scenarioName, runID)

	var received atomic.Int64
	var errCount atomic.Int64

	subs, err := connectMQTTSubscribers(cfg.MQTTAddrs, scenarioName, runID, workload.Subscribers, func(_ int, _ string, _ *mqttclient.Message) {
		received.Add(1)
	})
	if err != nil {
		return res, err
	}
	defer disconnectMQTTClients(subs)

	for i, sub := range subs {
		if err := sub.SubscribeSingle(topic, 1); err != nil {
			errCount.Add(1)
			return res, fmt.Errorf("subscriber %d failed to subscribe %q: %w", i, topic, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	published := runMQTTPublishers(ctx, mqttPublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             scenarioName,
		RunID:                runID,
		Publishers:           workload.Publishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		TopicFor: func(_ int, _ int) string {
			return topic
		},
		ErrCount: &errCount,
	})

	expected := published * int64(workload.Subscribers)
	_ = waitForAtLeast(&received, expected, cfg.DrainTimeout)

	res.Published = published
	res.Expected = expected
	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.MinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.MinRatio)
	}

	return res, nil
}

func runAMQPTopicScenario(ctx context.Context, cfg runConfig, scenarioName string, workload topicWorkload) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     scenarioName,
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
		Publishers:   workload.Publishers,
		Subscribers:  workload.Subscribers,
	}

	runID := time.Now().UnixNano()
	topic := fmt.Sprintf("perf/topic/%s/%d", scenarioName, runID)
	// AMQP 0.9.1 non-queue pub/sub in this architecture is broker-local.
	// Keep publishers/subscribers on the same AMQP endpoint for deterministic fan-in/fan-out expectations.
	topicAddrs := []string{cfg.AMQPAddrs[0]}

	var received atomic.Int64
	var errCount atomic.Int64
	dedup := newDeduper()

	subs, err := connectAMQPTopicSubscribers(topicAddrs, scenarioName, runID, workload.Subscribers, topic, func(subID int, msg *amqpclient.Message) {
		msgID := extractMsgID(msg.Body)
		if dedup.add(fmt.Sprintf("%d:%s", subID, msgID)) {
			received.Add(1)
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, err
	}
	defer closeAMQPClients(subs)

	time.Sleep(500 * time.Millisecond)

	published := runAMQPTopicPublishers(ctx, amqpTopicPublishParams{
		Addrs:                topicAddrs,
		Scenario:             scenarioName,
		RunID:                runID,
		Publishers:           workload.Publishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		TopicFor: func(_ int, _ int) string {
			return topic
		},
		ErrCount: &errCount,
	})

	expected := published * int64(workload.Subscribers)
	_ = waitForAtLeast(&received, expected, cfg.DrainTimeout)

	res.Published = published
	res.Expected = expected
	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.MinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.MinRatio)
	}

	return res, nil
}

func runMQTTSubscriptionStorm(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "mqtt-substorm",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type stormWorkload struct {
		Publishers           int
		StableSubscribers    int
		ChurnSubscribers     int
		MessagesPerPublisher int
	}

	workload := func() stormWorkload {
		w := stormWorkload{
			Publishers:           40,
			StableSubscribers:    80,
			ChurnSubscribers:     140,
			MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 40, 15, 4),
		}
		if cfg.Publishers > 0 {
			w.Publishers = cfg.Publishers
		}
		if cfg.MessagesPerPublisher > 0 {
			w.MessagesPerPublisher = cfg.MessagesPerPublisher
		}
		if cfg.Subscribers > 0 {
			w.StableSubscribers = cfg.Subscribers / 2
			if w.StableSubscribers < 1 {
				w.StableSubscribers = 1
			}
			w.ChurnSubscribers = cfg.Subscribers - w.StableSubscribers
			if w.ChurnSubscribers < 1 {
				w.ChurnSubscribers = 1
			}
		}
		return w
	}()

	res.Publishers = workload.Publishers
	res.Subscribers = workload.StableSubscribers + workload.ChurnSubscribers

	runID := time.Now().UnixNano()
	base := fmt.Sprintf("perf/topic/storm/%d", runID)
	stormTopics := makeStormTopics(base, 160)

	stableFilters := make([]string, 0, workload.StableSubscribers)
	var stableReceived atomic.Int64
	var expected atomic.Int64
	var errCount atomic.Int64
	var subOps atomic.Int64
	var unsubOps atomic.Int64

	stableSubs, err := connectMQTTSubscribers(cfg.MQTTAddrs, "mqtt-substorm-stable", runID, workload.StableSubscribers, func(_ int, _ string, _ *mqttclient.Message) {
		stableReceived.Add(1)
	})
	if err != nil {
		return res, err
	}
	defer disconnectMQTTClients(stableSubs)

	for i, sub := range stableSubs {
		filter := stormFilter(base, stormTopics, i)
		stableFilters = append(stableFilters, filter)
		if err := sub.SubscribeSingle(filter, 1); err != nil {
			errCount.Add(1)
			return res, fmt.Errorf("stable subscriber %d failed to subscribe %q: %w", i, filter, err)
		}
	}

	churnSubs, err := connectMQTTSubscribers(cfg.MQTTAddrs, "mqtt-substorm-churn", runID, workload.ChurnSubscribers, nil)
	if err != nil {
		return res, err
	}
	defer disconnectMQTTClients(churnSubs)

	stopChurn := make(chan struct{})
	var churnWG sync.WaitGroup
	for i, sub := range churnSubs {
		churnWG.Add(1)
		go func(idx int, c *mqttclient.Client) {
			defer churnWG.Done()
			topicIndex := idx % len(stormTopics)
			for {
				select {
				case <-stopChurn:
					return
				case <-ctx.Done():
					return
				default:
				}

				topic := stormTopics[topicIndex%len(stormTopics)]
				topicIndex++
				if err := c.SubscribeSingle(topic, 1); err == nil {
					subOps.Add(1)
				} else {
					errCount.Add(1)
				}
				if err := c.Unsubscribe(topic); err == nil {
					unsubOps.Add(1)
				} else {
					errCount.Add(1)
				}

				wild := base + "/device/+/metric/+"
				if err := c.SubscribeSingle(wild, 1); err == nil {
					subOps.Add(1)
				} else {
					errCount.Add(1)
				}
				if err := c.Unsubscribe(wild); err == nil {
					unsubOps.Add(1)
				} else {
					errCount.Add(1)
				}
			}
		}(i, sub)
	}

	time.Sleep(600 * time.Millisecond)

	published := runMQTTPublishers(ctx, mqttPublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             "mqtt-substorm",
		RunID:                runID,
		Publishers:           workload.Publishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		TopicFor: func(pubIdx, msgIdx int) string {
			return stormTopics[(pubIdx*97+msgIdx)%len(stormTopics)]
		},
		OnPublished: func(topic string) {
			var matches int64
			for _, filter := range stableFilters {
				if topics.TopicMatch(filter, topic) {
					matches++
				}
			}
			expected.Add(matches)
		},
		ErrCount: &errCount,
	})

	close(stopChurn)
	churnWG.Wait()

	_ = waitForAtLeast(&stableReceived, expected.Load(), cfg.DrainTimeout)

	res.Published = published
	res.Expected = expected.Load()
	res.Received = stableReceived.Load()
	res.Errors = errCount.Load()
	res.SubscribeOps = subOps.Load()
	res.UnsubscribeOps = unsubOps.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.MinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("storm delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.MinRatio)
	}
	return res, nil
}

func runQueueFanin(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "queue-fanin",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type queueFaninWorkload struct {
		Publishers           int
		Consumers            int
		MessagesPerPublisher int
	}

	workload := func() queueFaninWorkload {
		w := queueFaninWorkload{Publishers: 120, Consumers: 80, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 30, 12, 4)}
		if cfg.Publishers > 0 {
			w.Publishers = cfg.Publishers
		}
		if cfg.Subscribers > 0 {
			w.Consumers = cfg.Subscribers
		}
		if cfg.MessagesPerPublisher > 0 {
			w.MessagesPerPublisher = cfg.MessagesPerPublisher
		}
		return w
	}()

	res.Publishers = workload.Publishers
	res.Subscribers = workload.Consumers
	res.ConsumerGroups = 1
	res.ConsumersPerGroup = workload.Consumers

	runID := time.Now().UnixNano()
	queueName := fmt.Sprintf("perf-queue-fanin-%d", runID)
	group := "workers"

	var errCount atomic.Int64
	var receivedUnique atomic.Int64
	seen := newDeduper()

	consumers, err := connectMQTTQueueConsumers(cfg.MQTTAddrs, "queue-fanin", runID, workload.Consumers, queueName, group, func(_ int, msg *mqttclient.QueueMessage) {
		msgID := extractMsgID(msg.Payload)
		if seen.add(msgID) {
			receivedUnique.Add(1)
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, err
	}
	defer disconnectMQTTClients(consumers)

	time.Sleep(500 * time.Millisecond)

	published := runMQTTQueuePublishers(ctx, mqttQueuePublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             "queue-fanin",
		RunID:                runID,
		QueueName:            queueName,
		Publishers:           workload.Publishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	_ = waitForAtLeast(&receivedUnique, published, cfg.DrainTimeout)

	res.Published = published
	res.Expected = published
	res.Received = receivedUnique.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("queue fanin delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runQueueFanout(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "queue-fanout",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type queueFanoutWorkload struct {
		Publishers           int
		Groups               int
		ConsumersPerGroup    int
		MessagesPerPublisher int
	}

	workload := func() queueFanoutWorkload {
		w := queueFanoutWorkload{Publishers: 80, Groups: 3, ConsumersPerGroup: 40, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 25, 10, 3)}
		if cfg.Publishers > 0 {
			w.Publishers = cfg.Publishers
		}
		if cfg.ConsumerGroups > 0 {
			w.Groups = cfg.ConsumerGroups
		}
		if cfg.ConsumersPerGroup > 0 {
			w.ConsumersPerGroup = cfg.ConsumersPerGroup
		} else if cfg.Subscribers > 0 {
			if w.Groups <= 0 {
				w.Groups = 1
			}
			w.ConsumersPerGroup = maxInt(1, cfg.Subscribers/w.Groups)
		}
		if cfg.MessagesPerPublisher > 0 {
			w.MessagesPerPublisher = cfg.MessagesPerPublisher
		}
		return w
	}()

	res.Publishers = workload.Publishers
	res.ConsumerGroups = workload.Groups
	res.ConsumersPerGroup = workload.ConsumersPerGroup
	res.Subscribers = workload.Groups * workload.ConsumersPerGroup

	runID := time.Now().UnixNano()
	queueName := fmt.Sprintf("perf-queue-fanout-%d", runID)

	var errCount atomic.Int64
	groupCounters := make([]*atomic.Int64, workload.Groups)
	groupSeen := make([]*deduper, workload.Groups)
	for i := 0; i < workload.Groups; i++ {
		groupCounters[i] = &atomic.Int64{}
		groupSeen[i] = newDeduper()
	}

	allConsumers := make([]*mqttclient.Client, 0, workload.Groups*workload.ConsumersPerGroup)
	for g := 0; g < workload.Groups; g++ {
		gid := g
		groupName := fmt.Sprintf("group-%d", g+1)
		consumers, err := connectMQTTQueueConsumers(cfg.MQTTAddrs, "queue-fanout", runID+int64(g), workload.ConsumersPerGroup, queueName, groupName, func(_ int, msg *mqttclient.QueueMessage) {
			msgID := extractMsgID(msg.Payload)
			if groupSeen[gid].add(msgID) {
				groupCounters[gid].Add(1)
			}
			if err := msg.Ack(); err != nil {
				errCount.Add(1)
			}
		})
		if err != nil {
			disconnectMQTTClients(allConsumers)
			return res, err
		}
		allConsumers = append(allConsumers, consumers...)
	}
	defer disconnectMQTTClients(allConsumers)

	time.Sleep(500 * time.Millisecond)

	published := runMQTTQueuePublishers(ctx, mqttQueuePublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             "queue-fanout",
		RunID:                runID,
		QueueName:            queueName,
		Publishers:           workload.Publishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	deadline := time.Now().Add(cfg.DrainTimeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, c := range groupCounters {
			if c.Load() < published {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	var totalReceived int64
	allGroupsReady := true
	for _, c := range groupCounters {
		count := c.Load()
		totalReceived += count
		if count < published {
			allGroupsReady = false
		}
	}

	res.Published = published
	res.Expected = published * int64(workload.Groups)
	res.Received = totalReceived
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = allGroupsReady && res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("queue fanout ratio %.4f below min %.4f or some groups lagged", res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runBridgeQueueMQTTToAMQP(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "bridge-queue-mqtt-amqp",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type bridgeQueueWorkload struct {
		MQTTPublishers       int
		AMQPConsumers        int
		MessagesPerPublisher int
	}

	workload := func() bridgeQueueWorkload {
		w := bridgeQueueWorkload{MQTTPublishers: 100, AMQPConsumers: 100, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 25, 10, 4)}
		if cfg.Publishers > 0 {
			w.MQTTPublishers = cfg.Publishers
		}
		if cfg.Subscribers > 0 {
			w.AMQPConsumers = cfg.Subscribers
		}
		if cfg.MessagesPerPublisher > 0 {
			w.MessagesPerPublisher = cfg.MessagesPerPublisher
		}
		return w
	}()

	res.Publishers = workload.MQTTPublishers
	res.Subscribers = workload.AMQPConsumers
	res.ConsumerGroups = 1
	res.ConsumersPerGroup = workload.AMQPConsumers

	runID := time.Now().UnixNano()
	queueName := fmt.Sprintf("perf-bridge-queue-mqtt-amqp-%d", runID)
	groupName := "bridge-workers"

	var errCount atomic.Int64
	var received atomic.Int64
	seen := newDeduper()

	amqpConsumers, err := connectAMQPQueueConsumers(cfg.AMQPAddrs, "bridge-queue-mqtt-amqp", runID, workload.AMQPConsumers, queueName, groupName, func(_ int, msg *amqpclient.QueueMessage) {
		msgID := extractMsgID(msg.Body)
		if seen.add(msgID) {
			received.Add(1)
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, err
	}
	defer closeAMQPClients(amqpConsumers)

	time.Sleep(500 * time.Millisecond)

	published := runMQTTQueuePublishers(ctx, mqttQueuePublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             "bridge-queue-mqtt-amqp",
		RunID:                runID,
		QueueName:            queueName,
		Publishers:           workload.MQTTPublishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	_ = waitForAtLeast(&received, published, cfg.DrainTimeout)

	res.Published = published
	res.Expected = published
	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("bridge queue mqtt->amqp ratio %.4f below min %.4f", res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runBridgeQueueAMQPToMQTT(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "bridge-queue-amqp-mqtt",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type bridgeQueueWorkload struct {
		AMQPPublishers       int
		MQTTConsumers        int
		MessagesPerPublisher int
	}

	workload := func() bridgeQueueWorkload {
		w := bridgeQueueWorkload{AMQPPublishers: 100, MQTTConsumers: 100, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 25, 10, 4)}
		if cfg.Publishers > 0 {
			w.AMQPPublishers = cfg.Publishers
		}
		if cfg.Subscribers > 0 {
			w.MQTTConsumers = cfg.Subscribers
		}
		if cfg.MessagesPerPublisher > 0 {
			w.MessagesPerPublisher = cfg.MessagesPerPublisher
		}
		return w
	}()

	res.Publishers = workload.AMQPPublishers
	res.Subscribers = workload.MQTTConsumers
	res.ConsumerGroups = 1
	res.ConsumersPerGroup = workload.MQTTConsumers

	runID := time.Now().UnixNano()
	queueName := fmt.Sprintf("perf-bridge-queue-amqp-mqtt-%d", runID)
	groupName := "bridge-workers"

	var errCount atomic.Int64
	var received atomic.Int64
	seen := newDeduper()

	mqttConsumers, err := connectMQTTQueueConsumers(cfg.MQTTAddrs, "bridge-queue-amqp-mqtt", runID, workload.MQTTConsumers, queueName, groupName, func(_ int, msg *mqttclient.QueueMessage) {
		msgID := extractMsgID(msg.Payload)
		if seen.add(msgID) {
			received.Add(1)
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, err
	}
	defer disconnectMQTTClients(mqttConsumers)

	time.Sleep(500 * time.Millisecond)

	published := runAMQPQueuePublishers(ctx, amqpQueuePublishParams{
		Addrs:                cfg.AMQPAddrs,
		Scenario:             "bridge-queue-amqp-mqtt",
		RunID:                runID,
		QueueName:            queueName,
		Publishers:           workload.AMQPPublishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	_ = waitForAtLeast(&received, published, cfg.DrainTimeout)

	res.Published = published
	res.Expected = published
	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("bridge queue amqp->mqtt ratio %.4f below min %.4f", res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runMQTTConsumerGroups(ctx context.Context, cfg runConfig, scenarioName string) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     scenarioName,
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type queueFanoutWorkload struct {
		Publishers           int
		Groups               int
		ConsumersPerGroup    int
		MessagesPerPublisher int
	}

	workload := func() queueFanoutWorkload {
		w := queueFanoutWorkload{Publishers: 80, Groups: 3, ConsumersPerGroup: 40, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 25, 10, 3)}
		if cfg.Publishers > 0 {
			w.Publishers = cfg.Publishers
		}
		if cfg.ConsumerGroups > 0 {
			w.Groups = cfg.ConsumerGroups
		}
		if cfg.ConsumersPerGroup > 0 {
			w.ConsumersPerGroup = cfg.ConsumersPerGroup
		} else if cfg.Subscribers > 0 {
			if w.Groups <= 0 {
				w.Groups = 1
			}
			w.ConsumersPerGroup = maxInt(1, cfg.Subscribers/w.Groups)
		}
		if cfg.MessagesPerPublisher > 0 {
			w.MessagesPerPublisher = cfg.MessagesPerPublisher
		}
		return w
	}()

	res.Publishers = workload.Publishers
	res.ConsumerGroups = workload.Groups
	res.ConsumersPerGroup = workload.ConsumersPerGroup
	res.Subscribers = workload.Groups * workload.ConsumersPerGroup

	runID := time.Now().UnixNano()
	queueName := fmt.Sprintf("perf-%s-%d", strings.ReplaceAll(scenarioName, "_", "-"), runID)

	var errCount atomic.Int64
	groupCounters := make([]*atomic.Int64, workload.Groups)
	groupSeen := make([]*deduper, workload.Groups)
	for i := 0; i < workload.Groups; i++ {
		groupCounters[i] = &atomic.Int64{}
		groupSeen[i] = newDeduper()
	}

	allConsumers := make([]*mqttclient.Client, 0, workload.Groups*workload.ConsumersPerGroup)
	for g := 0; g < workload.Groups; g++ {
		gid := g
		groupName := fmt.Sprintf("group-%d", g+1)
		consumers, err := connectMQTTQueueConsumers(cfg.MQTTAddrs, scenarioName, runID+int64(g), workload.ConsumersPerGroup, queueName, groupName, func(_ int, msg *mqttclient.QueueMessage) {
			msgID := extractMsgID(msg.Payload)
			if groupSeen[gid].add(msgID) {
				groupCounters[gid].Add(1)
			}
			if err := msg.Ack(); err != nil {
				errCount.Add(1)
			}
		})
		if err != nil {
			disconnectMQTTClients(allConsumers)
			return res, err
		}
		allConsumers = append(allConsumers, consumers...)
	}
	defer disconnectMQTTClients(allConsumers)

	time.Sleep(500 * time.Millisecond)

	published := runMQTTQueuePublishers(ctx, mqttQueuePublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             scenarioName,
		RunID:                runID,
		QueueName:            queueName,
		Publishers:           workload.Publishers,
		MessagesPerPublisher: workload.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	deadline := time.Now().Add(cfg.DrainTimeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, c := range groupCounters {
			if c.Load() < published {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	var totalReceived int64
	allGroupsReady := true
	for _, c := range groupCounters {
		count := c.Load()
		totalReceived += count
		if count < published {
			allGroupsReady = false
		}
	}

	res.Published = published
	res.Expected = published * int64(workload.Groups)
	res.Received = totalReceived
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = allGroupsReady && res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("%s ratio %.4f below min %.4f or some groups lagged", scenarioName, res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runAMQP091ConsumerGroups(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "amqp091-consumer-groups",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type workload struct {
		Publishers           int
		Groups               int
		ConsumersPerGroup    int
		MessagesPerPublisher int
	}

	w := workload{Publishers: 80, Groups: 3, ConsumersPerGroup: 40, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 25, 10, 3)}
	if cfg.Publishers > 0 {
		w.Publishers = cfg.Publishers
	}
	if cfg.ConsumerGroups > 0 {
		w.Groups = cfg.ConsumerGroups
	}
	if cfg.ConsumersPerGroup > 0 {
		w.ConsumersPerGroup = cfg.ConsumersPerGroup
	} else if cfg.Subscribers > 0 {
		if w.Groups <= 0 {
			w.Groups = 1
		}
		w.ConsumersPerGroup = maxInt(1, cfg.Subscribers/w.Groups)
	}
	if cfg.MessagesPerPublisher > 0 {
		w.MessagesPerPublisher = cfg.MessagesPerPublisher
	}

	res.Publishers = w.Publishers
	res.ConsumerGroups = w.Groups
	res.ConsumersPerGroup = w.ConsumersPerGroup
	res.Subscribers = w.Groups * w.ConsumersPerGroup

	runID := time.Now().UnixNano()
	queueName := fmt.Sprintf("perf-amqp091-consumer-groups-%d", runID)

	var errCount atomic.Int64
	groupCounters := make([]*atomic.Int64, w.Groups)
	groupSeen := make([]*deduper, w.Groups)
	for i := 0; i < w.Groups; i++ {
		groupCounters[i] = &atomic.Int64{}
		groupSeen[i] = newDeduper()
	}

	allConsumers := make([]*amqpclient.Client, 0, w.Groups*w.ConsumersPerGroup)
	for g := 0; g < w.Groups; g++ {
		gid := g
		groupName := fmt.Sprintf("group-%d", g+1)
		consumers, err := connectAMQPQueueConsumers(cfg.AMQPAddrs, "amqp091-consumer-groups", runID+int64(g), w.ConsumersPerGroup, queueName, groupName, func(_ int, msg *amqpclient.QueueMessage) {
			msgID := extractMsgID(msg.Body)
			if groupSeen[gid].add(msgID) {
				groupCounters[gid].Add(1)
			}
			if err := msg.Ack(); err != nil {
				errCount.Add(1)
			}
		})
		if err != nil {
			closeAMQPClients(allConsumers)
			return res, err
		}
		allConsumers = append(allConsumers, consumers...)
	}
	defer closeAMQPClients(allConsumers)

	time.Sleep(500 * time.Millisecond)

	published := runAMQPQueuePublishers(ctx, amqpQueuePublishParams{
		Addrs:                cfg.AMQPAddrs,
		Scenario:             "amqp091-consumer-groups",
		RunID:                runID,
		QueueName:            queueName,
		Publishers:           w.Publishers,
		MessagesPerPublisher: w.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	deadline := time.Now().Add(cfg.DrainTimeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, c := range groupCounters {
			if c.Load() < published {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	var totalReceived int64
	allGroupsReady := true
	for _, c := range groupCounters {
		count := c.Load()
		totalReceived += count
		if count < published {
			allGroupsReady = false
		}
	}

	res.Published = published
	res.Expected = published * int64(w.Groups)
	res.Received = totalReceived
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = allGroupsReady && res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("amqp091 consumer groups ratio %.4f below min %.4f or some groups lagged", res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runMQTTSharedSubscriptions(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "mqtt-shared-subscriptions",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type workload struct {
		Publishers           int
		Groups               int
		ConsumersPerGroup    int
		MessagesPerPublisher int
	}

	w := workload{Publishers: 60, Groups: 3, ConsumersPerGroup: 24, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 30, 12, 4)}
	if cfg.Publishers > 0 {
		w.Publishers = cfg.Publishers
	}
	if cfg.ConsumerGroups > 0 {
		w.Groups = cfg.ConsumerGroups
	}
	if cfg.ConsumersPerGroup > 0 {
		w.ConsumersPerGroup = cfg.ConsumersPerGroup
	} else if cfg.Subscribers > 0 {
		if w.Groups <= 0 {
			w.Groups = 1
		}
		w.ConsumersPerGroup = maxInt(1, cfg.Subscribers/w.Groups)
	}
	if cfg.MessagesPerPublisher > 0 {
		w.MessagesPerPublisher = cfg.MessagesPerPublisher
	}

	res.Publishers = w.Publishers
	res.ConsumerGroups = w.Groups
	res.ConsumersPerGroup = w.ConsumersPerGroup
	res.Subscribers = w.Groups * w.ConsumersPerGroup

	runID := time.Now().UnixNano()
	topic := fmt.Sprintf("perf/topic/mqtt-shared-subscriptions/%d", runID)

	var errCount atomic.Int64
	groupCounters := make([]*atomic.Int64, w.Groups)
	groupSeen := make([]*deduper, w.Groups)
	for i := 0; i < w.Groups; i++ {
		groupCounters[i] = &atomic.Int64{}
		groupSeen[i] = newDeduper()
	}

	allConsumers := make([]*mqttclient.Client, 0, w.Groups*w.ConsumersPerGroup)
	for g := 0; g < w.Groups; g++ {
		gid := g
		groupName := fmt.Sprintf("group-%d", g+1)
		filter := fmt.Sprintf("$share/%s/%s", groupName, topic)
		for i := 0; i < w.ConsumersPerGroup; i++ {
			addr := cfg.MQTTAddrs[(g*w.ConsumersPerGroup+i)%len(cfg.MQTTAddrs)]
			clientID := fmt.Sprintf("mqtt-shared-sub-g%d-c%d-%d", g, i, runID)
			client, err := connectMQTTClient(addr, clientID, func(msg *mqttclient.Message) {
				msgID := extractMsgID(msg.Payload)
				if groupSeen[gid].add(msgID) {
					groupCounters[gid].Add(1)
				}
			})
			if err != nil {
				disconnectMQTTClients(allConsumers)
				return res, fmt.Errorf("failed to connect shared subscriber %s: %w", clientID, err)
			}
			if err := client.SubscribeSingle(filter, 1); err != nil {
				_ = client.Disconnect()
				disconnectMQTTClients(allConsumers)
				return res, fmt.Errorf("failed to subscribe %s to %q: %w", clientID, filter, err)
			}
			allConsumers = append(allConsumers, client)
		}
	}
	defer disconnectMQTTClients(allConsumers)

	time.Sleep(500 * time.Millisecond)

	published := runMQTTPublishers(ctx, mqttPublishParams{
		Addrs:                cfg.MQTTAddrs,
		Scenario:             "mqtt-shared-subscriptions",
		RunID:                runID,
		Publishers:           w.Publishers,
		MessagesPerPublisher: w.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		TopicFor: func(_ int, _ int) string {
			return topic
		},
		ErrCount: &errCount,
	})

	deadline := time.Now().Add(cfg.DrainTimeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, c := range groupCounters {
			if c.Load() < published {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	var totalReceived int64
	allGroupsReady := true
	for _, c := range groupCounters {
		count := c.Load()
		totalReceived += count
		if count < published {
			allGroupsReady = false
		}
	}

	res.Published = published
	res.Expected = published * int64(w.Groups)
	res.Received = totalReceived
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = allGroupsReady && res.DeliveryRatio >= cfg.QueueMinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("shared subscription ratio %.4f below min %.4f or some groups lagged", res.DeliveryRatio, cfg.QueueMinRatio)
	}

	return res, nil
}

func runMQTTLastWill(_ context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "mqtt-last-will",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
	}

	type workload struct {
		WillPublishers int
		Subscribers    int
	}

	w := workload{WillPublishers: 80, Subscribers: 40}
	if cfg.Publishers > 0 {
		w.WillPublishers = cfg.Publishers
	}
	if cfg.Subscribers > 0 {
		w.Subscribers = cfg.Subscribers
	}

	res.Publishers = w.WillPublishers
	res.Subscribers = w.Subscribers

	runID := time.Now().UnixNano()
	willTopic := fmt.Sprintf("perf/topic/mqtt-last-will/%d", runID)

	var errCount atomic.Int64
	var received atomic.Int64
	seen := newDeduper()

	subs, err := connectMQTTSubscribers(cfg.MQTTAddrs, "mqtt-last-will-sub", runID, w.Subscribers, func(subID int, _ string, msg *mqttclient.Message) {
		msgID := extractMsgID(msg.Payload)
		if seen.add(fmt.Sprintf("%d:%s", subID, msgID)) {
			received.Add(1)
		}
	})
	if err != nil {
		return res, err
	}
	defer disconnectMQTTClients(subs)

	for i, sub := range subs {
		if err := sub.SubscribeSingle(willTopic, 1); err != nil {
			errCount.Add(1)
			return res, fmt.Errorf("subscriber %d failed to subscribe %q: %w", i, willTopic, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	willClients := make([]*mqttclient.Client, 0, w.WillPublishers)
	for i := 0; i < w.WillPublishers; i++ {
		addr := cfg.MQTTAddrs[i%len(cfg.MQTTAddrs)]
		clientID := fmt.Sprintf("mqtt-last-will-pub-%d-%d", runID, i)
		msgID := fmt.Sprintf("mqtt-last-will-%d", i)
		payload := makePayload(msgID, cfg.PayloadBytes)
		client, err := connectMQTTClientWithCustomizer(addr, clientID, nil, func(opts *mqttclient.Options) {
			opts.SetWill(willTopic, payload, 1, false)
			opts.SetAutoReconnect(false)
		})
		if err != nil {
			errCount.Add(1)
			continue
		}
		willClients = append(willClients, client)
	}

	for _, client := range willClients {
		// Close socket without DISCONNECT so broker emits last-will.
		if err := client.Close(); err != nil {
			errCount.Add(1)
		}
	}

	res.Publishers = len(willClients)
	res.Published = int64(len(willClients))
	res.Expected = int64(len(willClients) * w.Subscribers)

	_ = waitForAtLeast(&received, res.Expected, cfg.DrainTimeout)

	res.Received = received.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = res.DeliveryRatio >= cfg.MinRatio
	if !res.Pass {
		res.Notes = fmt.Sprintf("last-will delivery ratio %.4f below min %.4f", res.DeliveryRatio, cfg.MinRatio)
	}

	return res, nil
}

func runAMQP091StreamCursor(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "amqp091-stream-cursor",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
		Subscribers:  2,
	}

	type workload struct {
		Publishers           int
		MessagesPerPublisher int
	}

	w := workload{Publishers: 20, MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 80, 30, 10)}
	if cfg.Publishers > 0 {
		w.Publishers = cfg.Publishers
	}
	if cfg.MessagesPerPublisher > 0 {
		w.MessagesPerPublisher = cfg.MessagesPerPublisher
	}
	res.Publishers = w.Publishers

	runID := time.Now().UnixNano()
	streamName := fmt.Sprintf("perf-stream-cursor-%d", runID)

	var errCount atomic.Int64
	admin, err := connectAMQPClient(cfg.AMQPAddrs[0])
	if err != nil {
		return res, err
	}
	defer admin.Close()

	_, err = admin.DeclareStreamQueue(&amqpclient.StreamQueueOptions{
		Name:              streamName,
		Durable:           true,
		MaxLengthMessages: int64(maxInt(256, w.Publishers*w.MessagesPerPublisher*2)),
	})
	if err != nil {
		return res, fmt.Errorf("declare stream %s failed: %w", streamName, err)
	}

	published := runAMQPStreamPublishers(ctx, amqpStreamPublishParams{
		Addrs:                cfg.AMQPAddrs,
		Scenario:             "amqp091-stream-cursor",
		RunID:                runID,
		StreamName:           streamName,
		Publishers:           w.Publishers,
		MessagesPerPublisher: w.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		ErrCount:             &errCount,
	})

	readerFirst, err := connectAMQPClient(cfg.AMQPAddrs[0])
	if err != nil {
		return res, err
	}
	defer readerFirst.Close()

	var firstReceived atomic.Int64
	firstSeen := newDeduper()
	var offsetsMu sync.Mutex
	offsets := make([]uint64, 0, maxInt(0, int(published)))

	err = readerFirst.SubscribeToStream(&amqpclient.StreamConsumeOptions{
		QueueName:     streamName,
		ConsumerGroup: fmt.Sprintf("cursor-first-%d", runID),
		Offset:        "first",
		AutoAck:       false,
	}, func(msg *amqpclient.QueueMessage) {
		msgID := extractMsgID(msg.Body)
		if firstSeen.add(msgID) {
			firstReceived.Add(1)
			if off, ok := msg.StreamOffset(); ok {
				offsetsMu.Lock()
				offsets = append(offsets, off)
				offsetsMu.Unlock()
			}
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, fmt.Errorf("subscribe stream first failed: %w", err)
	}

	_ = waitForAtLeast(&firstReceived, published, cfg.DrainTimeout)
	_ = readerFirst.UnsubscribeFromStream(streamName)

	firstCount := firstReceived.Load()
	offsetsMu.Lock()
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	offsetsCopy := append([]uint64(nil), offsets...)
	offsetsMu.Unlock()

	if len(offsetsCopy) == 0 {
		res.Published = published
		res.Expected = published
		res.Received = firstCount
		res.Errors = errCount.Load()
		res.Pass = false
		res.Notes = "stream cursor phase collected no offsets"
		return res, nil
	}

	cursorOffset := offsetsCopy[len(offsetsCopy)/2]
	var expectedTail int64
	for _, off := range offsetsCopy {
		if off >= cursorOffset {
			expectedTail++
		}
	}

	readerCursor, err := connectAMQPClient(cfg.AMQPAddrs[0])
	if err != nil {
		return res, err
	}
	defer readerCursor.Close()

	var cursorReceived atomic.Int64
	cursorSeen := newDeduper()
	err = readerCursor.SubscribeToStream(&amqpclient.StreamConsumeOptions{
		QueueName:     streamName,
		ConsumerGroup: fmt.Sprintf("cursor-tail-%d", runID),
		Offset:        fmt.Sprintf("offset=%d", cursorOffset),
		AutoAck:       false,
	}, func(msg *amqpclient.QueueMessage) {
		msgID := extractMsgID(msg.Body)
		if cursorSeen.add(msgID) {
			cursorReceived.Add(1)
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, fmt.Errorf("subscribe stream cursor failed: %w", err)
	}

	_ = waitForAtLeast(&cursorReceived, expectedTail, cfg.DrainTimeout)
	_ = readerCursor.UnsubscribeFromStream(streamName)

	tailCount := cursorReceived.Load()
	res.Published = published
	res.Expected = published + expectedTail
	res.Received = firstCount + tailCount
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)

	firstOK := ratio(firstCount, published) >= cfg.QueueMinRatio
	tailOK := ratio(tailCount, expectedTail) >= cfg.QueueMinRatio
	res.Pass = firstOK && tailOK
	res.Notes = fmt.Sprintf("cursor_offset=%d first_phase=%d/%d tail_phase=%d/%d", cursorOffset, firstCount, published, tailCount, expectedTail)
	if !res.Pass {
		res.Notes = res.Notes + fmt.Sprintf("; cursor scenario below min %.4f", cfg.QueueMinRatio)
	}

	return res, nil
}

func runAMQP091RetentionPolicies(ctx context.Context, cfg runConfig) (scenarioResult, error) {
	res := scenarioResult{
		Scenario:     "amqp091-retention-policies",
		PayloadLabel: cfg.PayloadLabel,
		PayloadBytes: cfg.PayloadBytes,
		Subscribers:  1,
	}

	type workload struct {
		Publishers           int
		MessagesPerPublisher int
		MaxRetainedMessages  int
	}

	w := workload{
		Publishers:           12,
		MessagesPerPublisher: messagesByPayloadBytes(cfg.PayloadBytes, 120, 50, 16),
		MaxRetainedMessages:  200,
	}
	if cfg.Publishers > 0 {
		w.Publishers = cfg.Publishers
	}
	if cfg.MessagesPerPublisher > 0 {
		w.MessagesPerPublisher = cfg.MessagesPerPublisher
	}
	res.Publishers = w.Publishers

	runID := time.Now().UnixNano()
	streamName := fmt.Sprintf("perf-retention-policy-%d", runID)

	var errCount atomic.Int64
	admin, err := connectAMQPClient(cfg.AMQPAddrs[0])
	if err != nil {
		return res, err
	}
	defer admin.Close()

	_, err = admin.DeclareStreamQueue(&amqpclient.StreamQueueOptions{
		Name:              streamName,
		Durable:           true,
		MaxLengthMessages: int64(w.MaxRetainedMessages),
	})
	if err != nil {
		return res, fmt.Errorf("declare retention stream %s failed: %w", streamName, err)
	}

	var ordMu sync.Mutex
	publishedOrdinals := make([]int, 0, w.Publishers*w.MessagesPerPublisher)
	published := runAMQPStreamPublishers(ctx, amqpStreamPublishParams{
		Addrs:                cfg.AMQPAddrs,
		Scenario:             "amqp091-retention-policies",
		RunID:                runID,
		StreamName:           streamName,
		Publishers:           w.Publishers,
		MessagesPerPublisher: w.MessagesPerPublisher,
		PayloadSize:          cfg.PayloadBytes,
		PublishInterval:      cfg.PublishInterval,
		MsgIDFor: func(pubIdx, msgIdx int) string {
			return fmt.Sprintf("ret-%08d", pubIdx*w.MessagesPerPublisher+msgIdx)
		},
		OnPublished: func(msgID string) {
			ord, ok := parseRetentionOrdinal(msgID)
			if !ok {
				return
			}
			ordMu.Lock()
			publishedOrdinals = append(publishedOrdinals, ord)
			ordMu.Unlock()
		},
		ErrCount: &errCount,
	})

	ordMu.Lock()
	sort.Ints(publishedOrdinals)
	publishedSnapshot := append([]int(nil), publishedOrdinals...)
	ordMu.Unlock()

	expectedRetained := minInt(len(publishedSnapshot), w.MaxRetainedMessages)
	expectedTail := make(map[int]struct{}, expectedRetained)
	for i := len(publishedSnapshot) - expectedRetained; i < len(publishedSnapshot); i++ {
		if i >= 0 {
			expectedTail[publishedSnapshot[i]] = struct{}{}
		}
	}

	reader, err := connectAMQPClient(cfg.AMQPAddrs[0])
	if err != nil {
		return res, err
	}
	defer reader.Close()

	var receivedUnique atomic.Int64
	seen := newDeduper()
	var readMu sync.Mutex
	readOrdinals := make(map[int]struct{}, expectedRetained)

	err = reader.SubscribeToStream(&amqpclient.StreamConsumeOptions{
		QueueName:     streamName,
		ConsumerGroup: fmt.Sprintf("retention-read-%d", runID),
		Offset:        "first",
		AutoAck:       false,
	}, func(msg *amqpclient.QueueMessage) {
		msgID := extractMsgID(msg.Body)
		if seen.add(msgID) {
			receivedUnique.Add(1)
			if ord, ok := parseRetentionOrdinal(msgID); ok {
				readMu.Lock()
				readOrdinals[ord] = struct{}{}
				readMu.Unlock()
			}
		}
		if err := msg.Ack(); err != nil {
			errCount.Add(1)
		}
	})
	if err != nil {
		return res, fmt.Errorf("subscribe retention stream failed: %w", err)
	}

	waitForCounterIdle(&receivedUnique, 1200*time.Millisecond, minDuration(cfg.DrainTimeout, 12*time.Second))
	_ = reader.UnsubscribeFromStream(streamName)

	readMu.Lock()
	missingTail := 0
	for ord := range expectedTail {
		if _, ok := readOrdinals[ord]; !ok {
			missingTail++
		}
	}
	readMu.Unlock()

	res.Published = published
	res.Expected = int64(expectedRetained)
	res.Received = receivedUnique.Load()
	res.Errors = errCount.Load()
	res.DeliveryRatio = ratio(res.Received, res.Expected)
	res.Pass = missingTail == 0 && int(res.Received) <= expectedRetained && res.DeliveryRatio >= cfg.QueueMinRatio
	res.Notes = fmt.Sprintf("retention_limit=%d missing_tail=%d", expectedRetained, missingTail)
	if !res.Pass {
		res.Notes = res.Notes + fmt.Sprintf("; retention policy check failed (min ratio %.4f)", cfg.QueueMinRatio)
	}

	return res, nil
}

func connectMQTTSubscribers(addrs []string, scenario string, runID int64, count int, onMessage func(subID int, subName string, msg *mqttclient.Message)) ([]*mqttclient.Client, error) {
	clients := make([]*mqttclient.Client, 0, count)
	for i := 0; i < count; i++ {
		addr := addrs[i%len(addrs)]
		clientID := fmt.Sprintf("%s-sub-%d-%d", scenario, runID, i)
		sid := i
		sname := clientID
		client, err := connectMQTTClient(addr, clientID, func(msg *mqttclient.Message) {
			if onMessage != nil {
				onMessage(sid, sname, msg)
			}
		})
		if err != nil {
			disconnectMQTTClients(clients)
			return nil, fmt.Errorf("failed to connect MQTT subscriber %s to %s: %w", clientID, addr, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func connectMQTTQueueConsumers(addrs []string, scenario string, runID int64, count int, queueName, group string, handler func(consumerID int, msg *mqttclient.QueueMessage)) ([]*mqttclient.Client, error) {
	clients := make([]*mqttclient.Client, 0, count)
	for i := 0; i < count; i++ {
		addr := addrs[i%len(addrs)]
		clientID := fmt.Sprintf("%s-qc-%d-%d", scenario, runID, i)
		cid := i
		client, err := connectMQTTClient(addr, clientID, nil)
		if err != nil {
			disconnectMQTTClients(clients)
			return nil, fmt.Errorf("failed to connect MQTT queue consumer %s to %s: %w", clientID, addr, err)
		}
		if err := client.SubscribeToQueue(queueName, group, func(msg *mqttclient.QueueMessage) {
			handler(cid, msg)
		}); err != nil {
			_ = client.Disconnect()
			disconnectMQTTClients(clients)
			return nil, fmt.Errorf("failed to subscribe MQTT queue consumer %s on %s/%s: %w", clientID, queueName, group, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func connectAMQPTopicSubscribers(addrs []string, scenario string, runID int64, count int, topic string, handler func(subID int, msg *amqpclient.Message)) ([]*amqpclient.Client, error) {
	clients := make([]*amqpclient.Client, 0, count)
	for i := 0; i < count; i++ {
		addr := addrs[i%len(addrs)]
		cid := i
		client, err := connectAMQPClient(addr)
		if err != nil {
			closeAMQPClients(clients)
			return nil, fmt.Errorf("failed to connect AMQP subscriber %d to %s: %w", i, addr, err)
		}
		subOpts := &amqpclient.SubscribeOptions{Topic: topic, AutoAck: false}
		if err := client.SubscribeWithOptions(subOpts, func(msg *amqpclient.Message) {
			handler(cid, msg)
		}); err != nil {
			_ = client.Close()
			closeAMQPClients(clients)
			return nil, fmt.Errorf("failed AMQP subscribe on topic %q: %w", topic, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func connectAMQPQueueConsumers(addrs []string, scenario string, runID int64, count int, queueName, group string, handler func(consumerID int, msg *amqpclient.QueueMessage)) ([]*amqpclient.Client, error) {
	clients := make([]*amqpclient.Client, 0, count)
	for i := 0; i < count; i++ {
		addr := addrs[i%len(addrs)]
		cid := i
		client, err := connectAMQPClient(addr)
		if err != nil {
			closeAMQPClients(clients)
			return nil, fmt.Errorf("failed to connect AMQP queue consumer %d to %s: %w", i, addr, err)
		}
		if err := client.SubscribeToQueue(queueName, group, func(msg *amqpclient.QueueMessage) {
			handler(cid, msg)
		}); err != nil {
			_ = client.Close()
			closeAMQPClients(clients)
			return nil, fmt.Errorf("failed AMQP subscribe to queue %q/%q: %w", queueName, group, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

type mqttPublishParams struct {
	Addrs                []string
	Scenario             string
	RunID                int64
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	TopicFor             func(pubIdx, msgIdx int) string
	OnPublished          func(topic string)
	ErrCount             *atomic.Int64
}

func runMQTTPublishers(ctx context.Context, params mqttPublishParams) int64 {
	pubClients := make([]*mqttclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		addr := params.Addrs[i%len(params.Addrs)]
		clientID := fmt.Sprintf("%s-pub-%d-%d", params.Scenario, params.RunID, i)
		client, err := connectMQTTClient(addr, clientID, nil)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer disconnectMQTTClients(pubClients)

	var published atomic.Int64
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *mqttclient.Client) {
			defer wg.Done()
			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				topic := params.TopicFor(idx, msgIdx)
				msgID := fmt.Sprintf("%s-%d-%d", params.Scenario, idx, msgIdx)
				payload := makePayload(msgID, params.PayloadSize)
				if err := client.Publish(topic, payload, 1, false); err != nil {
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				published.Add(1)
				if params.OnPublished != nil {
					params.OnPublished(topic)
				}
				if params.PublishInterval > 0 {
					time.Sleep(params.PublishInterval)
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return published.Load()
}

type mqttQueuePublishParams struct {
	Addrs                []string
	Scenario             string
	RunID                int64
	QueueName            string
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	ErrCount             *atomic.Int64
}

func runMQTTQueuePublishers(ctx context.Context, params mqttQueuePublishParams) int64 {
	pubClients := make([]*mqttclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		addr := params.Addrs[i%len(params.Addrs)]
		clientID := fmt.Sprintf("%s-qpub-%d-%d", params.Scenario, params.RunID, i)
		client, err := connectMQTTClient(addr, clientID, nil)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer disconnectMQTTClients(pubClients)

	var published atomic.Int64
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *mqttclient.Client) {
			defer wg.Done()
			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				msgID := fmt.Sprintf("%s-q-%d-%d", params.Scenario, idx, msgIdx)
				payload := makePayload(msgID, params.PayloadSize)
				err := client.PublishToQueueWithOptions(&mqttclient.QueuePublishOptions{
					QueueName: params.QueueName,
					Payload:   payload,
					QoS:       1,
				})
				if err != nil {
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				published.Add(1)
				if params.PublishInterval > 0 {
					time.Sleep(params.PublishInterval)
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return published.Load()
}

type amqpTopicPublishParams struct {
	Addrs                []string
	Scenario             string
	RunID                int64
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	TopicFor             func(pubIdx, msgIdx int) string
	ErrCount             *atomic.Int64
}

func runAMQPTopicPublishers(ctx context.Context, params amqpTopicPublishParams) int64 {
	pubClients := make([]*amqpclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		addr := params.Addrs[i%len(params.Addrs)]
		client, err := connectAMQPClient(addr)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer closeAMQPClients(pubClients)

	var published atomic.Int64
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *amqpclient.Client) {
			defer wg.Done()
			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				topic := params.TopicFor(idx, msgIdx)
				msgID := fmt.Sprintf("%s-at-%d-%d", params.Scenario, idx, msgIdx)
				payload := makePayload(msgID, params.PayloadSize)
				if err := client.Publish(topic, payload); err != nil {
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				published.Add(1)
				if params.PublishInterval > 0 {
					time.Sleep(params.PublishInterval)
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return published.Load()
}

type amqpQueuePublishParams struct {
	Addrs                []string
	Scenario             string
	RunID                int64
	QueueName            string
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	ErrCount             *atomic.Int64
}

func runAMQPQueuePublishers(ctx context.Context, params amqpQueuePublishParams) int64 {
	pubClients := make([]*amqpclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		addr := params.Addrs[i%len(params.Addrs)]
		client, err := connectAMQPClient(addr)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer closeAMQPClients(pubClients)

	var published atomic.Int64
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *amqpclient.Client) {
			defer wg.Done()
			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				msgID := fmt.Sprintf("%s-aq-%d-%d", params.Scenario, idx, msgIdx)
				payload := makePayload(msgID, params.PayloadSize)
				err := client.PublishToQueueWithOptions(&amqpclient.QueuePublishOptions{
					QueueName: params.QueueName,
					Payload:   payload,
				})
				if err != nil {
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				published.Add(1)
				if params.PublishInterval > 0 {
					time.Sleep(params.PublishInterval)
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return published.Load()
}

type amqpStreamPublishParams struct {
	Addrs                []string
	Scenario             string
	RunID                int64
	StreamName           string
	Publishers           int
	MessagesPerPublisher int
	PayloadSize          int
	PublishInterval      time.Duration
	MsgIDFor             func(pubIdx, msgIdx int) string
	OnPublished          func(msgID string)
	ErrCount             *atomic.Int64
}

func runAMQPStreamPublishers(ctx context.Context, params amqpStreamPublishParams) int64 {
	pubClients := make([]*amqpclient.Client, 0, params.Publishers)
	for i := 0; i < params.Publishers; i++ {
		addr := params.Addrs[i%len(params.Addrs)]
		client, err := connectAMQPClient(addr)
		if err != nil {
			if params.ErrCount != nil {
				params.ErrCount.Add(1)
			}
			continue
		}
		pubClients = append(pubClients, client)
	}
	defer closeAMQPClients(pubClients)

	var published atomic.Int64
	var wg sync.WaitGroup
	for pubIdx, c := range pubClients {
		wg.Add(1)
		go func(idx int, client *amqpclient.Client) {
			defer wg.Done()
			for msgIdx := 0; msgIdx < params.MessagesPerPublisher; msgIdx++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				msgID := fmt.Sprintf("%s-as-%d-%d", params.Scenario, idx, msgIdx)
				if params.MsgIDFor != nil {
					msgID = params.MsgIDFor(idx, msgIdx)
				}
				payload := makePayload(msgID, params.PayloadSize)
				if err := client.PublishToStream(params.StreamName, payload, nil); err != nil {
					if params.ErrCount != nil {
						params.ErrCount.Add(1)
					}
					continue
				}
				published.Add(1)
				if params.OnPublished != nil {
					params.OnPublished(msgID)
				}
				if params.PublishInterval > 0 {
					time.Sleep(params.PublishInterval)
				}
			}
		}(pubIdx, c)
	}
	wg.Wait()
	return published.Load()
}

func connectMQTTClient(addr, clientID string, onMessage func(msg *mqttclient.Message)) (*mqttclient.Client, error) {
	return connectMQTTClientWithCustomizer(addr, clientID, onMessage, nil)
}

func connectMQTTClientWithCustomizer(addr, clientID string, onMessage func(msg *mqttclient.Message), customize func(opts *mqttclient.Options)) (*mqttclient.Client, error) {
	opts := mqttclient.NewOptions().
		SetServers(addr).
		SetClientID(clientID).
		SetProtocolVersion(5).
		SetCleanSession(true).
		SetKeepAlive(20 * time.Second).
		SetConnectTimeout(8 * time.Second).
		SetAckTimeout(12 * time.Second).
		SetAutoReconnect(true).
		SetMessageChanSize(4096)

	if onMessage != nil {
		opts.SetOnMessageV2(onMessage)
	}
	if customize != nil {
		customize(opts)
	}

	c, err := mqttclient.New(opts)
	if err != nil {
		return nil, err
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func connectAMQPClient(addr string) (*amqpclient.Client, error) {
	opts := amqpclient.NewOptions().
		SetAddress(addr).
		SetCredentials("guest", "guest").
		SetPrefetch(512, 0).
		SetAutoReconnect(true)

	c, err := amqpclient.New(opts)
	if err != nil {
		return nil, err
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func disconnectMQTTClients(clients []*mqttclient.Client) {
	for _, c := range clients {
		if c == nil {
			continue
		}
		_ = c.Disconnect()
	}
}

func closeAMQPClients(clients []*amqpclient.Client) {
	for _, c := range clients {
		if c == nil {
			continue
		}
		_ = c.Close()
	}
}

func messagesByPayloadBytes(payloadBytes, small, medium, large int) int {
	switch {
	case payloadBytes <= 512:
		return small
	case payloadBytes <= 8192:
		return medium
	default:
		return large
	}
}

func applyTopicOverrides(workload *topicWorkload, cfg runConfig) {
	if cfg.Publishers > 0 {
		workload.Publishers = cfg.Publishers
	}
	if cfg.Subscribers > 0 {
		workload.Subscribers = cfg.Subscribers
	}
	if cfg.MessagesPerPublisher > 0 {
		workload.MessagesPerPublisher = cfg.MessagesPerPublisher
	}
}

func scenarioDescription(scenario string) string {
	switch scenario {
	case "mqtt-fanin":
		return "Many MQTT publishers send to one topic consumed by many subscribers."
	case "mqtt-fanout":
		return "MQTT fanout with broad subscriber population."
	case "amqp-fanin":
		return "Many AMQP 0.9.1 publishers send to one topic consumed by many subscribers."
	case "amqp-fanout":
		return "AMQP 0.9.1 fanout with broad subscriber population."
	case "mqtt-substorm":
		return "MQTT subscribe/unsubscribe churn with wildcard filters under concurrent publishing."
	case "queue-fanin":
		return "Many publishers feeding one queue consumer group."
	case "queue-fanout":
		return "One queue fanout to multiple consumer groups."
	case "mqtt-consumer-groups":
		return "MQTT queue fanout with multiple consumer groups and concurrent workers."
	case "mqtt-shared-subscriptions":
		return "MQTT shared subscriptions ($share/group/topic) with per-group load balancing."
	case "mqtt-last-will":
		return "MQTT last-will delivery after abrupt client disconnect."
	case "amqp091-consumer-groups":
		return "AMQP 0.9.1 queue fanout to multiple consumer groups."
	case "amqp091-stream-cursor":
		return "AMQP 0.9.1 stream read with first-pass consume and cursor-based tail replay."
	case "amqp091-retention-policies":
		return "AMQP 0.9.1 stream retention policy verification (max retained messages)."
	case "bridge-queue-mqtt-amqp":
		return "MQTT queue producers, AMQP queue consumers."
	case "bridge-queue-amqp-mqtt":
		return "AMQP queue producers, MQTT queue consumers."
	default:
		return "Custom scenario run."
	}
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

func minDuration(a, b time.Duration) time.Duration {
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

func waitForCounterIdle(counter *atomic.Int64, idleWindow, timeout time.Duration) int64 {
	deadline := time.Now().Add(timeout)
	lastVal := counter.Load()
	lastChange := time.Now()

	for time.Now().Before(deadline) {
		current := counter.Load()
		if current != lastVal {
			lastVal = current
			lastChange = time.Now()
		}
		if time.Since(lastChange) >= idleWindow {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	return counter.Load()
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

func parseRetentionOrdinal(msgID string) (int, bool) {
	if !strings.HasPrefix(msgID, "ret-") {
		return 0, false
	}
	v := strings.TrimPrefix(msgID, "ret-")
	ord, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return ord, true
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
	if _, err := f.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	return nil
}

func stormFilter(base string, stormTopics []string, idx int) string {
	switch idx % 5 {
	case 0:
		return stormTopics[idx%len(stormTopics)]
	case 1:
		return base + "/device/+/metric/temp"
	case 2:
		return base + "/device/+/metric/humidity"
	case 3:
		return base + "/device/+/metric/+"
	default:
		return base + "/#"
	}
}

func makeStormTopics(base string, count int) []string {
	metrics := []string{"temp", "humidity", "pressure"}
	topicsList := make([]string, 0, count)
	for i := 0; i < count; i++ {
		topicsList = append(topicsList, fmt.Sprintf("%s/device/%03d/metric/%s", base, i%64, metrics[i%len(metrics)]))
	}
	sort.Strings(topicsList)
	return topicsList
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
