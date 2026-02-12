// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/storage"
)

func buildSubscriptionBenchData(numClients, filtersPerClient int) (map[string]*storage.Subscription, *router.TrieRouter, []string) {
	subCache := make(map[string]*storage.Subscription, numClients*filtersPerClient)
	trie := router.NewRouter()
	topics := make([]string, 0, numClients)

	for c := 0; c < numClients; c++ {
		clientID := fmt.Sprintf("client-%d", c)
		topic := fmt.Sprintf("sensor/room-%d/temp", c)
		topics = append(topics, topic)

		for f := 0; f < filtersPerClient; f++ {
			filter := fmt.Sprintf("sensor/room-%d/temp", (c+f)%numClients)
			if f%10 == 0 {
				filter = "sensor/+/temp"
			}
			if f%25 == 0 {
				filter = "sensor/#"
			}

			sub := &storage.Subscription{
				ClientID: clientID,
				Filter:   filter,
				QoS:      1,
				Options:  storage.SubscribeOptions{},
			}
			key := clientID + "|" + filter + fmt.Sprintf("|%d", f)
			subCache[key] = sub
			_ = trie.Subscribe(sub.ClientID, sub.Filter, sub.QoS, sub.Options)
		}
	}

	return subCache, trie, topics
}

func legacyLinearSubscriberMatch(subCache map[string]*storage.Subscription, topic string) []*storage.Subscription {
	matched := make([]*storage.Subscription, 0, 16)
	for _, sub := range subCache {
		if topicMatchesFilter(topic, sub.Filter) {
			matched = append(matched, sub)
		}
	}
	return matched
}

func BenchmarkSubscribersLookup_LinearScan_10k(b *testing.B) {
	subCache, _, topics := buildSubscriptionBenchData(2000, 5) // 10k subscriptions
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := topics[i%len(topics)]
		_ = legacyLinearSubscriberMatch(subCache, topic)
	}
}

func BenchmarkSubscribersLookup_Trie_10k(b *testing.B) {
	_, trie, topics := buildSubscriptionBenchData(2000, 5) // 10k subscriptions
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := topics[i%len(topics)]
		_, _ = trie.Match(topic)
	}
}

func buildQueueConsumerBenchCluster(queues, groupsPerQueue, consumersPerGroup int) *EtcdCluster {
	c := &EtcdCluster{
		queueConsumersAll:     make(map[string]*QueueConsumerInfo),
		queueConsumersByQueue: make(map[string]map[string]*QueueConsumerInfo),
		queueConsumersByGroup: make(map[string]map[string]map[string]*QueueConsumerInfo),
	}

	for q := 0; q < queues; q++ {
		queueName := fmt.Sprintf("queue-%d", q)
		for g := 0; g < groupsPerQueue; g++ {
			groupID := fmt.Sprintf("group-%d", g)
			for n := 0; n < consumersPerGroup; n++ {
				consumerID := fmt.Sprintf("consumer-%d", n)
				c.upsertQueueConsumerCache(&QueueConsumerInfo{
					QueueName:   queueName,
					GroupID:     groupID,
					ConsumerID:  consumerID,
					ClientID:    fmt.Sprintf("%s-%s-%s", queueName, groupID, consumerID),
					ProxyNodeID: fmt.Sprintf("node-%d", (q+g+n)%3),
				})
			}
		}
	}

	return c
}

func legacyScanQueueConsumersByQueue(all map[string]*QueueConsumerInfo, queueName string) []*QueueConsumerInfo {
	out := make([]*QueueConsumerInfo, 0, 32)
	for _, info := range all {
		if info.QueueName == queueName {
			out = append(out, info)
		}
	}
	return out
}

func legacyScanQueueConsumersByQueueClone(all map[string]*QueueConsumerInfo, queueName string) []*QueueConsumerInfo {
	out := make([]*QueueConsumerInfo, 0, 32)
	for _, info := range all {
		if info.QueueName == queueName {
			copy := *info
			out = append(out, &copy)
		}
	}
	return out
}

func BenchmarkQueueConsumersLookup_ScanAll_50k(b *testing.B) {
	c := buildQueueConsumerBenchCluster(200, 10, 25) // 50k consumers
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queueName := fmt.Sprintf("queue-%d", i%200)
		_ = legacyScanQueueConsumersByQueue(c.queueConsumersAll, queueName)
	}
}

func BenchmarkQueueConsumersLookup_Indexed_50k(b *testing.B) {
	c := buildQueueConsumerBenchCluster(200, 10, 25) // 50k consumers
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queueName := fmt.Sprintf("queue-%d", i%200)
		_, _ = c.ListQueueConsumers(ctx, queueName)
	}
}

func BenchmarkQueueConsumersLookup_ScanAllClone_50k(b *testing.B) {
	c := buildQueueConsumerBenchCluster(200, 10, 25) // 50k consumers
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queueName := fmt.Sprintf("queue-%d", i%200)
		_ = legacyScanQueueConsumersByQueueClone(c.queueConsumersAll, queueName)
	}
}
