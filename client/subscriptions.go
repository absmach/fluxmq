// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "sync"

type subscriptionRecord struct {
	topic string
	qos   byte
	opt   *SubscribeOption
}

type subscriptionRegistry struct {
	mu   sync.RWMutex
	subs map[string]subscriptionRecord
}

func newSubscriptionRegistry() *subscriptionRegistry {
	return &subscriptionRegistry{
		subs: make(map[string]subscriptionRecord),
	}
}

func (r *subscriptionRegistry) setBasic(topic string, qos byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs[topic] = subscriptionRecord{
		topic: topic,
		qos:   qos,
	}
}

func (r *subscriptionRegistry) setOption(opt *SubscribeOption) {
	if opt == nil {
		return
	}

	copied := *opt
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs[opt.Topic] = subscriptionRecord{
		topic: opt.Topic,
		qos:   opt.QoS,
		opt:   &copied,
	}
}

func (r *subscriptionRegistry) remove(topics ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, topic := range topics {
		delete(r.subs, topic)
	}
}

func (r *subscriptionRegistry) snapshot() []subscriptionRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	records := make([]subscriptionRecord, 0, len(r.subs))
	for _, rec := range r.subs {
		copied := rec
		if rec.opt != nil {
			optCopy := *rec.opt
			copied.opt = &optCopy
		}
		records = append(records, copied)
	}

	return records
}
