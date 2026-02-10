// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "strings"

// RouteKind indicates how a publish or subscribe should be routed.
type RouteKind int

const (
	// RoutePubSub routes through the topic-based pub/sub path.
	RoutePubSub RouteKind = iota
	// RouteQueue routes through the queue manager.
	RouteQueue
	// RouteQueueAck routes a queue acknowledgment (ack/nack/reject).
	RouteQueueAck
	// RouteQueueCommit routes a stream offset commit.
	RouteQueueCommit
)

// AckKind identifies the type of queue acknowledgment.
type AckKind int

const (
	AckAccept AckKind = iota
	AckNack
	AckReject
)

// RouteResult holds the resolved routing decision for a topic or address.
type RouteResult struct {
	Kind RouteKind

	// QueueName is the resolved queue name (populated for RouteQueue, RouteQueueAck, RouteQueueCommit).
	QueueName string

	// Pattern is the topic pattern after the queue name (e.g., "images/#").
	Pattern string

	// PublishTopic is the full topic to use when publishing to the queue manager
	// (e.g., "$queue/tasks/images"). For RouteQueueAck, this is the base queue topic
	// without the /$ack suffix.
	PublishTopic string

	// AckKind identifies ack/nack/reject for RouteQueueAck results.
	AckKind AckKind
}

const queuePrefix = "$queue/"

// RoutingResolver encapsulates all routing decisions so protocol handlers
// don't need to know about the $queue/ prefix convention.
type RoutingResolver struct{}

// NewRoutingResolver creates a new RoutingResolver.
func NewRoutingResolver() *RoutingResolver {
	return &RoutingResolver{}
}

// Resolve determines the routing for a given topic or address.
func (r *RoutingResolver) Resolve(topic string) RouteResult {
	if !strings.HasPrefix(topic, queuePrefix) {
		return RouteResult{Kind: RoutePubSub}
	}

	// Check for ack/nack/reject suffixes first
	if ackKind, baseTopic, ok := parseAckSuffix(topic); ok {
		queueName, pattern := ParseQueueFilter(baseTopic)
		return RouteResult{
			Kind:         RouteQueueAck,
			QueueName:    queueName,
			Pattern:      pattern,
			PublishTopic: baseTopic,
			AckKind:      ackKind,
		}
	}

	// Check for stream commit suffix
	if strings.HasSuffix(topic, "/$commit") {
		inner := strings.TrimSuffix(strings.TrimPrefix(topic, queuePrefix), "/$commit")
		return RouteResult{
			Kind:         RouteQueueCommit,
			QueueName:    inner,
			PublishTopic: topic,
		}
	}

	queueName, pattern := ParseQueueFilter(topic)
	return RouteResult{
		Kind:         RouteQueue,
		QueueName:    queueName,
		Pattern:      pattern,
		PublishTopic: topic,
	}
}

// IsQueueTopic returns true if the topic targets the queue system.
func (r *RoutingResolver) IsQueueTopic(topic string) bool {
	return strings.HasPrefix(topic, queuePrefix)
}

// QueueTopic constructs a full queue topic from a queue name and optional sub-path.
func (r *RoutingResolver) QueueTopic(queueName string, parts ...string) string {
	topic := queuePrefix + queueName
	for _, p := range parts {
		if p != "" {
			topic = topic + "/" + p
		}
	}
	return topic
}

// ParseQueueFilter parses a $queue/ prefixed filter into queue name and pattern.
// Examples:
//   - "$queue/tasks" -> queueName="tasks", pattern=""
//   - "$queue/tasks/images" -> queueName="tasks", pattern="images"
//   - "$queue/tasks/images/#" -> queueName="tasks", pattern="images/#"
func ParseQueueFilter(filter string) (queueName, pattern string) {
	if !strings.HasPrefix(filter, queuePrefix) {
		return "", ""
	}

	rest := filter[len(queuePrefix):]
	if rest == "" {
		return "", ""
	}

	if name, pat, ok := strings.Cut(rest, "/"); ok {
		return name, pat
	}
	return rest, ""
}

func parseAckSuffix(topic string) (AckKind, string, bool) {
	if base, ok := strings.CutSuffix(topic, "/$ack"); ok {
		return AckAccept, base, true
	}
	if base, ok := strings.CutSuffix(topic, "/$nack"); ok {
		return AckNack, base, true
	}
	if base, ok := strings.CutSuffix(topic, "/$reject"); ok {
		return AckReject, base, true
	}
	return 0, "", false
}
