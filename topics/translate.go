// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import "strings"

// AMQPFilterToMQTT translates an AMQP topic filter to MQTT canonical form.
//
//	'.' -> '/'
//	'*' -> '+'
//	'#' -> '#'
func AMQPFilterToMQTT(filter string) string {
	if filter == "" {
		return ""
	}
	if !strings.ContainsAny(filter, ".*") {
		return filter
	}

	var b strings.Builder
	b.Grow(len(filter))
	for i := 0; i < len(filter); i++ {
		switch filter[i] {
		case '.':
			b.WriteByte('/')
		case '*':
			b.WriteByte('+')
		default:
			b.WriteByte(filter[i])
		}
	}
	return b.String()
}

// AMQPTopicToMQTT translates an AMQP routing key to MQTT topic canonical form.
//
//	'.' -> '/'
func AMQPTopicToMQTT(topic string) string {
	if topic == "" {
		return ""
	}
	if !strings.Contains(topic, ".") {
		return topic
	}
	return strings.ReplaceAll(topic, ".", "/")
}

// MQTTTopicToAMQP translates an MQTT topic to AMQP routing-key form.
//
//	'/' -> '.'
func MQTTTopicToAMQP(topic string) string {
	if topic == "" {
		return ""
	}
	if !strings.Contains(topic, "/") {
		return topic
	}
	return strings.ReplaceAll(topic, "/", ".")
}
