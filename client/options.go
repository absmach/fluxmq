// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "strconv"

// Protocol identifies the messaging transport.
type Protocol string

const (
	// ProtocolMQTT routes operations over MQTT.
	ProtocolMQTT Protocol = "mqtt"
	// ProtocolAMQP routes operations over AMQP 0.9.1.
	ProtocolAMQP Protocol = "amqp"
)

const (
	propMQTTQoS      = "mqtt.qos"
	propMQTTRetain   = "mqtt.retain"
	propMQTTDup      = "mqtt.dup"
	propMQTTUserPref = "mqtt.user."

	propAMQPExchange    = "amqp.exchange"
	propAMQPRoutingKey  = "amqp.routing_key"
	propAMQPMandatory   = "amqp.mandatory"
	propAMQPImmediate   = "amqp.immediate"
	propAMQPHeadersPref = "amqp.headers."
)

// Option applies to publish and/or subscribe operations.
type Option interface {
	applyPublish(*PublishOptions)
	applySubscribe(*SubscribeOptions)
}

type publishOption func(*PublishOptions)

func (o publishOption) applyPublish(opts *PublishOptions) { o(opts) }
func (o publishOption) applySubscribe(*SubscribeOptions)  {}

type subscribeOption func(*SubscribeOptions)

func (o subscribeOption) applyPublish(*PublishOptions)          {}
func (o subscribeOption) applySubscribe(opts *SubscribeOptions) { o(opts) }

type dualOption func(*PublishOptions, *SubscribeOptions)

func (o dualOption) applyPublish(opts *PublishOptions) {
	o(opts, nil)
}
func (o dualOption) applySubscribe(opts *SubscribeOptions) {
	o(nil, opts)
}

// PublishOptions configure publishing behavior.
type PublishOptions struct {
	Protocol   Protocol
	QoS        *byte
	Retain     *bool
	Exchange   string
	RoutingKey string
	Mandatory  *bool
	Immediate  *bool

	Properties map[string]string
}

// SubscribeOptions configure subscription behavior.
type SubscribeOptions struct {
	Protocol Protocol
	QoS      *byte
	AutoAck  *bool
}

// WithProtocol sets protocol routing for publish/subscribe operations.
func WithProtocol(protocol Protocol) Option {
	return dualOption(func(p *PublishOptions, s *SubscribeOptions) {
		if p != nil {
			p.Protocol = protocol
		}
		if s != nil {
			s.Protocol = protocol
		}
	})
}

// WithQoS sets the MQTT QoS for publish/subscribe.
func WithQoS(qos byte) Option {
	return dualOption(func(p *PublishOptions, s *SubscribeOptions) {
		if p != nil {
			p.QoS = &qos
		}
		if s != nil {
			s.QoS = &qos
		}
	})
}

// WithRetain sets the MQTT retain flag (publish only).
func WithRetain(retain bool) Option {
	return publishOption(func(p *PublishOptions) {
		p.Retain = &retain
	})
}

// WithExchange sets the AMQP exchange (publish only).
func WithExchange(exchange string) Option {
	return publishOption(func(p *PublishOptions) {
		p.Exchange = exchange
	})
}

// WithRoutingKey sets the AMQP routing key (publish only).
func WithRoutingKey(key string) Option {
	return publishOption(func(p *PublishOptions) {
		p.RoutingKey = key
	})
}

// WithMandatory sets the AMQP mandatory flag (publish only).
func WithMandatory(mandatory bool) Option {
	return publishOption(func(p *PublishOptions) {
		p.Mandatory = &mandatory
	})
}

// WithImmediate sets the AMQP immediate flag (publish only).
func WithImmediate(immediate bool) Option {
	return publishOption(func(p *PublishOptions) {
		p.Immediate = &immediate
	})
}

// WithProperties sets unified properties.
func WithProperties(props map[string]string) Option {
	return publishOption(func(p *PublishOptions) {
		if len(props) == 0 {
			return
		}
		if p.Properties == nil {
			p.Properties = make(map[string]string, len(props))
		}
		for k, v := range props {
			p.Properties[k] = v
		}
	})
}

func buildPublishOptions(opts []Option) PublishOptions {
	var po PublishOptions
	for _, opt := range opts {
		opt.applyPublish(&po)
	}
	applyPropertyPrefixes(&po)
	return po
}

func buildSubscribeOptions(opts []Option) SubscribeOptions {
	var so SubscribeOptions
	for _, opt := range opts {
		opt.applySubscribe(&so)
	}
	applySubscribePropertyPrefixes(&so, nil)
	return so
}

// WithAutoAck controls AMQP auto-ack for subscriptions (publish/subscribe only).
// It has no effect for MQTT subscriptions.
func WithAutoAck(autoAck bool) Option {
	return subscribeOption(func(s *SubscribeOptions) {
		s.AutoAck = &autoAck
	})
}

func applyPropertyPrefixes(po *PublishOptions) {
	if po == nil || len(po.Properties) == 0 {
		return
	}

	if po.QoS == nil {
		if v, ok := po.Properties[propMQTTQoS]; ok {
			if qos, err := strconv.Atoi(v); err == nil && qos >= 0 && qos <= 2 {
				q := byte(qos)
				po.QoS = &q
			}
		}
	}
	if po.Retain == nil {
		if v, ok := po.Properties[propMQTTRetain]; ok {
			if retain, err := strconv.ParseBool(v); err == nil {
				po.Retain = &retain
			}
		}
	}

	if po.Exchange == "" {
		if v, ok := po.Properties[propAMQPExchange]; ok {
			po.Exchange = v
		}
	}
	if po.RoutingKey == "" {
		if v, ok := po.Properties[propAMQPRoutingKey]; ok {
			po.RoutingKey = v
		}
	}
	if po.Mandatory == nil {
		if v, ok := po.Properties[propAMQPMandatory]; ok {
			if mandatory, err := strconv.ParseBool(v); err == nil {
				po.Mandatory = &mandatory
			}
		}
	}
	if po.Immediate == nil {
		if v, ok := po.Properties[propAMQPImmediate]; ok {
			if immediate, err := strconv.ParseBool(v); err == nil {
				po.Immediate = &immediate
			}
		}
	}
}

func applySubscribePropertyPrefixes(so *SubscribeOptions, props map[string]string) {
	if so == nil || len(props) == 0 {
		return
	}
	if so.QoS == nil {
		if v, ok := props[propMQTTQoS]; ok {
			if qos, err := strconv.Atoi(v); err == nil && qos >= 0 && qos <= 2 {
				q := byte(qos)
				so.QoS = &q
			}
		}
	}
}
