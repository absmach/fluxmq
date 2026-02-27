// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	amqpclient "github.com/absmach/fluxmq/client/amqp"
	mqttclient "github.com/absmach/fluxmq/client/mqtt"
)

// Config configures the unified client transports.
type Config struct {
	MQTT            *mqttclient.Options
	AMQP            *amqpclient.Options
	DefaultProtocol Protocol
}

// NewConfig creates Config with sensible defaults.
func NewConfig() *Config {
	return &Config{
		DefaultProtocol: ProtocolMQTT,
	}
}

// SetMQTT sets MQTT transport options.
func (c *Config) SetMQTT(opts *mqttclient.Options) *Config {
	c.MQTT = opts
	return c
}

// SetAMQP sets AMQP transport options.
func (c *Config) SetAMQP(opts *amqpclient.Options) *Config {
	c.AMQP = opts
	return c
}

// SetDefaultProtocol sets the protocol used when operation options do not specify one.
func (c *Config) SetDefaultProtocol(protocol Protocol) *Config {
	c.DefaultProtocol = protocol
	return c
}
