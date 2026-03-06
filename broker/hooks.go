// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "context"

// EventHook receives lifecycle and messaging events from the broker.
// Implementations should be non-blocking; slow hooks degrade broker throughput.
type EventHook interface {
	OnConnect(ctx context.Context, clientID, username, protocol string) error
	OnDisconnect(ctx context.Context, clientID, reason string) error
	OnSubscribe(ctx context.Context, clientID, topic string, qos byte) error
	OnUnsubscribe(ctx context.Context, clientID, topic string) error
	OnPublish(ctx context.Context, clientID, topic string, qos byte, payload []byte) error
	Close() error
}

// TopicRewriter rewrites topics before routing.
// This enables name-to-ID resolution, prefix injection, or other transformations.
type TopicRewriter interface {
	RewritePublish(ctx context.Context, clientID, topic string) (string, error)
	RewriteSubscribe(ctx context.Context, clientID, topic string) (string, error)
}

// ChainedEventHook composes multiple EventHook implementations.
// Hooks are called in order; first error short-circuits.
type ChainedEventHook struct {
	hooks []EventHook
}

func NewChainedEventHook(hooks ...EventHook) *ChainedEventHook {
	return &ChainedEventHook{hooks: hooks}
}

func (c *ChainedEventHook) OnConnect(ctx context.Context, clientID, username, protocol string) error {
	for _, h := range c.hooks {
		if err := h.OnConnect(ctx, clientID, username, protocol); err != nil {
			return err
		}
	}
	return nil
}

func (c *ChainedEventHook) OnDisconnect(ctx context.Context, clientID, reason string) error {
	for _, h := range c.hooks {
		if err := h.OnDisconnect(ctx, clientID, reason); err != nil {
			return err
		}
	}
	return nil
}

func (c *ChainedEventHook) OnSubscribe(ctx context.Context, clientID, topic string, qos byte) error {
	for _, h := range c.hooks {
		if err := h.OnSubscribe(ctx, clientID, topic, qos); err != nil {
			return err
		}
	}
	return nil
}

func (c *ChainedEventHook) OnUnsubscribe(ctx context.Context, clientID, topic string) error {
	for _, h := range c.hooks {
		if err := h.OnUnsubscribe(ctx, clientID, topic); err != nil {
			return err
		}
	}
	return nil
}

func (c *ChainedEventHook) OnPublish(ctx context.Context, clientID, topic string, qos byte, payload []byte) error {
	for _, h := range c.hooks {
		if err := h.OnPublish(ctx, clientID, topic, qos, payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *ChainedEventHook) Close() error {
	for _, h := range c.hooks {
		h.Close()
	}
	return nil
}
