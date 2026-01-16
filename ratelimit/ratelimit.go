// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// IPRateLimiter manages rate limiting for IP addresses (connection layer).
// Used to limit connection attempts per IP to prevent DoS attacks.
type IPRateLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*ipEntry
	rate     rate.Limit
	burst    int
	cleanup  time.Duration
	stopCh   chan struct{}
}

type ipEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewIPRateLimiter creates a new IP-based rate limiter.
// rate is connections per second, burst is the burst allowance.
func NewIPRateLimiter(r float64, burst int, cleanupInterval time.Duration) *IPRateLimiter {
	l := &IPRateLimiter{
		limiters: make(map[string]*ipEntry),
		rate:     rate.Limit(r),
		burst:    burst,
		cleanup:  cleanupInterval,
		stopCh:   make(chan struct{}),
	}
	go l.cleanupLoop()
	return l
}

// Allow checks if a connection from the given IP address is allowed.
// Returns true if the connection is allowed, false if rate limited.
func (l *IPRateLimiter) Allow(addr net.Addr) bool {
	ip := extractIP(addr)
	if ip == "" {
		return true // Allow if we can't extract IP
	}

	l.mu.Lock()
	entry, exists := l.limiters[ip]
	if !exists {
		entry = &ipEntry{
			limiter:  rate.NewLimiter(l.rate, l.burst),
			lastSeen: time.Now(),
		}
		l.limiters[ip] = entry
	} else {
		entry.lastSeen = time.Now()
	}
	limiter := entry.limiter
	l.mu.Unlock()

	return limiter.Allow()
}

// cleanupLoop periodically removes stale entries.
func (l *IPRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(l.cleanup)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.cleanup_stale()
		case <-l.stopCh:
			return
		}
	}
}

func (l *IPRateLimiter) cleanup_stale() {
	l.mu.Lock()
	defer l.mu.Unlock()

	threshold := time.Now().Add(-l.cleanup * 2)
	for ip, entry := range l.limiters {
		if entry.lastSeen.Before(threshold) {
			delete(l.limiters, ip)
		}
	}
}

// Stop stops the cleanup goroutine.
func (l *IPRateLimiter) Stop() {
	close(l.stopCh)
}

// ClientRateLimiter manages rate limiting for individual MQTT clients.
// Used to limit message publishing and subscription rates per client.
type ClientRateLimiter struct {
	mu              sync.RWMutex
	messageLimiters map[string]*rate.Limiter
	subLimiters     map[string]*rate.Limiter
	messageRate     rate.Limit
	messageBurst    int
	subRate         rate.Limit
	subBurst        int
}

// NewClientRateLimiter creates a new client-based rate limiter.
func NewClientRateLimiter(messageRate float64, messageBurst int, subRate float64, subBurst int) *ClientRateLimiter {
	return &ClientRateLimiter{
		messageLimiters: make(map[string]*rate.Limiter),
		subLimiters:     make(map[string]*rate.Limiter),
		messageRate:     rate.Limit(messageRate),
		messageBurst:    messageBurst,
		subRate:         rate.Limit(subRate),
		subBurst:        subBurst,
	}
}

// AllowPublish checks if a publish from the given client is allowed.
// Returns true if allowed, false if rate limited.
func (l *ClientRateLimiter) AllowPublish(clientID string) bool {
	l.mu.Lock()
	limiter, exists := l.messageLimiters[clientID]
	if !exists {
		limiter = rate.NewLimiter(l.messageRate, l.messageBurst)
		l.messageLimiters[clientID] = limiter
	}
	l.mu.Unlock()

	return limiter.Allow()
}

// AllowSubscribe checks if a subscription from the given client is allowed.
// Returns true if allowed, false if rate limited.
func (l *ClientRateLimiter) AllowSubscribe(clientID string) bool {
	l.mu.Lock()
	limiter, exists := l.subLimiters[clientID]
	if !exists {
		limiter = rate.NewLimiter(l.subRate, l.subBurst)
		l.subLimiters[clientID] = limiter
	}
	l.mu.Unlock()

	return limiter.Allow()
}

// RemoveClient removes rate limiters for a disconnected client.
func (l *ClientRateLimiter) RemoveClient(clientID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.messageLimiters, clientID)
	delete(l.subLimiters, clientID)
}

// extractIP extracts the IP address from a net.Addr.
func extractIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}

	switch a := addr.(type) {
	case *net.TCPAddr:
		return a.IP.String()
	case *net.UDPAddr:
		return a.IP.String()
	default:
		// Try to parse as host:port format
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return addr.String()
		}
		return host
	}
}

// RateLimitResult represents the result of a rate limit check.
type RateLimitResult struct {
	Allowed bool
	Reason  string
}

// Config holds rate limiting configuration.
type Config struct {
	Enabled bool `yaml:"enabled"`

	Connection ConnectionConfig `yaml:"connection"`
	Message    MessageConfig    `yaml:"message"`
	Subscribe  SubscribeConfig  `yaml:"subscribe"`
}

// ConnectionConfig holds per-IP connection rate limiting settings.
type ConnectionConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Rate            float64       `yaml:"rate"`             // connections per second per IP
	Burst           int           `yaml:"burst"`            // burst allowance
	CleanupInterval time.Duration `yaml:"cleanup_interval"` // cleanup interval for stale entries
}

// MessageConfig holds per-client message rate limiting settings.
type MessageConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // messages per second per client
	Burst   int     `yaml:"burst"` // burst allowance
}

// SubscribeConfig holds per-client subscription rate limiting settings.
type SubscribeConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // subscriptions per second per client
	Burst   int     `yaml:"burst"` // burst allowance
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() Config {
	return Config{
		Enabled: false,
		Connection: ConnectionConfig{
			Enabled:         true,
			Rate:            100.0 / 60.0, // 100 connections per minute per IP
			Burst:           20,
			CleanupInterval: 5 * time.Minute,
		},
		Message: MessageConfig{
			Enabled: true,
			Rate:    1000, // 1000 messages per second per client
			Burst:   100,
		},
		Subscribe: SubscribeConfig{
			Enabled: true,
			Rate:    100, // 100 subscriptions per second per client
			Burst:   10,
		},
	}
}

// Manager coordinates all rate limiters.
type Manager struct {
	config   Config
	ip       *IPRateLimiter
	client   *ClientRateLimiter
	disabled bool
}

// NewManager creates a new rate limit manager.
func NewManager(cfg Config) *Manager {
	if !cfg.Enabled {
		return &Manager{disabled: true, config: cfg}
	}

	var ip *IPRateLimiter
	var client *ClientRateLimiter

	if cfg.Connection.Enabled {
		ip = NewIPRateLimiter(cfg.Connection.Rate, cfg.Connection.Burst, cfg.Connection.CleanupInterval)
	}

	if cfg.Message.Enabled || cfg.Subscribe.Enabled {
		client = NewClientRateLimiter(
			cfg.Message.Rate,
			cfg.Message.Burst,
			cfg.Subscribe.Rate,
			cfg.Subscribe.Burst,
		)
	}

	return &Manager{
		config: cfg,
		ip:     ip,
		client: client,
	}
}

// AllowConnection checks if a new connection from the given address is allowed.
func (m *Manager) AllowConnection(addr net.Addr) bool {
	if m.disabled || m.ip == nil || !m.config.Connection.Enabled {
		return true
	}
	return m.ip.Allow(addr)
}

// Allow implements the IPRateLimiter interface used by TCP and WebSocket servers.
func (m *Manager) Allow(addr net.Addr) bool {
	return m.AllowConnection(addr)
}

// AllowPublish checks if a publish from the given client is allowed.
func (m *Manager) AllowPublish(clientID string) bool {
	if m.disabled || m.client == nil || !m.config.Message.Enabled {
		return true
	}
	return m.client.AllowPublish(clientID)
}

// AllowSubscribe checks if a subscription from the given client is allowed.
func (m *Manager) AllowSubscribe(clientID string) bool {
	if m.disabled || m.client == nil || !m.config.Subscribe.Enabled {
		return true
	}
	return m.client.AllowSubscribe(clientID)
}

// OnClientDisconnect cleans up rate limiters for a disconnected client.
func (m *Manager) OnClientDisconnect(clientID string) {
	if m.disabled || m.client == nil {
		return
	}
	m.client.RemoveClient(clientID)
}

// Stop stops the rate limiter manager and cleans up resources.
func (m *Manager) Stop() {
	if m.ip != nil {
		m.ip.Stop()
	}
}
