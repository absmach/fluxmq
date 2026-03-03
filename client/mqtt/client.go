// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type writeRequest struct {
	data     []byte
	deadline time.Time
	errCh    chan error // nil for fire-and-forget
	// Indicates errCh came from the write error channel pool.
	pooledErrCh bool
	ctx         context.Context
	// Set for outbound publish writes subject to outbound pressure limits.
	trackOutbound bool
	dropTopic     string
	dropQoS       byte
	dropPayloadSz int
}

const defaultControlWriteChanSize = 64
const defaultCallbackQueueSize = 256

var (
	callbackQueueOnce sync.Once
	callbackQueueCh   chan func()

	writeErrChPool = sync.Pool{
		New: func() any {
			return make(chan error, 1)
		},
	}
)

type writeRuntime struct {
	conn           net.Conn
	stopCh         chan struct{}
	doneCh         chan struct{}
	writeCh        chan writeRequest
	controlWriteCh chan writeRequest
	qos0Wake       chan struct{}
	writeDone      chan struct{}
}

type dispatchRuntime struct {
	msgCh   chan *Message
	msgStop chan struct{}
}

type reconnectBuffered struct {
	wire       []byte
	topic      string
	qos        byte
	payloadSz  int
	bufferedSz int
}

// Client is a thread-safe MQTT client.
type Client struct {
	opts *Options

	// State management
	state *stateManager

	// Guards connection and write-loop channel pointers.
	writeStateMu sync.RWMutex
	writeRT      atomic.Pointer[writeRuntime]

	// Connection — only touched by writeLoop (writes) and readLoop (reads) after Connect().
	conn net.Conn

	// Write serialization
	writeCh        chan writeRequest
	controlWriteCh chan writeRequest
	qos0Wake       chan struct{}
	writeDone      chan struct{}
	qos0BufMu      sync.Mutex
	qos0Buf        []writeRequest

	// Server capabilities (MQTT 5.0)
	serverCaps   *ServerCapabilities
	serverCapsMu sync.RWMutex

	// Topic aliases (MQTT 5.0)
	topicAliases *topicAliasManager

	// Queue subscriptions
	queueSubs     *queueSubscriptions
	queueAckCache *queueAckCache
	subscriptions *subscriptionRegistry

	// Pending operations
	pending *pendingStore

	// Message store for QoS 1/2
	store MessageStore

	// QoS 2 incoming messages waiting for PUBREL
	qos2Incoming   map[uint16]*Message
	qos2IncomingMu sync.Mutex

	// Lifecycle
	lifecycleMu sync.Mutex
	stopCh      chan struct{}
	doneCh      chan struct{}
	reconnMu    sync.Mutex

	// Keep-alive
	lastActivity atomic.Int64 // UnixNano timestamp, no mutex needed
	pingMu       sync.Mutex
	pingTimer    *time.Timer
	waitingPing  bool
	lastPingSent time.Time

	// Message dispatching
	dispatchMu sync.RWMutex
	dispatchRT atomic.Pointer[dispatchRuntime]
	msgCh      chan *Message
	msgStop    chan struct{}
	dispatchWg sync.WaitGroup

	// Callback queue pressure accounting.
	pendingMu       sync.Mutex
	pendingCond     *sync.Cond // signaled when pendingMessages/pendingBytes decrease
	pendingMessages int
	pendingBytes    int64
	droppedMessages atomic.Uint64

	// Avoid flooding async error callback with repeated slow-consumer notifications.
	slowConsumerNotified atomic.Bool

	// Outbound publish pressure accounting.
	outboundMu              sync.Mutex
	outboundCond            *sync.Cond // signaled when outbound pending decreases
	outboundPendingMessages int
	outboundPendingBytes    int64

	// Buffered publishes while disconnected.
	reconnectBufMu    sync.Mutex
	reconnectBuf      []*reconnectBuffered
	reconnectBufBytes int

	// Server index for round-robin
	serverIdx int

	// Cleanup guard to keep teardown idempotent under concurrent lifecycle calls.
	cleanupInProgress uint32

	// Drain mode rejects new publishes until disconnect completes.
	draining atomic.Bool

	// drainCh is poked (non-blocking) whenever a condition that Drain() waits on changes.
	drainCh chan struct{}
}

// New creates a new MQTT client with the given options.
func New(opts *Options) (*Client, error) {
	if opts == nil {
		return nil, ErrNilOptions
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}

	c := &Client{
		opts:          opts,
		state:         newStateManager(),
		pending:       newPendingStore(opts.MaxInflight),
		store:         store,
		qos2Incoming:  make(map[uint16]*Message),
		queueSubs:     newQueueSubscriptions(),
		queueAckCache: newQueueAckCache(5 * time.Minute),
		subscriptions: newSubscriptionRegistry(),
		drainCh:       make(chan struct{}, 1),
	}
	c.pending.onPublishChange = c.notifyDrain
	c.pendingCond = sync.NewCond(&c.pendingMu)
	c.outboundCond = sync.NewCond(&c.outboundMu)
	return c, nil
}
