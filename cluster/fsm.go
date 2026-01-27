// Package cluster provides the FSM for managing cluster-wide metadata,
// intended to replace etcd.
package cluster

import (
	"encoding/json"
	"io"
	"sync"

	packets "github.com/absmach/fluxmq/core/packets/v5"
	"github.com/hashicorp/raft"
)

// FSMState represents the in-memory state of the cluster that is replicated
// across all nodes. It includes node information, MQTT retained messages,
// and will messages.
type FSMState struct {
	// Nodes maps a node's ID to its information.
	Nodes map[string]NodeInfo `json:"nodes"`

	// RetainedMessages maps a topic to its retained message.
	RetainedMessages map[string]*packets.Publish `json:"retained_messages"`

	// WillMessages maps a client ID to its will message.
	WillMessages map[string]*packets.Publish `json:"will_messages"`

	// Subscriptions maps a topic filter to a map of client IDs to their subscription info.
	Subscriptions map[string]map[string]SubscriptionInfo `json:"subscriptions"`
}

// SubscriptionInfo stores details about a client's subscription.
type SubscriptionInfo struct {
	ClientID string `json:"client_id"`
	QoS      byte   `json:"qos"`
	// Other MQTT v5 options (NoLocal, RetainAsPublished, etc.) can be added here.
}

// Command represents a serializable operation that can be applied to the FSM.
// In a real implementation, this would likely be an interface with multiple
// concrete command types. For simplicity, we'll use a single struct with a
// type field.
type Command struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// FSM is the Raft finite-state machine for the cluster.
// It implements the raft.FSM interface.
type FSM struct {
	mu    sync.RWMutex
	state *FSMState
}

// NewFSM creates a new instance of the cluster FSM.
func NewFSM() *FSM {
	return &FSM{
		state: &FSMState{
			Nodes:            make(map[string]NodeInfo),
			RetainedMessages: make(map[string]*packets.Publish),
			WillMessages:     make(map[string]*packets.Publish),
			Subscriptions:    make(map[string]map[string]SubscriptionInfo),
		},
	}
}

// Apply applies a Raft log entry to the FSM. This is the core of the FSM logic,
// where state modifications happen. It is called by the Raft consensus group
// once a log entry is committed.
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		// This is a fatal error, as it means we cannot process the Raft log.
		// In a real system, you'd want to panic or have a more robust error handling
		// mechanism that could potentially lead to a state recovery.
		panic("failed to unmarshal command: " + err.Error())
	}

	// For now, we assume commands are simple and don't require complex logic.
	// A real implementation would have a switch statement for different cmd.Type
	// values and unmarshal the payload into specific command structs.
	// e.g., case "addNode": ...
	//      case "storeRetained": ...

	// As a placeholder, this FSM doesn't yet modify state.
	// You would add your logic here.

	return nil
}

// Snapshot is called by Raft to take a snapshot of the FSM's current state.
// This is used to compact the Raft log and for fast recovery of new or lagging nodes.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// It's crucial to make a deep copy of the state to avoid race conditions.
	// The FSM can continue to be modified while the snapshot is being written.
	copiedState := &FSMState{
		Nodes:            make(map[string]NodeInfo),
		RetainedMessages: make(map[string]*packets.Publish),
		WillMessages:     make(map[string]*packets.Publish),
		Subscriptions:    make(map[string]map[string]SubscriptionInfo),
	}

	for k, v := range f.state.Nodes {
		copiedState.Nodes[k] = v
	}
	for k, v := range f.state.RetainedMessages {
		copiedState.RetainedMessages[k] = v
	}
	for k, v := range f.state.WillMessages {
		copiedState.WillMessages[k] = v
	}
	for topic, subs := range f.state.Subscriptions {
		copiedSubs := make(map[string]SubscriptionInfo)
		for k, v := range subs {
			copiedSubs[k] = v
		}
		copiedState.Subscriptions[topic] = copiedSubs
	}

	return &fsmSnapshot{state: copiedState}, nil
}

// Restore is called by Raft to restore the FSM's state from a snapshot.
// This will overwrite the FSM's current state entirely.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var newState FSMState
	if err := json.NewDecoder(rc).Decode(&newState); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = &newState

	return nil
}

// fsmSnapshot implements the raft.FSMSnapshot interface. It provides Raft
// with a way to persist the FSM's state to a sink (usually a file on disk).
type fsmSnapshot struct {
	state *FSMState
}

// Persist saves the FSM state to the provided sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode the state as JSON
		if err := json.NewEncoder(sink).Encode(s.state); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		sink.Cancel()
	}

	return err
}

// Release is called when Raft is finished with the snapshot.
func (s *fsmSnapshot) Release() {}
