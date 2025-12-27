// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryStore_Store(t *testing.T) {
	store := NewInMemoryStore()

	msg := &Message{
		Topic:   "test/topic",
		Payload: []byte("hello"),
		QoS:     1,
	}

	store.Store(msg)
	assert.Equal(t, 1, store.Count())
}

func TestInMemoryStore_Get(t *testing.T) {
	store := NewInMemoryStore()

	store.Store(&Message{Topic: "a/b", Payload: []byte("1")})
	store.Store(&Message{Topic: "a/c", Payload: []byte("2")})
	store.Store(&Message{Topic: "a/b", Payload: []byte("3")})

	msgs := store.Get("a/b")
	assert.Len(t, msgs, 2)
	assert.Equal(t, []byte("1"), msgs[0].Payload)
	assert.Equal(t, []byte("3"), msgs[1].Payload)

	msgs = store.Get("a/c")
	assert.Len(t, msgs, 1)

	msgs = store.Get("nonexistent")
	assert.Len(t, msgs, 0)
}

func TestInMemoryStore_GetAll(t *testing.T) {
	store := NewInMemoryStore()

	store.Store(&Message{Topic: "a", Payload: []byte("1")})
	store.Store(&Message{Topic: "b", Payload: []byte("2")})

	all := store.GetAll()
	assert.Len(t, all, 2)
	assert.Equal(t, "a", all[0].Topic)
	assert.Equal(t, "b", all[1].Topic)
}

func TestInMemoryStore_Clear(t *testing.T) {
	store := NewInMemoryStore()

	store.Store(&Message{Topic: "a", Payload: []byte("1")})
	store.Store(&Message{Topic: "b", Payload: []byte("2")})
	assert.Equal(t, 2, store.Count())

	store.Clear()
	assert.Equal(t, 0, store.Count())
}

func TestInMemoryStore_ConcurrentAccess(t *testing.T) {
	store := NewInMemoryStore()
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			store.Store(&Message{Topic: "test", Payload: []byte("msg")})
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_ = store.GetAll()
			_ = store.Count()
		}
		done <- true
	}()

	<-done
	<-done
	assert.Equal(t, 100, store.Count())
}

func TestWillMessage(t *testing.T) {
	will := &WillMessage{
		Topic:   "client/status",
		Payload: []byte("offline"),
		QoS:     1,
		Retain:  true,
	}

	assert.Equal(t, "client/status", will.Topic)
	assert.Equal(t, []byte("offline"), will.Payload)
	assert.Equal(t, byte(1), will.QoS)
	assert.True(t, will.Retain)
}

func TestConnectOptions(t *testing.T) {
	opts := ConnectOptions{
		CleanSession: false,
		KeepAlive:    120,
		Will: &WillMessage{
			Topic:   "lwt",
			Payload: []byte("bye"),
			QoS:     0,
		},
	}

	assert.False(t, opts.CleanSession)
	assert.Equal(t, uint16(120), opts.KeepAlive)
	assert.NotNil(t, opts.Will)
	assert.Equal(t, "lwt", opts.Will.Topic)
}

func TestMessage(t *testing.T) {
	msg := &Message{
		Topic:    "test/topic",
		Payload:  []byte("payload"),
		QoS:      2,
		Retain:   true,
		Dup:      false,
		PacketID: 123,
	}

	assert.Equal(t, "test/topic", msg.Topic)
	assert.Equal(t, []byte("payload"), msg.Payload)
	assert.Equal(t, byte(2), msg.QoS)
	assert.True(t, msg.Retain)
	assert.False(t, msg.Dup)
	assert.Equal(t, uint16(123), msg.PacketID)
}

func TestClientState(t *testing.T) {
	node := &TestNode{TCPAddr: "127.0.0.1:1883"}
	client := NewTestMQTTClient(t, node, "test-client")

	assert.Equal(t, StateDisconnected, client.State())
	assert.False(t, client.IsConnected())
}

func TestClientSetMessageStore(t *testing.T) {
	node := &TestNode{TCPAddr: "127.0.0.1:1883"}
	client := NewTestMQTTClient(t, node, "test-client")

	customStore := NewInMemoryStore()
	client.SetMessageStore(customStore)

	store := client.Messages()
	require.NotNil(t, store)

	// Verify it's the same store
	store.Store(&Message{Topic: "test", Payload: []byte("data")})
	assert.Equal(t, 1, customStore.Count())
}

func TestClientConnectWithoutServer(t *testing.T) {
	node := &TestNode{TCPAddr: "127.0.0.1:19999"} // No server listening
	client := NewTestMQTTClient(t, node, "test-client")

	err := client.Connect(true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect")
	assert.Equal(t, StateDisconnected, client.State())
}

func TestClientOperationsWhenDisconnected(t *testing.T) {
	node := &TestNode{TCPAddr: "127.0.0.1:1883"}
	client := NewTestMQTTClient(t, node, "test-client")

	err := client.Subscribe("test", 0)
	assert.Equal(t, ErrNotConnected, err)

	err = client.Publish("test", 0, []byte("data"), false)
	assert.Equal(t, ErrNotConnected, err)

	// Disconnect when not connected should be safe
	err = client.Disconnect()
	assert.NoError(t, err)
}
