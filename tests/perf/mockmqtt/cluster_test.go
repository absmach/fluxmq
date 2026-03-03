// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mockmqtt

import (
	"bytes"
	"context"
	"testing"
	"time"

	mqttclient "github.com/absmach/fluxmq/client/mqtt"
)

func TestClusterRoutesMQTTV3AcrossNodes(t *testing.T) {
	_, addrs := startTestCluster(t)

	received := make(chan []byte, 1)
	sub := startMQTTClient(t, addrs[1], 4, func(msg *mqttclient.Message) {
		if msg == nil {
			return
		}
		if msg.Topic != "perf/mock/v3" {
			return
		}
		payload := append([]byte(nil), msg.Payload...)
		select {
		case received <- payload:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sub.SubscribeSingle(ctx, "perf/mock/v3", 2); err != nil {
		t.Fatalf("subscribe v3 failed: %v", err)
	}

	pub := startMQTTClient(t, addrs[0], 4, nil)
	if err := pub.Publish(ctx, "perf/mock/v3", []byte("hello-v3"), 2, false); err != nil {
		t.Fatalf("publish v3 qos2 failed: %v", err)
	}

	select {
	case got := <-received:
		if !bytes.Equal(got, []byte("hello-v3")) {
			t.Fatalf("unexpected payload: got=%q", string(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for v3 message")
	}
}

func TestClusterRoutesMQTTV5AcrossNodes(t *testing.T) {
	_, addrs := startTestCluster(t)

	received := make(chan []byte, 1)
	sub := startMQTTClient(t, addrs[2], 5, func(msg *mqttclient.Message) {
		if msg == nil {
			return
		}
		if msg.Topic != "perf/mock/v5" {
			return
		}
		payload := append([]byte(nil), msg.Payload...)
		select {
		case received <- payload:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sub.SubscribeSingle(ctx, "perf/mock/v5", 2); err != nil {
		t.Fatalf("subscribe v5 failed: %v", err)
	}

	pub := startMQTTClient(t, addrs[0], 5, nil)
	if err := pub.Publish(ctx, "perf/mock/v5", []byte("hello-v5"), 2, false); err != nil {
		t.Fatalf("publish v5 qos2 failed: %v", err)
	}

	select {
	case got := <-received:
		if !bytes.Equal(got, []byte("hello-v5")) {
			t.Fatalf("unexpected payload: got=%q", string(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for v5 message")
	}
}

func startTestCluster(t *testing.T) (*Cluster, []string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cluster := New([]string{"127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"})
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.Close()
	})

	addrs := cluster.Addrs()
	if len(addrs) != 3 {
		t.Fatalf("expected 3 addrs, got %d", len(addrs))
	}

	return cluster, addrs
}

func startMQTTClient(t *testing.T, addr string, protocolVersion byte, onMessage func(*mqttclient.Message)) *mqttclient.Client {
	t.Helper()

	opts := mqttclient.NewOptions().
		SetServers(addr).
		SetClientID(t.Name() + "-" + addr).
		SetProtocolVersion(protocolVersion).
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second).
		SetWriteTimeout(2 * time.Second).
		SetAckTimeout(2 * time.Second).
		SetKeepAlive(10 * time.Second).
		SetAutoReconnect(false)
	if onMessage != nil {
		opts.SetOnMessageV2(onMessage)
	}

	client, err := mqttclient.New(opts)
	if err != nil {
		t.Fatalf("new mqtt client failed: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close(context.Background())
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect mqtt client failed: %v", err)
	}

	return client
}
