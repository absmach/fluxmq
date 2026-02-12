// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	qraft "github.com/absmach/fluxmq/queue/raft"
)

func TestBuildRaftGroupRuntimes(t *testing.T) {
	disable := false
	enableAsync := false

	raftCfg := config.RaftConfig{
		Enabled:           true,
		ReplicationFactor: 3,
		SyncMode:          true,
		MinInSyncReplicas: 2,
		AckTimeout:        5 * time.Second,
		BindAddr:          "127.0.0.1:7100",
		DataDir:           "/tmp/fluxmq/raft",
		Peers: map[string]string{
			"node1": "127.0.0.1:7100",
		},
		HeartbeatTimeout:  1 * time.Second,
		ElectionTimeout:   3 * time.Second,
		SnapshotInterval:  5 * time.Minute,
		SnapshotThreshold: 8192,
		Groups: map[string]config.RaftGroupConfig{
			"hot": {
				BindAddr:          "127.0.0.1:8100",
				DataDir:           "/tmp/fluxmq/raft-hot",
				Peers:             map[string]string{"node1": "127.0.0.1:8100"},
				ReplicationFactor: 5,
				SyncMode:          &enableAsync,
				MinInSyncReplicas: 3,
			},
			"cold": {
				Enabled: &disable,
			},
		},
	}

	runtimes, err := buildRaftGroupRuntimes(raftCfg)
	if err != nil {
		t.Fatalf("buildRaftGroupRuntimes failed: %v", err)
	}
	if len(runtimes) != 2 {
		t.Fatalf("expected 2 runtimes (default + hot), got %d", len(runtimes))
	}

	var (
		foundDefault bool
		foundHot     bool
	)
	for _, rt := range runtimes {
		switch rt.GroupID {
		case qraft.DefaultGroupID:
			foundDefault = true
			if rt.BindAddr != "127.0.0.1:7100" {
				t.Fatalf("unexpected default bind addr: %q", rt.BindAddr)
			}
			if rt.ManagerConfig.SyncMode != true {
				t.Fatalf("unexpected default sync mode: %v", rt.ManagerConfig.SyncMode)
			}
		case "hot":
			foundHot = true
			if rt.BindAddr != "127.0.0.1:8100" {
				t.Fatalf("unexpected hot bind addr: %q", rt.BindAddr)
			}
			if rt.ManagerConfig.ReplicationFactor != 5 {
				t.Fatalf("unexpected hot replication factor: %d", rt.ManagerConfig.ReplicationFactor)
			}
			if rt.ManagerConfig.SyncMode {
				t.Fatalf("expected hot sync mode override to false")
			}
		}
	}

	if !foundDefault || !foundHot {
		t.Fatalf("expected both default and hot runtimes")
	}
}

func TestBuildRaftGroupRuntimesRejectsDuplicateBind(t *testing.T) {
	raftCfg := config.RaftConfig{
		Enabled:           true,
		ReplicationFactor: 3,
		MinInSyncReplicas: 2,
		AckTimeout:        5 * time.Second,
		BindAddr:          "127.0.0.1:7100",
		DataDir:           "/tmp/fluxmq/raft",
		Groups: map[string]config.RaftGroupConfig{
			"hot": {
				BindAddr: "127.0.0.1:7100",
				Peers:    map[string]string{"node1": "127.0.0.1:7100"},
			},
		},
	}

	_, err := buildRaftGroupRuntimes(raftCfg)
	if err == nil {
		t.Fatalf("expected duplicate bind address error")
	}
	if !strings.Contains(err.Error(), "share bind_addr") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRaftGroupProvisionerDerivesAutoGroupEndpoints(t *testing.T) {
	raftCfg := config.RaftConfig{
		Enabled:             true,
		AutoProvisionGroups: true,
		ReplicationFactor:   3,
		MinInSyncReplicas:   2,
		AckTimeout:          5 * time.Second,
		BindAddr:            "127.0.0.1:7100",
		DataDir:             "/tmp/fluxmq/raft",
		Peers: map[string]string{
			"node2": "127.0.0.1:7200",
		},
	}

	p := newRaftGroupProvisioner("node1", raftCfg, nil, nil, nil, nil, nil)
	derived, err := p.deriveAutoGroupConfig("hot")
	if err != nil {
		t.Fatalf("deriveAutoGroupConfig failed: %v", err)
	}
	if derived.BindAddr == raftCfg.BindAddr {
		t.Fatalf("expected derived bind addr to differ from base")
	}
	if len(derived.Peers) != 1 {
		t.Fatalf("expected 1 derived peer, got %d", len(derived.Peers))
	}
}

func TestRaftGroupProvisionerRejectsUnknownWhenAutoProvisionDisabled(t *testing.T) {
	raftCfg := config.RaftConfig{
		Enabled:             true,
		AutoProvisionGroups: false,
		ReplicationFactor:   3,
		MinInSyncReplicas:   2,
		AckTimeout:          5 * time.Second,
		BindAddr:            "127.0.0.1:7100",
		DataDir:             "/tmp/fluxmq/raft",
	}

	p := newRaftGroupProvisioner("node1", raftCfg, nil, nil, map[string]*qraft.Manager{
		qraft.DefaultGroupID: {},
	}, nil, nil)

	_, err := p.GetOrCreateGroup(context.Background(), "hot")
	if err == nil {
		t.Fatalf("expected unknown group error")
	}
	if !strings.Contains(err.Error(), "auto_provision_groups is disabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}
