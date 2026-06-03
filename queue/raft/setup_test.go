// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
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
		BindAddr:          testRaftBind7100,
		DataDir:           testRaftDataDir,
		Peers: map[string]string{
			testRaftNode1: testRaftBind7100,
		},
		HeartbeatTimeout:  1 * time.Second,
		ElectionTimeout:   3 * time.Second,
		SnapshotInterval:  5 * time.Minute,
		SnapshotThreshold: 8192,
		Groups: map[string]config.RaftGroupConfig{
			testGroupHot: {
				BindAddr:          testRaftBind8100,
				DataDir:           "/tmp/fluxmq/raft-hot",
				Peers:             map[string]string{testRaftNode1: testRaftBind8100},
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
		case DefaultGroupID:
			foundDefault = true
			if rt.BindAddr != testRaftBind7100 {
				t.Fatalf("unexpected default bind addr: %q", rt.BindAddr)
			}
			if rt.ManagerConfig.SyncMode != true {
				t.Fatalf("unexpected default sync mode: %v", rt.ManagerConfig.SyncMode)
			}
		case testGroupHot: //nolint:goconst // test value
			foundHot = true
			if rt.BindAddr != testRaftBind8100 {
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
		BindAddr:          testRaftBind7100,
		DataDir:           testRaftDataDir,
		Groups: map[string]config.RaftGroupConfig{
			testGroupHot: {
				BindAddr: testRaftBind7100,
				Peers:    map[string]string{testRaftNode1: testRaftBind7100},
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
		BindAddr:            testRaftBind7100,
		DataDir:             testRaftDataDir,
		Peers: map[string]string{
			"node2": "127.0.0.1:7200",
		},
	}

	p := newRaftGroupProvisioner(testRaftNode1, raftCfg, nil, nil, nil, nil, nil)
	derived, err := p.deriveAutoGroupConfig(testGroupHot)
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
		BindAddr:            testRaftBind7100,
		DataDir:             testRaftDataDir,
	}

	p := newRaftGroupProvisioner(testRaftNode1, raftCfg, nil, nil, map[string]*Manager{
		DefaultGroupID: {},
	}, nil, nil)

	_, err := p.GetOrCreateGroup(context.Background(), testGroupHot)
	if err == nil {
		t.Fatalf("expected unknown group error")
	}
	if !strings.Contains(err.Error(), "auto_provision_groups is disabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRaftGroupProvisionerTryReleaseGroupSkipsStaticGroups(t *testing.T) {
	p := newRaftGroupProvisioner(
		testRaftNode1,
		config.RaftConfig{},
		nil,
		nil,
		map[string]*Manager{
			DefaultGroupID: {},
			testGroupHot:   {},
		},
		[]RaftGroupRuntime{
			{GroupID: DefaultGroupID, BindAddr: testRaftBind7100},
			{GroupID: testGroupHot, BindAddr: testRaftBind8100},
		},
		nil,
	)

	released, err := p.TryReleaseGroup(context.Background(), testGroupHot)
	if err != nil {
		t.Fatalf("TryReleaseGroup failed: %v", err)
	}
	if released {
		t.Fatalf("expected static group release to be skipped")
	}
	if _, ok := p.managers[testGroupHot]; !ok {
		t.Fatalf("expected static group manager to remain")
	}
	if got := p.usedBind[testRaftBind8100]; got != testGroupHot {
		t.Fatalf("expected static bind mapping to remain, got %q", got)
	}
}

func TestRaftGroupProvisionerTryReleaseGroupRemovesDynamicTracking(t *testing.T) {
	p := newRaftGroupProvisioner(
		testRaftNode1,
		config.RaftConfig{},
		nil,
		nil,
		map[string]*Manager{
			DefaultGroupID: {},
		},
		[]RaftGroupRuntime{
			{GroupID: DefaultGroupID, BindAddr: testRaftBind7100},
		},
		nil,
	)

	p.managers["dynamic"] = nil
	p.bindByGroup["dynamic"] = "127.0.0.1:9100"
	p.usedBind["127.0.0.1:9100"] = "dynamic"

	released, err := p.TryReleaseGroup(context.Background(), "dynamic")
	if err != nil {
		t.Fatalf("TryReleaseGroup failed: %v", err)
	}
	if !released {
		t.Fatalf("expected dynamic group to be released")
	}
	if _, ok := p.managers["dynamic"]; ok {
		t.Fatalf("expected dynamic manager to be removed")
	}
	if _, ok := p.bindByGroup["dynamic"]; ok {
		t.Fatalf("expected dynamic bind-by-group mapping to be removed")
	}
	if _, ok := p.usedBind["127.0.0.1:9100"]; ok {
		t.Fatalf("expected dynamic bind mapping to be removed")
	}
}
