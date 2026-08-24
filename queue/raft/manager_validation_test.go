// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

func TestManagerValidatesReplicationTopologyAndQueueGuarantees(t *testing.T) {
	base := DefaultManagerConfig()
	base.Enabled = true
	newManager := func(cfg ManagerConfig, peers map[string]string) *Manager {
		return NewManager("node-1", "127.0.0.1:7100", t.TempDir(), nil, nil, peers, cfg, nil, nil)
	}
	manager := newManager(base, map[string]string{
		"node-2": "127.0.0.1:7101",
		"node-3": "127.0.0.1:7102",
	})
	require.NoError(t, manager.validateReplicationTopology())
	require.NoError(t, manager.ValidateReplicationConfig(types.ReplicationConfig{
		Enabled: true, ReplicationFactor: 3, MinInSyncReplicas: 2, Mode: types.ReplicationSync, AckTimeout: time.Second,
	}))

	mismatch := newManager(base, map[string]string{"node-2": "127.0.0.1:7101"})
	require.Error(t, mismatch.validateReplicationTopology())

	asyncCfg := base
	asyncCfg.SyncMode = false
	async := newManager(asyncCfg, map[string]string{
		"node-2": "127.0.0.1:7101",
		"node-3": "127.0.0.1:7102",
	})
	require.Error(t, async.validateReplicationTopology())

	require.Error(t, manager.ValidateReplicationConfig(types.ReplicationConfig{
		Enabled: true, ReplicationFactor: 2, MinInSyncReplicas: 2, Mode: types.ReplicationSync, AckTimeout: time.Second,
	}))
	require.Error(t, manager.ValidateReplicationConfig(types.ReplicationConfig{
		Enabled: true, ReplicationFactor: 3, MinInSyncReplicas: 2, Mode: types.ReplicationAsync, AckTimeout: time.Second,
	}))
}
