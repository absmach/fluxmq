// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"sync"

	"github.com/absmach/fluxmq/cluster"
	queueStorage "github.com/absmach/fluxmq/queue/storage"
)

// Worker manages partition workers for a queue.
type Worker struct {
	queue            QueueSource
	messageStore     queueStorage.MessageStore
	broker           DeliverFn
	partitionWorkers []*PartitionWorker
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

// NewWorker creates a new delivery worker for a queue.
func NewWorker(
	queue QueueSource,
	messageStore queueStorage.MessageStore,
	broker DeliverFn,
	c cluster.Cluster,
	localNodeID string,
	routingMode ConsumerRoutingMode,
	raftMgr RaftManager,
	retentionMgr RetentionManager,
) *Worker {
	config := queue.Config()
	batchSize := config.BatchSize
	// Default batch size
	if batchSize <= 0 {
		batchSize = 100
	}

	// Get partitions from config or calculate?
	// The queue implementation knows its partitions. But queue.Partitions() returns []*Partition (queue.Partition).
	// The worker just needs to know how many partitions there are to create partition workers.
	// Since QueueSource doesn't expose partitions (to avoid dependency on queue.Partition), we use config.Partitions.

	numPartitions := config.Partitions
	workers := make([]*PartitionWorker, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		worker := NewPartitionWorker(
			queue.Name(),
			i,
			queue,
			messageStore,
			broker,
			batchSize,
			c,
			localNodeID,
			routingMode,
			raftMgr,
			retentionMgr,
		)
		workers = append(workers, worker)
	}

	return &Worker{
		queue:            queue,
		messageStore:     messageStore,
		broker:           broker,
		partitionWorkers: workers,
		stopCh:           make(chan struct{}),
	}
}

// Start starts all partition workers.
func (dw *Worker) Start(ctx context.Context) {
	// Start one goroutine per partition worker
	for _, worker := range dw.partitionWorkers {
		dw.wg.Add(1)
		go func(w *PartitionWorker) {
			defer dw.wg.Done()
			w.Start(ctx)
		}(worker)
	}
}

// Stop stops all partition workers.
func (dw *Worker) Stop() {
	// Stop all workers
	for _, worker := range dw.partitionWorkers {
		worker.Stop()
	}

	// Wait for all workers to finish
	dw.wg.Wait()

	close(dw.stopCh)
}

// NotifyPartition notifies a specific partition worker that messages are available.
func (dw *Worker) NotifyPartition(partitionID int) {
	if partitionID >= 0 && partitionID < len(dw.partitionWorkers) {
		dw.partitionWorkers[partitionID].Notify()
	}
}

// DeliverMessages is kept for backward compatibility with tests.
// It processes messages synchronously on all partition workers.
func (dw *Worker) DeliverMessages(ctx context.Context) {
	// Process messages synchronously for all partition workers
	// This ensures tests can verify delivery immediately
	for _, worker := range dw.partitionWorkers {
		worker.ProcessMessages(ctx)
	}
}
