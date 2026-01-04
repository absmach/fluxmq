// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sync"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/queue/storage"
)

// DeliveryWorker manages partition workers for a queue.
// This is the coordinator that creates one worker per partition,
// enabling true parallelism and providing the foundation for
// both lock-free optimization and cluster scaling.
type DeliveryWorker struct {
	queue            *Queue
	messageStore     storage.MessageStore
	broker           DeliverFn
	partitionWorkers []*PartitionWorker
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

// NewDeliveryWorker creates a new delivery worker for a queue.
func NewDeliveryWorker(queue *Queue, messageStore storage.MessageStore, broker DeliverFn, c cluster.Cluster, localNodeID string, routingMode ConsumerRoutingMode, raftMgr *RaftManager) *DeliveryWorker {
	config := queue.Config()
	batchSize := config.BatchSize
	// Default batch size
	if batchSize <= 0 {
		batchSize = 100
	}

	// Create one worker per partition
	workers := make([]*PartitionWorker, 0, len(queue.Partitions()))
	for _, partition := range queue.Partitions() {
		worker := NewPartitionWorker(
			queue.Name(),
			partition.ID(),
			queue,
			messageStore,
			broker,
			batchSize,
			c,
			localNodeID,
			routingMode,
			raftMgr,
		)
		workers = append(workers, worker)
	}

	return &DeliveryWorker{
		queue:            queue,
		messageStore:     messageStore,
		broker:           broker,
		partitionWorkers: workers,
		stopCh:           make(chan struct{}),
	}
}

// Start starts all partition workers.
func (dw *DeliveryWorker) Start(ctx context.Context) {
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
func (dw *DeliveryWorker) Stop() {
	// Stop all workers
	for _, worker := range dw.partitionWorkers {
		worker.Stop()
	}

	// Wait for all workers to finish
	dw.wg.Wait()

	close(dw.stopCh)
}

// NotifyPartition notifies a specific partition worker that messages are available.
// This is called by the enqueue operation to wake the worker immediately.
func (dw *DeliveryWorker) NotifyPartition(partitionID int) {
	if partitionID >= 0 && partitionID < len(dw.partitionWorkers) {
		dw.partitionWorkers[partitionID].Notify()
	}
}

// deliverMessages is kept for backward compatibility with tests.
// It processes messages synchronously on all partition workers.
func (dw *DeliveryWorker) deliverMessages(ctx context.Context) {
	// Process messages synchronously for all partition workers
	// This ensures tests can verify delivery immediately
	for _, worker := range dw.partitionWorkers {
		worker.ProcessMessages(ctx)
	}
}
