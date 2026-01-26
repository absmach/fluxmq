// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

// import (
// 	"context"
// 	"encoding/binary"
// 	"encoding/json"
// 	"fmt"
// 	"sort"
// 	"strings"
// 	"time"

// 	"github.com/absmach/fluxmq/queue/storage"
// 	"github.com/absmach/fluxmq/queue/types"
// 	"github.com/dgraph-io/badger/v4"
// )

// const (
// 	queueMetaPrefix     = "queue:meta:"
// 	queueMessagePrefix  = "queue:msg:"
// 	queueInflightPrefix = "queue:inflight:"
// 	queueDLQPrefix      = "queue:dlq:"
// 	queueConsumerPrefix = "queue:consumer:"
// 	queueOffsetPrefix   = "queue:offset:"
// 	queueSeqPrefix      = "queue:seq:"
// 	queueCountPrefix    = "queue:count:" // Counter for O(1) Count()
// )

// // Store implements all queue storage interfaces using BadgerDB.
// type Store struct {
// 	db *badger.DB
// }

// // New creates a new BadgerDB queue store.
// func New(db *badger.DB) *Store {
// 	return &Store{db: db}
// }

// // QueueStore implementation

// func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
// 	if err := config.Validate(); err != nil {
// 		return err
// 	}

// 	key := queueMetaPrefix + config.Name
// 	data, err := json.Marshal(config)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		// Check if queue already exists
// 		_, err := txn.Get([]byte(key))
// 		if err == nil {
// 			return storage.ErrQueueAlreadyExists
// 		}
// 		if err != badger.ErrKeyNotFound {
// 			return err
// 		}

// 		// Set queue metadata
// 		if err := txn.Set([]byte(key), data); err != nil {
// 			return err
// 		}

// 		// Initialize message counter to 0
// 		counterKey := queueCountPrefix + config.Name
// 		return txn.Set([]byte(counterKey), []byte{0, 0, 0, 0, 0, 0, 0, 0}) // 8 bytes for int64
// 	})
// }

// func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
// 	key := queueMetaPrefix + queueName
// 	var config types.QueueConfig

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(key))
// 		if err != nil {
// 			if err == badger.ErrKeyNotFound {
// 				return storage.ErrQueueNotFound
// 			}
// 			return err
// 		}

// 		return item.Value(func(val []byte) error {
// 			return json.Unmarshal(val, &config)
// 		})
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &config, nil
// }

// func (s *Store) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
// 	if err := config.Validate(); err != nil {
// 		return err
// 	}

// 	key := queueMetaPrefix + config.Name
// 	data, err := json.Marshal(config)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		// Check if queue exists
// 		_, err := txn.Get([]byte(key))
// 		if err == badger.ErrKeyNotFound {
// 			return storage.ErrQueueNotFound
// 		}
// 		if err != nil {
// 			return err
// 		}

// 		return txn.Set([]byte(key), data)
// 	})
// }

// func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
// 	key := queueMetaPrefix + queueName
// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Delete([]byte(key))
// 	})
// }

// func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
// 	configs := make([]types.QueueConfig, 0)

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(queueMetaPrefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var config types.QueueConfig
// 				if err := json.Unmarshal(val, &config); err != nil {
// 					return err
// 				}
// 				configs = append(configs, config)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	return configs, err
// }

// // MessageStore implementation.
// func (s *Store) Enqueue(ctx context.Context, queueName string, msg *types.Message) error {
// 	key := makeMessageKey(queueName, msg.PartitionID, msg.Sequence)
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		// Calculate TTL - if ExpiresAt is zero (not set), use a very long TTL
// 		var ttl time.Duration
// 		if msg.ExpiresAt.IsZero() {
// 			ttl = 365 * 24 * time.Hour // 1 year default if no expiration set
// 		} else {
// 			ttl = time.Until(msg.ExpiresAt)
// 			if ttl <= 0 {
// 				// Don't store already-expired messages (and don't increment counter)
// 				return nil
// 			}
// 		}

// 		// Increment counter atomically
// 		counterKey := queueCountPrefix + queueName
// 		if err := s.incrementCounter(txn, counterKey, 1); err != nil {
// 			return err
// 		}

// 		// Store message with TTL
// 		return txn.SetEntry(badger.NewEntry([]byte(key), data).WithTTL(ttl))
// 	})
// }

// func (s *Store) Count(ctx context.Context, queueName string) (int64, error) {
// 	var count int64
// 	counterKey := queueCountPrefix + queueName

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(counterKey))
// 		if err == badger.ErrKeyNotFound {
// 			// Queue doesn't exist or counter not initialized
// 			return storage.ErrQueueNotFound
// 		}
// 		if err != nil {
// 			return err
// 		}

// 		return item.Value(func(val []byte) error {
// 			if len(val) != 8 {
// 				// Counter corrupted, fall back to zero
// 				count = 0
// 				return nil
// 			}
// 			count = int64(binary.BigEndian.Uint64(val))
// 			return nil
// 		})
// 	})

// 	return count, err
// }

// func (s *Store) Dequeue(ctx context.Context, queueName string, partitionID int) (*types.Message, error) {
// 	prefix := makePartitionPrefix(queueName, partitionID)
// 	var msg *types.Message

// 	err := s.db.Update(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)
// 		opts.PrefetchValues = true

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()

// 			var qm types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &qm)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			// Only return queued or retry messages that are ready
// 			if qm.State == types.StateQueued ||
// 				(qm.State == types.StateRetry && time.Now().After(qm.NextRetryAt)) {
// 				msg = &qm
// 				return nil
// 			}
// 		}
// 		return nil
// 	})

// 	return msg, err
// }

// func (s *Store) DequeueBatch(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
// 	if limit <= 0 {
// 		return nil, nil
// 	}

// 	prefix := makePartitionPrefix(queueName, partitionID)
// 	var messages []*types.Message

// 	// Note: Using View (not Update) because we don't delete on dequeue - just read
// 	// Messages are marked as delivered and tracked via inflight state
// 	// Deletion happens on ACK
// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)
// 		opts.PrefetchValues = true

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid() && len(messages) < limit; it.Next() {
// 			item := it.Item()

// 			var qm types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &qm)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			// Only return queued or retry messages that are ready
// 			if qm.State == types.StateQueued ||
// 				(qm.State == types.StateRetry && time.Now().After(qm.NextRetryAt)) {
// 				messages = append(messages, &qm)
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	if len(messages) == 0 {
// 		return nil, nil
// 	}

// 	return messages, nil
// }

// func (s *Store) UpdateMessage(ctx context.Context, queueName string, msg *types.Message) error {
// 	key := makeMessageKey(queueName, msg.PartitionID, msg.Sequence)
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		// Calculate TTL
// 		// For updates (e.g. changing state from QUEUED to DELIVERED), we should preserve original expiration
// 		ttl := time.Until(msg.ExpiresAt)
// 		if ttl <= 0 {
// 			ttl = time.Second
// 		}

// 		return txn.SetEntry(badger.NewEntry([]byte(key), data).WithTTL(ttl))
// 	})
// }

// func (s *Store) DeleteMessage(ctx context.Context, queueName string, messageID string) error {
// 	// Find message by ID across all partitions
// 	return s.db.Update(func(txn *badger.Txn) error {
// 		prefix := queueMessagePrefix + queueName + ":"
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()

// 			var msg types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &msg)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			if msg.ID == messageID {
// 				// Delete message
// 				if err := txn.Delete(item.Key()); err != nil {
// 					return err
// 				}

// 				// Decrement counter
// 				counterKey := queueCountPrefix + queueName
// 				return s.incrementCounter(txn, counterKey, -1)
// 			}
// 		}

// 		return storage.ErrMessageNotFound
// 	})
// }

// func (s *Store) GetMessage(ctx context.Context, queueName string, messageID string) (*types.Message, error) {
// 	var result *types.Message

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		prefix := queueMessagePrefix + queueName + ":"
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()

// 			var msg types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &msg)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			if msg.ID == messageID {
// 				result = &msg
// 				return nil
// 			}
// 		}

// 		return storage.ErrMessageNotFound
// 	})

// 	return result, err
// }

// func (s *Store) MarkInflight(ctx context.Context, state *types.DeliveryState) error {
// 	key := makeInflightKey(state.QueueName, state.MessageID, state.GroupID)
// 	data, err := json.Marshal(state)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Set([]byte(key), data)
// 	})
// }

// func (s *Store) GetInflight(ctx context.Context, queueName string) ([]*types.DeliveryState, error) {
// 	states := make([]*types.DeliveryState, 0)
// 	prefix := queueInflightPrefix + queueName + ":"

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var state types.DeliveryState
// 				if err := json.Unmarshal(val, &state); err != nil {
// 					return err
// 				}
// 				states = append(states, &state)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	return states, err
// }

// func (s *Store) GetInflightMessage(ctx context.Context, queueName, messageID, groupID string) (*types.DeliveryState, error) {
// 	key := makeInflightKey(queueName, messageID, groupID)
// 	var state types.DeliveryState

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(key))
// 		if err != nil {
// 			if err == badger.ErrKeyNotFound {
// 				return storage.ErrMessageNotFound
// 			}
// 			return err
// 		}

// 		return item.Value(func(val []byte) error {
// 			return json.Unmarshal(val, &state)
// 		})
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &state, nil
// }

// func (s *Store) GetInflightForMessage(ctx context.Context, queueName, messageID string) ([]*types.DeliveryState, error) {
// 	states := make([]*types.DeliveryState, 0)
// 	prefix := queueInflightPrefix + queueName + ":" + messageID + ":"

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var state types.DeliveryState
// 				if err := json.Unmarshal(val, &state); err != nil {
// 					return err
// 				}
// 				states = append(states, &state)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	return states, err
// }

// func (s *Store) RemoveInflight(ctx context.Context, queueName, messageID, groupID string) error {
// 	key := makeInflightKey(queueName, messageID, groupID)
// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Delete([]byte(key))
// 	})
// }

// func (s *Store) EnqueueDLQ(ctx context.Context, dlqTopic string, msg *types.Message) error {
// 	key := makeDLQKey(dlqTopic, msg.ID)
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Set([]byte(key), data)
// 	})
// }

// func (s *Store) ListDLQ(ctx context.Context, dlqTopic string, limit int) ([]*types.Message, error) {
// 	messages := make([]*types.Message, 0, limit)
// 	// Remove $queue/dlq/ prefix if present (consistent with makeDLQKey)
// 	topic := strings.TrimPrefix(dlqTopic, "$queue/dlq/")
// 	prefix := queueDLQPrefix + topic + ":"

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		count := 0
// 		for it.Rewind(); it.Valid() && (limit == 0 || count < limit); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var msg types.Message
// 				if err := json.Unmarshal(val, &msg); err != nil {
// 					return err
// 				}
// 				messages = append(messages, &msg)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 			count++
// 		}
// 		return nil
// 	})

// 	return messages, err
// }

// func (s *Store) DeleteDLQMessage(ctx context.Context, dlqTopic, messageID string) error {
// 	key := makeDLQKey(dlqTopic, messageID)
// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Delete([]byte(key))
// 	})
// }

// func (s *Store) ListRetry(ctx context.Context, queueName string, partitionID int) ([]*types.Message, error) {
// 	messages := make([]*types.Message, 0)
// 	prefix := makePartitionPrefix(queueName, partitionID)

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var msg types.Message
// 				if err := json.Unmarshal(val, &msg); err != nil {
// 					return err
// 				}
// 				if msg.State == types.StateRetry {
// 					messages = append(messages, &msg)
// 				}
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	return messages, err
// }

// func (s *Store) GetNextSequence(ctx context.Context, queueName string, partitionID int) (uint64, error) {
// 	key := makeSeqKey(queueName, partitionID)
// 	var seq uint64

// 	err := s.db.Update(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(key))
// 		if err == badger.ErrKeyNotFound {
// 			seq = 1
// 		} else if err != nil {
// 			return err
// 		} else {
// 			err = item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &seq)
// 			})
// 			if err != nil {
// 				return err
// 			}
// 			seq++
// 		}

// 		// Update sequence
// 		data, err := json.Marshal(seq)
// 		if err != nil {
// 			return err
// 		}
// 		return txn.Set([]byte(key), data)
// 	})

// 	return seq, err
// }

// func (s *Store) UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error {
// 	key := makeOffsetKey(queueName, partitionID)
// 	data, err := json.Marshal(offset)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Set([]byte(key), data)
// 	})
// }

// func (s *Store) GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error) {
// 	key := makeOffsetKey(queueName, partitionID)
// 	var offset uint64

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(key))
// 		if err == badger.ErrKeyNotFound {
// 			offset = 0
// 			return nil
// 		}
// 		if err != nil {
// 			return err
// 		}

// 		return item.Value(func(val []byte) error {
// 			return json.Unmarshal(val, &offset)
// 		})
// 	})

// 	return offset, err
// }

// func (s *Store) ListQueued(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
// 	messages := make([]*types.Message, 0, limit)
// 	prefix := makePartitionPrefix(queueName, partitionID)

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		count := 0
// 		for it.Rewind(); it.Valid() && (limit == 0 || count < limit); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var msg types.Message
// 				if err := json.Unmarshal(val, &msg); err != nil {
// 					return err
// 				}
// 				if msg.State == types.StateQueued || msg.State == types.StateRetry {
// 					messages = append(messages, &msg)
// 					count++
// 				}
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	return messages, err
// }

// // ConsumerStore implementation

// func (s *Store) RegisterConsumer(ctx context.Context, consumer *types.Consumer) error {
// 	key := makeConsumerKey(consumer.QueueName, consumer.GroupID, consumer.ID)
// 	data, err := json.Marshal(consumer)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Set([]byte(key), data)
// 	})
// }

// func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
// 	key := makeConsumerKey(queueName, groupID, consumerID)
// 	return s.db.Update(func(txn *badger.Txn) error {
// 		return txn.Delete([]byte(key))
// 	})
// }

// func (s *Store) GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*types.Consumer, error) {
// 	key := makeConsumerKey(queueName, groupID, consumerID)
// 	var consumer types.Consumer

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(key))
// 		if err != nil {
// 			if err == badger.ErrKeyNotFound {
// 				return storage.ErrConsumerNotFound
// 			}
// 			return err
// 		}

// 		return item.Value(func(val []byte) error {
// 			return json.Unmarshal(val, &consumer)
// 		})
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &consumer, nil
// }

// func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.Consumer, error) {
// 	consumers := make([]*types.Consumer, 0)
// 	prefix := queueConsumerPrefix + queueName + ":" + groupID + ":"

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var consumer types.Consumer
// 				if err := json.Unmarshal(val, &consumer); err != nil {
// 					return err
// 				}
// 				consumers = append(consumers, &consumer)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	return consumers, err
// }

// func (s *Store) ListGroups(ctx context.Context, queueName string) ([]string, error) {
// 	groupMap := make(map[string]bool)
// 	prefix := queueConsumerPrefix + queueName + ":"

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			err := item.Value(func(val []byte) error {
// 				var consumer types.Consumer
// 				if err := json.Unmarshal(val, &consumer); err != nil {
// 					return err
// 				}
// 				groupMap[consumer.GroupID] = true
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})

// 	groups := make([]string, 0, len(groupMap))
// 	for group := range groupMap {
// 		groups = append(groups, group)
// 	}

// 	return groups, err
// }

// func (s *Store) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error {
// 	consumer, err := s.GetConsumer(ctx, queueName, groupID, consumerID)
// 	if err != nil {
// 		return err
// 	}

// 	consumer.LastHeartbeat = timestamp
// 	return s.RegisterConsumer(ctx, consumer)
// }

// // Helper functions for key construction

// func makeMessageKey(queueName string, partitionID int, sequence uint64) string {
// 	return fmt.Sprintf("%s%s:%d:%020d", queueMessagePrefix, queueName, partitionID, sequence)
// }

// func makePartitionPrefix(queueName string, partitionID int) string {
// 	return fmt.Sprintf("%s%s:%d:", queueMessagePrefix, queueName, partitionID)
// }

// func makeInflightKey(queueName, messageID, groupID string) string {
// 	return queueInflightPrefix + queueName + ":" + messageID + ":" + groupID
// }

// func makeDLQKey(dlqTopic, messageID string) string {
// 	// Remove $queue/dlq/ prefix if present
// 	topic := strings.TrimPrefix(dlqTopic, "$queue/dlq/")
// 	return queueDLQPrefix + topic + ":" + messageID
// }

// // incrementCounter atomically increments/decrements a counter in BadgerDB.
// // delta can be positive (increment) or negative (decrement).
// func (s *Store) incrementCounter(txn *badger.Txn, key string, delta int64) error {
// 	// Read current value
// 	var currentValue int64
// 	item, err := txn.Get([]byte(key))
// 	if err == nil {
// 		err = item.Value(func(val []byte) error {
// 			if len(val) == 8 {
// 				currentValue = int64(binary.BigEndian.Uint64(val))
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			return err
// 		}
// 	} else if err != badger.ErrKeyNotFound {
// 		return err
// 	}

// 	// Calculate new value
// 	newValue := currentValue + delta
// 	if newValue < 0 {
// 		newValue = 0 // Never go negative
// 	}

// 	// Write new value
// 	buf := make([]byte, 8)
// 	binary.BigEndian.PutUint64(buf, uint64(newValue))
// 	return txn.Set([]byte(key), buf)
// }

// func makeConsumerKey(queueName, groupID, consumerID string) string {
// 	return queueConsumerPrefix + queueName + ":" + groupID + ":" + consumerID
// }

// func makeSeqKey(queueName string, partitionID int) string {
// 	return fmt.Sprintf("%s%s:%d", queueSeqPrefix, queueName, partitionID)
// }

// func makeOffsetKey(queueName string, partitionID int) string {
// 	return fmt.Sprintf("%s%s:%d", queueOffsetPrefix, queueName, partitionID)
// }

// // Retention operations

// func (s *Store) ListOldestMessages(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
// 	prefix := makePartitionPrefix(queueName, partitionID)
// 	var messages []*types.Message

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)
// 		opts.PrefetchValues = true

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		// Messages are already sorted by sequence due to key format
// 		for it.Rewind(); it.Valid(); it.Next() {
// 			if limit > 0 && len(messages) >= limit {
// 				break
// 			}

// 			item := it.Item()
// 			var msg types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &msg)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			messages = append(messages, &msg)
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	return messages, nil
// }

// func (s *Store) ListMessagesBefore(ctx context.Context, queueName string, partitionID int, cutoffTime time.Time, limit int) ([]*types.Message, error) {
// 	prefix := makePartitionPrefix(queueName, partitionID)
// 	var messages []*types.Message

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)
// 		opts.PrefetchValues = true

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			if limit > 0 && len(messages) >= limit {
// 				break
// 			}

// 			item := it.Item()
// 			var msg types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &msg)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			// Filter by creation time
// 			if msg.CreatedAt.Before(cutoffTime) {
// 				messages = append(messages, &msg)
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	return messages, nil
// }

// func (s *Store) DeleteMessageBatch(ctx context.Context, queueName string, messageIDs []string) (int64, error) {
// 	if len(messageIDs) == 0 {
// 		return 0, nil
// 	}

// 	// Create a set for fast ID lookup
// 	idSet := make(map[string]struct{}, len(messageIDs))
// 	for _, id := range messageIDs {
// 		idSet[id] = struct{}{}
// 	}

// 	// Get queue config to know partition count
// 	queueConfig, err := s.GetQueue(ctx, queueName)
// 	if err != nil {
// 		return 0, err
// 	}

// 	var deletedCount int64

// 	// Scan all partitions to find and delete messages by ID
// 	err = s.db.Update(func(txn *badger.Txn) error {
// 		keysToDelete := make([][]byte, 0)

// 		// Scan each partition
// 		for partitionID := 0; partitionID < queueConfig.Partitions; partitionID++ {
// 			prefix := makePartitionPrefix(queueName, partitionID)
// 			opts := badger.DefaultIteratorOptions
// 			opts.Prefix = []byte(prefix)
// 			opts.PrefetchValues = true

// 			it := txn.NewIterator(opts)

// 			for it.Rewind(); it.Valid(); it.Next() {
// 				item := it.Item()
// 				var msg types.Message
// 				err := item.Value(func(val []byte) error {
// 					return json.Unmarshal(val, &msg)
// 				})
// 				if err != nil {
// 					continue
// 				}

// 				// Check if this message should be deleted
// 				if _, found := idSet[msg.ID]; found {
// 					keysToDelete = append(keysToDelete, item.KeyCopy(nil))
// 					deletedCount++
// 				}
// 			}
// 			it.Close()
// 		}

// 		// Delete all found messages
// 		for _, key := range keysToDelete {
// 			if err := txn.Delete(key); err != nil {
// 				return err
// 			}
// 		}

// 		// Update count
// 		if deletedCount > 0 {
// 			counterKey := queueCountPrefix + queueName
// 			return s.incrementCounter(txn, counterKey, -deletedCount)
// 		}

// 		return nil
// 	})
// 	if err != nil {
// 		return 0, err
// 	}

// 	return deletedCount, nil
// }

// func (s *Store) GetQueueSize(ctx context.Context, queueName string) (int64, error) {
// 	// Get queue config to know partition count
// 	queueConfig, err := s.GetQueue(ctx, queueName)
// 	if err != nil {
// 		return 0, err
// 	}

// 	var totalSize int64

// 	err = s.db.View(func(txn *badger.Txn) error {
// 		// Scan each partition
// 		for partitionID := 0; partitionID < queueConfig.Partitions; partitionID++ {
// 			prefix := makePartitionPrefix(queueName, partitionID)
// 			opts := badger.DefaultIteratorOptions
// 			opts.Prefix = []byte(prefix)
// 			opts.PrefetchValues = true

// 			it := txn.NewIterator(opts)
// 			defer it.Close()

// 			for it.Rewind(); it.Valid(); it.Next() {
// 				item := it.Item()
// 				var msg types.Message
// 				err := item.Value(func(val []byte) error {
// 					return json.Unmarshal(val, &msg)
// 				})
// 				if err != nil {
// 					continue
// 				}

// 				totalSize += int64(len(msg.GetPayload()))
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return 0, err
// 	}

// 	return totalSize, nil
// }

// func (s *Store) ListAllMessages(ctx context.Context, queueName string, partitionID int) ([]*types.Message, error) {
// 	messages := make([]*types.Message, 0)
// 	prefix := makePartitionPrefix(queueName, partitionID)

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.Prefix = []byte(prefix)
// 		opts.PrefetchValues = true

// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			var msg types.Message
// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &msg)
// 			})
// 			if err != nil {
// 				continue
// 			}

// 			msgCopy := msg
// 			messages = append(messages, &msgCopy)
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Sort by sequence (oldest first) - already sorted by key, but make sure
// 	sort.Slice(messages, func(i, j int) bool {
// 		return messages[i].Sequence < messages[j].Sequence
// 	})

// 	return messages, nil
// }
