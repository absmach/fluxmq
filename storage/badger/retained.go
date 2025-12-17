package badger

import (
	"encoding/json"
	"fmt"

	"github.com/absmach/mqtt/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.RetainedStore = (*RetainedStore)(nil)

// RetainedStore implements storage.RetainedStore using BadgerDB.
//
// Key format: retained:{topic}
type RetainedStore struct {
	db *badger.DB
}

// NewRetainedStore creates a new BadgerDB retained message store.
func NewRetainedStore(db *badger.DB) *RetainedStore {
	return &RetainedStore{db: db}
}

// Set stores or updates a retained message.
// Empty payload deletes the retained message.
func (r *RetainedStore) Set(topic string, msg *storage.Message) error {
	// Empty payload means delete
	if len(msg.Payload) == 0 {
		return r.Delete(topic)
	}

	key := []byte("retained:" + topic)
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal retained message: %w", err)
	}

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Get retrieves a retained message by exact topic.
func (r *RetainedStore) Get(topic string) (*storage.Message, error) {
	key := []byte("retained:" + topic)
	var msg *storage.Message

	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			msg = &storage.Message{}
			return json.Unmarshal(val, msg)
		})
	})

	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Delete removes a retained message.
func (r *RetainedStore) Delete(topic string) error {
	key := []byte("retained:" + topic)

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Match returns all retained messages matching a filter (supports wildcards).
func (r *RetainedStore) Match(filter string) ([]*storage.Message, error) {
	var matched []*storage.Message

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("retained:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			// Extract topic from key (remove "retained:" prefix)
			topic := key[len("retained:"):]

			// Check if topic matches the filter
			if topicMatchesFilter(topic, filter) {
				err := item.Value(func(val []byte) error {
					var msg storage.Message
					if err := json.Unmarshal(val, &msg); err != nil {
						return err
					}
					matched = append(matched, &msg)
					return nil
				})

				if err != nil {
					return fmt.Errorf("failed to unmarshal retained message: %w", err)
				}
			}
		}

		return nil
	})

	return matched, err
}
