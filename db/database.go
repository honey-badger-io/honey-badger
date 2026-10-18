package db

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Database struct {
	b *badger.DB
}

type DbStats struct {
	Lsm           int64
	Vlog          int64
	InMemory      bool
	KeyCount      uint32
	Size          int64
	OnDiskSize    uint32
	StaleDataSize uint32
	Metrics       string
}

type DbMetrics struct {
	KeysAdded uint64
}

func (db *Database) Get(key string) ([]byte, bool, error) {
	txn := db.b.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(key))

	if err == badger.ErrKeyNotFound {
		return make([]byte, 0), false, nil
	}

	if err != nil {
		return nil, false, err
	}

	value, err := item.ValueCopy(nil)

	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

func (db *Database) Stats() DbStats {
	lsm, vlog := db.b.Size()
	options := db.b.Opts()
	metrics := db.b.BlockCacheMetrics()

	stats := DbStats{
		Lsm:      lsm,
		Vlog:     vlog,
		InMemory: options.InMemory,
		Metrics:  metrics.String(),
	}

	for _, t := range db.b.Tables() {
		stats.KeyCount += t.KeyCount
		stats.OnDiskSize += t.OnDiskSize
		stats.StaleDataSize += t.StaleDataSize
	}

	for _, l := range db.b.Levels() {
		stats.Size += l.Size
	}

	return stats
}

func (db *Database) Set(key string, data []byte, ttl uint) error {
	return db.b.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data)

		if ttl > 0 {
			entry = entry.WithTTL(time.Duration(ttl) * time.Second)
		}

		return txn.SetEntry(entry)
	})
}

func (db *Database) Sync() error {
	// Cannot sync in memory databases
	if db.b.Opts().InMemory {
		return nil
	}

	return db.b.Sync()
}

// DeleteByKey deletes the key and reports whether it existed
func (db *Database) DeleteByKey(key string) (bool, error) {
	for {
		deleted := false
		err := db.b.Update(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(key))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}

			if err != nil {
				return err
			}

			deleted = true
			return txn.Delete([]byte(key))
		})

		// The key was written after we read it, so read it again
		if errors.Is(err, badger.ErrConflict) {
			continue
		}

		return deleted, err
	}
}

func (db *Database) DeleteByPrefix(prefix string) error {
	return db.b.DropPrefix([]byte(prefix))
}

func (db *Database) NewWriter() *Writer {
	return &Writer{
		bw: db.b.NewWriteBatch(),
	}
}
