package db

import (
	"context"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/z"
	hpb "github.com/honey-badger-io/honey-badger/pb"
)

const (
	UserMetaTag  byte   = 255
	TagDelimiter string = "_tag_"
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

type ReadDataClbk func(*hpb.DataItem) error

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

func (db *Database) Set(key string, data []byte, ttl uint, tags []string) error {
	return db.b.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data)

		if ttl > 0 {
			entry = entry.WithTTL(time.Duration(ttl) * time.Second)
		}

		for _, tag := range tags {
			tagEntry := badger.NewEntry([]byte(tag+TagDelimiter+key), make([]byte, 0))
			tagEntry = tagEntry.WithMeta(UserMetaTag)

			if ttl > 0 {
				tagEntry = tagEntry.WithTTL(time.Duration(ttl) * time.Second)
			}

			if err := txn.SetEntry(tagEntry); err != nil {
				return err
			}
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

func (db *Database) DeleteByKey(key string) error {
	return db.b.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (db *Database) DeleteByPrefix(prefix string) error {
	return db.b.DropPrefix([]byte(prefix))
}

func (db *Database) NewWriter() *Writer {
	return &Writer{
		bw: db.b.NewWriteBatch(),
	}
}

func (db *Database) ReadDataByPrefix(ctx context.Context, prefix string, callback ReadDataClbk) error {
	stream := db.b.NewStream()

	stream.LogPrefix = "ReadDataByPrefix"
	stream.Prefix = []byte(prefix)
	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		for _, kv := range list.Kv {
			item := hpb.DataItem{
				Key:  string(kv.Key),
				Data: kv.Value,
			}
			if err := callback(&item); err != nil {
				return err
			}
		}

		return nil
	}

	return stream.Orchestrate(ctx)
}

func (db *Database) ReadDataByTag(ctx context.Context, tag string, callback ReadDataClbk) error {
	stream := db.b.NewStream()
	txn := db.b.NewTransaction(false)
	defer txn.Discard()

	stream.LogPrefix = "ReadDataByTag"
	stream.Prefix = []byte(tag + TagDelimiter)
	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		for _, kv := range list.Kv {
			_, key, found := strings.Cut(string(kv.Key), TagDelimiter)
			if !found {
				continue
			}

			itm, err := txn.Get([]byte(key))
			if err == badger.ErrKeyNotFound {
				continue
			}

			if err != nil {
				return err
			}

			data, err := itm.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err != nil {
				return err
			}

			item := hpb.DataItem{
				Key:  key,
				Data: data,
			}
			if err := callback(&item); err != nil {
				return err
			}
		}

		return nil
	}

	return stream.Orchestrate(ctx)
}
