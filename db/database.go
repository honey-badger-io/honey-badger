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
	TagDelimiter   string = "_tag_"
	ItemTagsPrefix string = "_tags_"
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

func (db *Database) GetTags(key string, txn *badger.Txn) ([]string, error) {
	tags := make([]string, 0)

	deleteFunc := func(t *badger.Txn) error {
		item, err := t.Get([]byte(ItemTagsPrefix + key))
		if err == badger.ErrKeyNotFound {
			return nil
		}

		return item.Value(func(val []byte) error {
			tags = strings.Split(string(val), ",")
			return nil
		})
	}

	if txn != nil {
		if err := deleteFunc(txn); err != nil {
			return tags, err
		}

		return tags, nil
	}

	err := db.b.View(deleteFunc)

	return tags, err
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

		// Create tag entries
		for _, tag := range tags {
			// Tag entry has key format tagName_tag_itemKey
			tagEntry := badger.NewEntry([]byte(tag+TagDelimiter+key), make([]byte, 0))

			if ttl > 0 {
				tagEntry = tagEntry.WithTTL(time.Duration(ttl) * time.Second)
			}

			if err := txn.SetEntry(tagEntry); err != nil {
				return err
			}
		}

		// Create entry with tags list
		if len(tags) > 0 {
			itemTagsKey := []byte(ItemTagsPrefix + key)
			itemTagsValue := []byte(strings.Join(tags, ","))

			itemTagsEntry := badger.NewEntry(itemTagsKey, itemTagsValue)

			if ttl > 0 {
				itemTagsEntry = itemTagsEntry.WithTTL(time.Duration(ttl) * time.Second)
			}

			if err := txn.SetEntry(itemTagsEntry); err != nil {
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

		tags, err := db.GetTags(key, txn)
		if err != nil {
			return err
		}

		for _, tag := range tags {
			if err := txn.Delete([]byte(tag + TagDelimiter + key)); err != nil {
				return err
			}
		}

		return txn.Delete([]byte(key))
	})
}

func (db *Database) DeleteByPrefix(prefix string) error {
	return db.b.Update(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = []byte(prefix)

		itr := txn.NewIterator(options)
		defer itr.Close()

		for itr.Rewind(); itr.Valid(); itr.Next() {
			key := itr.Item().Key()
			keyStr := string(key)

			if err := txn.Delete(key); err != nil {
				return err
			}

			tags, err := db.GetTags(string(key), txn)
			if err != nil {
				return err
			}

			for _, tag := range tags {
				if err := txn.Delete([]byte(tag + TagDelimiter + keyStr)); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (db *Database) DeleteByTag(tag string) error {
	return db.b.Update(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = []byte(tag + TagDelimiter)

		itr := txn.NewIterator(options)
		defer itr.Close()

		for itr.Rewind(); itr.Valid(); itr.Next() {
			tagEntryKey := itr.Item().Key()

			// Tag entry has key format tagName_tag_itemKey
			_, childKey, _ := strings.Cut(string(tagEntryKey), TagDelimiter)

			// Deletes the entry
			if err := txn.Delete([]byte(childKey)); err != nil {
				return err
			}

			// Get all tags of the item
			itemTags, err := db.GetTags(childKey, txn)
			if err != nil {
				return nil
			}

			for _, tag := range itemTags {
				tagEntryKey := tag + TagDelimiter + childKey

				if err := txn.Delete([]byte(tagEntryKey)); err != nil {
					return err
				}
			}
		}

		return nil
	})
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

		for _, tagEntry := range list.Kv {
			// Tag entry has key format tagName_tag_itemKey
			_, key, found := strings.Cut(string(tagEntry.Key), TagDelimiter)
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
