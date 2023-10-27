package db

import (
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/honey-badger-io/honey-badger/pb"
)

type Writer struct {
	bw *badger.WriteBatch
}

func (w *Writer) Write(item *pb.DataItem) error {
	// Create tag entries
	for _, tag := range item.Tags {
		// Tag entry has key format tagName_tag_itemKey
		if err := w.bw.Set([]byte(tag+TagDelimiter+item.Key), make([]byte, 0)); err != nil {
			return err
		}
	}

	// Create entry with tags list
	if len(item.Tags) > 0 {
		itemTagsKey := []byte(ItemTagsPrefix + item.Key)
		itemTagsValue := []byte(strings.Join(item.Tags, ","))

		if err := w.bw.Set(itemTagsKey, itemTagsValue); err != nil {
			return err
		}
	}

	return w.bw.Set([]byte(item.Key), item.Data)
}

func (w *Writer) Commit() error {
	return w.bw.Flush()
}

func (w *Writer) Close() {
	w.bw.Cancel()
}
