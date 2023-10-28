package db

import (
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/honey-badger-io/honey-badger/pb"
)

type Writer struct {
	bw *badger.WriteBatch
}

func (w *Writer) Write(item *pb.DataItem) error {
	wg := new(sync.WaitGroup)
	wg.Add(2) // We know we have to wait for 2  goroutines

	go createTagEntries(w, item, wg)
	go createTagsList(w, item, wg)

	wg.Wait()
	return w.bw.Set([]byte(item.Key), item.Data)
}

func (w *Writer) Commit() error {
	return w.bw.Flush()
}

func (w *Writer) Close() {
	w.bw.Cancel()
}

func createTagEntries(w *Writer, item *pb.DataItem, wg *sync.WaitGroup) error {
	defer wg.Done()

	// Create tag entries
	for _, tag := range item.Tags {
		// Tag entry has key format tagName_tag_itemKey
		if err := w.bw.Set([]byte(tag+TagDelimiter+item.Key), make([]byte, 0)); err != nil {
			return err
		}
	}

	return nil
}

func createTagsList(w *Writer, item *pb.DataItem, wg *sync.WaitGroup) error {
	defer wg.Done()

	// Create entry with tags list
	if len(item.Tags) > 0 {
		itemTagsKey := []byte(ItemTagsPrefix + item.Key)
		itemTagsValue := []byte(strings.Join(item.Tags, ","))

		if err := w.bw.Set(itemTagsKey, itemTagsValue); err != nil {
			return err
		}
	}

	return nil
}
