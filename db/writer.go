package db

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/honey-badger-io/honey-badger/pb"
)

type Writer struct {
	bw *badger.WriteBatch
}

func (w *Writer) Write(item *pb.DataItem) error {
	return w.bw.Set([]byte(item.Key), item.Data)
}

func (w *Writer) Commit() error {
	return w.bw.Flush()
}

func (w *Writer) Close() {
	w.bw.Cancel()
}
