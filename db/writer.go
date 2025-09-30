package db

import (
	"github.com/dgraph-io/badger/v4"
)

type Writer struct {
	bw *badger.WriteBatch
}

func (w *Writer) Write(key string, data []byte) error {
	return w.bw.Set([]byte(key), data)
}

func (w *Writer) Commit() error {
	return w.bw.Flush()
}

func (w *Writer) Close() {
	w.bw.Cancel()
}
