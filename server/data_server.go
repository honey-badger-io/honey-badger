package server

import (
	"context"

	"github.com/honey-badger-io/honey-badger/db"
)

const (
	Db0 = "db0"
)

type DataServer struct {
	dbCtx *db.DbContext
}

func (s *DataServer) Set(ctx context.Context, key string, data []byte, ttl uint) error {
	db, err := s.dbCtx.GetDb(Db0)
	if err != nil {
		return err
	}

	err = db.Set(key, data, ttl)
	if err != nil {
		return err
	}

	return nil
}

func (s *DataServer) Get(ctx context.Context, key string) ([]byte, bool, error) {
	db, err := s.dbCtx.GetDb(Db0)
	if err != nil {
		return nil, false, err
	}

	return db.Get(key)
}

func (s *DataServer) Delete(ctx context.Context, key string) error {
	db, err := s.dbCtx.GetDb(Db0)
	if err != nil {
		return err
	}

	if err := db.DeleteByKey(key); err != nil {
		return err
	}

	return nil
}

func (s *DataServer) DeleteByPrefix(ctx context.Context, prefix string) error {
	db, err := s.dbCtx.GetDb(Db0)
	if err != nil {
		return err
	}

	if err := db.DeleteByPrefix(prefix); err != nil {
		return err
	}

	return nil
}
