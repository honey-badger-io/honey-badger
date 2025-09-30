package server

import (
	"context"

	"github.com/honey-badger-io/honey-badger/db"
)

type DbServer struct {
	dbCtx *db.DbContext
}

func (s *DbServer) Create(ctx context.Context, name string) error {
	_, err := s.dbCtx.CreateDb(name, true)
	if err != nil {
		return err
	}

	return nil
}

func (s *DbServer) Drop(ctx context.Context, name string) error {
	if err := s.dbCtx.DropDb(name); err != nil {
		return err
	}

	return nil
}

func (s *DbServer) Exists(ctx context.Context, name string) bool {
	return s.dbCtx.Exists(name)
}

func (s *DbServer) EnsureDb(ctx context.Context, name string) error {
	if s.dbCtx.Exists(name) {
		return nil
	}

	return s.Create(ctx, name)
}
