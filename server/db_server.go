package server

import (
	"context"

	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/pb"
)

type DbServer struct {
	pb.UnimplementedDbServer

	dbCtx *db.DbContext
}

func (s *DbServer) Create(ctx context.Context, in *pb.CreateDbRequest) (*pb.EmptyResult, error) {
	_, err := s.dbCtx.CreateDb(in.Name, in.InMemory)
	if err != nil {
		return nil, err
	}

	return &pb.EmptyResult{}, nil
}

func (s *DbServer) Drop(ctx context.Context, in *pb.DropDbRequest) (*pb.EmptyResult, error) {
	if err := s.dbCtx.DropDb(in.Name); err != nil {
		return nil, err
	}

	return &pb.EmptyResult{}, nil
}
