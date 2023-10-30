package server

import (
	"context"

	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DbServer struct {
	pb.UnimplementedDbServer

	dbCtx *db.DbContext
}

func (s *DbServer) Create(ctx context.Context, in *pb.CreateDbReq) (*emptypb.Empty, error) {
	_, err := s.dbCtx.CreateDb(in.Name, in.Opt.InMemory)
	if err != nil {
		return &emptypb.Empty{}, err
	}

	return &emptypb.Empty{}, nil
}

func (s *DbServer) Drop(ctx context.Context, in *pb.DropDbRequest) (*emptypb.Empty, error) {
	if err := s.dbCtx.DropDb(in.Name); err != nil {
		return &emptypb.Empty{}, err
	}

	return &emptypb.Empty{}, nil
}

func (s *DbServer) Exists(ctx context.Context, in *pb.ExistsDbReq) (*pb.ExistsDbRes, error) {
	return &pb.ExistsDbRes{
		Exists: s.dbCtx.Exists(in.Name),
	}, nil
}

func (s *DbServer) EnsureDb(ctx context.Context, in *pb.CreateDbReq) (*emptypb.Empty, error) {
	if s.dbCtx.Exists(in.Name) {
		return &emptypb.Empty{}, nil
	}

	return s.Create(ctx, in)
}

func (s *DbServer) List(ctx context.Context, _ *emptypb.Empty) (*pb.DbListRes, error) {
	dbs := s.dbCtx.List()
	result := make([]*pb.DbListItem, len(dbs))

	for i, db := range dbs {
		stats := db.Stats()

		result[i] = &pb.DbListItem{
			Name:     db.Name,
			InMemory: stats.InMemory,
		}
	}

	return &pb.DbListRes{
		Dbs: result,
	}, nil
}
