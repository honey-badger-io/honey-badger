package server

import (
	"context"
	"errors"
	"io"

	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DataServer struct {
	pb.UnimplementedDataServer

	dbCtx *db.DbContext
}

func (s *DataServer) Set(ctx context.Context, in *pb.SetRequest) (*emptypb.Empty, error) {
	db, err := s.dbCtx.GetDb(in.Db)
	if err != nil {
		return nil, err
	}

	var ttl uint = 0
	if in.Ttl != nil {
		ttl = uint(*in.Ttl)
	}

	err = db.Set(in.Key, in.Data, ttl)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *DataServer) Get(ctx context.Context, in *pb.KeyRequest) (*pb.GetResult, error) {
	db, err := s.dbCtx.GetDb(in.Db)
	if err != nil {
		return nil, err
	}

	data, hit, err := db.Get(in.Key)
	if err != nil {
		return nil, err
	}

	return &pb.GetResult{Data: data, Hit: hit}, nil
}

func (s *DataServer) Delete(ctx context.Context, in *pb.KeyRequest) (*emptypb.Empty, error) {
	db, err := s.dbCtx.GetDb(in.Db)
	if err != nil {
		return nil, err
	}

	if err := db.DeleteByKey(in.Key); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *DataServer) DeleteByPrefix(ctx context.Context, in *pb.PrefixRequest) (*emptypb.Empty, error) {
	db, err := s.dbCtx.GetDb(in.Db)
	if err != nil {
		return nil, err
	}

	if err := db.DeleteByPrefix(in.Prefix); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *DataServer) CreateReadStream(in *pb.ReadStreamReq, stream pb.Data_CreateReadStreamServer) error {
	db, err := s.dbCtx.GetDb(in.Db)
	if err != nil {
		return err
	}

	if in.Prefix == nil {
		return nil
	}

	return db.ReadDataByPrefix(stream.Context(), *in.Prefix, stream.Send)
}

func (s *DataServer) CreateSendStream(stream pb.Data_CreateSendStreamServer) error {
	dbItem, err := stream.Recv()
	if err != nil {
		return err
	}

	if dbItem.Db == "" {
		return errors.New("invalid db: database should be in first message")
	}

	db, err := s.dbCtx.GetDb(dbItem.Db)
	if err != nil {
		return err
	}

	writer := db.NewWriter()
	defer writer.Close()

	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := writer.Write(item.Item); err != nil {
			return err
		}
	}

	if err := writer.Commit(); err != nil {
		return err
	}

	return stream.SendAndClose(&emptypb.Empty{})
}
