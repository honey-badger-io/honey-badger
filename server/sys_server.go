package server

import (
	"context"

	"github.com/honey-badger-io/honey-badger/pb"
)

type SysServer struct {
	pb.UnimplementedSysServer
}

func (s *SysServer) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingResult, error) {
	return &pb.PingResult{Mesage: "pong"}, nil
}
