package server

import (
	"context"
)

type SysServer struct {
}

func (s *SysServer) Ping(ctx context.Context) error {
	return nil
}
