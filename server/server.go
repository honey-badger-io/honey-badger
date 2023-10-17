package server

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	grpc   *grpc.Server
	logger *logger.Logger
	config config.ServerConfig
}

func New(c config.ServerConfig, dbCtx *db.DbContext) *Server {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(1024 * 1024 * c.MaxRecvMsgSizeMb),
	}

	grpcServer := grpc.NewServer(opts...)

	pb.RegisterDataServer(grpcServer, &DataServer{
		dbCtx: dbCtx,
	})
	pb.RegisterDbServer(grpcServer, &DbServer{
		dbCtx: dbCtx,
	})
	pb.RegisterSysServer(grpcServer, &SysServer{})

	reflection.Register(grpcServer)

	return &Server{
		grpc:   grpcServer,
		logger: logger.Server(),
		config: c,
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return err
	}

	//https://dgraph.io/docs/badger/faq/#are-there-any-go-specific-settings-that-i-should-use
	runtime.GOMAXPROCS(128)

	go notifySignal(s)

	s.logger.Infof("Server listening at %v", lis.Addr())

	if err := s.grpc.Serve(lis); err != nil {
		return err
	}

	s.logger.Infof("Server stopped")

	return nil
}

func (s *Server) Stop() {
	logger.Server().Infof("Stopping server...")
	s.grpc.GracefulStop()
}

func notifySignal(s *Server) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	s.logger.Infof("%s", sig)

	s.Stop()
}
