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
)

type Server struct {
	logger *logger.Logger
	config config.ServerConfig
}

func New(c config.ServerConfig, dbCtx *db.DbContext) *Server {
	return &Server{
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

	// TODO: Create TCP listener
	s.logger.Infof("Server stopped")

	return nil
}

func (s *Server) Stop() {
	logger.Server().Infof("Stopping server...")
	// TODO: Stop TCP listener
}

func notifySignal(s *Server) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	s.logger.Infof("%s", sig)

	s.Stop()
}
