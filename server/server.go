package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/resp"
)

type Server struct {
	logger    *logger.Logger
	listener  net.Listener
	config    *config.Config
	connCount int
	version   string
}

func New(c *config.Config, version string) *Server {
	//https://dgraph.io/docs/badger/faq/#are-there-any-go-specific-settings-that-i-should-use
	runtime.GOMAXPROCS(128)

	return &Server{
		logger:  logger.Server(),
		config:  c,
		version: version,
	}
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.config.Server.Port))
	if err != nil {
		return err
	}

	go notifySignal(s)

	s.logger.Infof("Server listening at %v", s.listener.Addr())

	for {
		conn, err := s.listener.Accept()

		if errors.Is(err, net.ErrClosed) {
			break
		}

		if err != nil {
			s.logger.Error(err)
			continue
		}

		// New connections always use db0
		db0, err := db.OpenDb("db0", s.config.Badger.InMemory)
		if err != nil {
			s.logger.Error(err)
			continue
		}

		s.connCount++

		respSession := resp.NewSession(s.connCount, conn, db0, s.version)
		go respSession.Handle()
	}

	s.logger.Infof("Server stopped")
	return nil
}

func (s *Server) Stop() {
	logger.Server().Infof("Stopping server...")
	s.listener.Close()
}

func notifySignal(s *Server) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	s.logger.Infof("%s", sig)

	s.Stop()
}
