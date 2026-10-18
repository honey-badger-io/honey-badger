package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/resp"
)

// How long sessions get to send replies to commands already received once the server is stopping
const shutdownTimeout = 5 * time.Second

type Server struct {
	logger    *logger.Logger
	listener  net.Listener
	config    *config.Config
	connCount int
	version   string

	// Guards listener, conns and stopping, which Stop uses from another goroutine
	mux      sync.Mutex
	conns    map[int]net.Conn
	stopping bool
	sessions sync.WaitGroup
}

func New(c *config.Config, version string) *Server {
	return &Server{
		logger:  logger.Server(),
		config:  c,
		version: version,
		conns:   make(map[int]net.Conn),
	}
}

// Start serves connections until Stop is called, then returns once every session has ended
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Server.Port))
	if err != nil {
		return err
	}

	s.mux.Lock()
	s.listener = listener
	s.mux.Unlock()

	go notifySignal(s)

	s.logger.Infof("Server listening at %v", listener.Addr())

	for {
		conn, err := listener.Accept()

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
			conn.Close()
			continue
		}

		s.connCount++
		id := s.connCount

		if !s.track(id, conn) {
			conn.Close()
			continue
		}

		respSession := resp.NewSession(id, conn, db0, s.version)
		s.sessions.Go(func() {
			defer s.untrack(id)
			respSession.Handle()
		})
	}

	s.sessions.Wait()

	s.logger.Infof("Server stopped")
	return nil
}

// Stop stops accepting connections and ends every session after it has replied to the commands it already received
func (s *Server) Stop() {
	logger.Server().Infof("Stopping server...")

	s.mux.Lock()
	defer s.mux.Unlock()

	s.stopping = true

	// Unblocks sessions waiting for their next command, and bounds how long replies may take
	for _, conn := range s.conns {
		_ = conn.SetReadDeadline(time.Now())
		_ = conn.SetWriteDeadline(time.Now().Add(shutdownTimeout))
	}

	if s.listener != nil {
		s.listener.Close()
	}
}

// track registers a session's connection, so Stop can end it. It returns false once the server is stopping.
func (s *Server) track(id int, conn net.Conn) bool {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.stopping {
		return false
	}

	s.conns[id] = conn
	return true
}

func (s *Server) untrack(id int) {
	s.mux.Lock()
	defer s.mux.Unlock()

	delete(s.conns, id)
}

func notifySignal(s *Server) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	s.logger.Infof("%s", sig)

	// A second signal kills the process right away if the shutdown hangs
	signal.Stop(signalChannel)

	s.Stop()
}
