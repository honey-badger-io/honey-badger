package server

import (
	"errors"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/resp"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

// How long sessions get to send replies to commands already received once the server is stopping
const shutdownTimeout = 5 * time.Second

type Server struct {
	logger    *logger.Logger
	listener  net.Listener
	config    *config.Config
	connCount int
	version   string

	// Guards listener, sessions and stopping, which Stop uses from another goroutine
	mux      sync.Mutex
	sessions map[int]*resp.Session
	stopping bool
	running  sync.WaitGroup
}

var (
	errStopping   = errors.New("server is stopping")
	errMaxClients = common.NewRespError("max number of clients reached")
)

func New(c *config.Config, version string) *Server {
	return &Server{
		logger:   logger.Server(),
		config:   c,
		version:  version,
		sessions: make(map[int]*resp.Session),
	}
}

// Start serves connections until Stop is called, then returns once every session has ended
func (s *Server) Start() error {
	addr := net.JoinHostPort(s.config.Server.Bind, strconv.Itoa(int(s.config.Server.Port)))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	if s.config.Server.Password == "" && !isLoopback(listener.Addr()) {
		s.logger.Warningf("Listening on %v without %s set, so anyone who can reach this address has full access", listener.Addr(), config.EnvPassword)
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

		respSession := resp.NewSession(id, conn, db0, s.version, resp.Options{
			Password: s.config.Server.Password,
			Timeout:  s.config.Server.Timeout,
		})

		if err := s.track(id, respSession); err != nil {
			if errors.Is(err, errMaxClients) {
				// Like Redis, tell the client why before closing. The deadline keeps a client that doesn't read from blocking Accept.
				_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
				_, _ = conn.Write([]byte(errMaxClients.Error()))
			}

			conn.Close()
			continue
		}

		s.running.Go(func() {
			defer s.untrack(id)
			respSession.Handle()
		})
	}

	s.running.Wait()

	s.logger.Infof("Server stopped")
	return nil
}

// Stop stops accepting connections and ends every session after it has replied to the commands it already received
func (s *Server) Stop() {
	logger.Server().Infof("Stopping server...")

	s.mux.Lock()
	defer s.mux.Unlock()

	s.stopping = true

	for _, session := range s.sessions {
		session.Stop(shutdownTimeout)
	}

	if s.listener != nil {
		s.listener.Close()
	}
}

// track registers a session, so Stop can end it. It fails once the server is stopping or has MaxClients sessions.
func (s *Server) track(id int, session *resp.Session) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.stopping {
		return errStopping
	}

	if len(s.sessions) >= s.config.Server.MaxClients {
		return errMaxClients
	}

	s.sessions[id] = session
	return nil
}

func (s *Server) untrack(id int) {
	s.mux.Lock()
	defer s.mux.Unlock()

	delete(s.sessions, id)
}

func isLoopback(addr net.Addr) bool {
	tcpAddr, ok := addr.(*net.TCPAddr)
	return ok && tcpAddr.IP.IsLoopback()
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
