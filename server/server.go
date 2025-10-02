package server

import (
	"bufio"
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
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type Server struct {
	logger   *logger.Logger
	listener net.Listener
	config   config.ServerConfig
	dbCtx    *db.DbContext
}

func New(c config.ServerConfig, dbCtx *db.DbContext) *Server {
	return &Server{
		logger: logger.Server(),
		config: c,
		dbCtx:  dbCtx,
	}
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return err
	}

	//https://dgraph.io/docs/badger/faq/#are-there-any-go-specific-settings-that-i-should-use
	runtime.GOMAXPROCS(128)

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

		// Handle connection in a new goroutine
		go handleConnection(s, conn)
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

func handleConnection(server *Server, conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		cmd, err := resp.ParseCmd(reader)
		var respErr common.RespError

		if errors.As(err, &respErr) {
			_, _ = conn.Write([]byte(respErr.Error()))
			continue
		}

		if err != nil && err.Error() == "EOF" {
			return
		}

		if err != nil {
			respErr = common.NewRespError("server error")
			_, _ = conn.Write([]byte(respErr.Error()))
			server.logger.Error(err)
			return
		}

		result, err := cmd.Invoke()

		if errors.As(err, &respErr) {
			_, _ = conn.Write([]byte(respErr.Error()))
			continue
		}

		if err != nil {
			respErr = common.NewRespError("server error")
			_, _ = conn.Write([]byte(respErr.Error()))
			server.logger.Error(err)
			continue
		}

		_, _ = conn.Write([]byte(result))

		/*
			if cmd == "PING" {
				_, err = conn.Write([]byte("+PONG\r\n"))
				continue
			}

			if cmd == "SET" {
				numOfSetParams := n - 1
				if numOfSetParams < 2 {
					_, err = conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					continue
				}

				if numOfSetParams > 2 {
					_, err = conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					continue
				}

				// Read KEY length (not used)
				_, _ = reader.ReadString('\n')
				key, _ := reader.ReadString('\n')
				key = strings.TrimSpace(key)

				// Read DATA length (not used)
				_, _ = reader.ReadString('\n')
				data, _ := reader.ReadString('\n')
				data = strings.TrimSpace(data)

				fmt.Printf("SET %s %s\n", key, data)
				_, err = conn.Write([]byte("+OK\r\n"))
				continue
			}

			_, err = conn.Write([]byte("-ERR unknown command\r\n"))
		*/
	}
}
