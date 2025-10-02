package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
)

type Server struct {
	logger   *logger.Logger
	listener net.Listener
	config   config.ServerConfig
}

func New(c config.ServerConfig, dbCtx *db.DbContext) *Server {
	return &Server{
		logger: logger.Server(),
		config: c,
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
		// Accept a client connection
		conn, err := s.listener.Accept()

		if errors.Is(err, net.ErrClosed) {
			break
		}

		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Client connected:", conn.RemoteAddr())

		// Handle connection in a new goroutine
		go handleConnection(conn)
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

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		// Read number of strings
		numberOfStrings, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Client disconnected:", conn.RemoteAddr())
			return
		}
		numberOfStrings = strings.TrimSpace(numberOfStrings)
		numberOfStrings = strings.Trim(numberOfStrings, "*")

		n, err := strconv.Atoi(numberOfStrings)
		if err != nil {
			_, err = conn.Write([]byte("-ERR invalid initial message\r\n"))
			continue
		}

		// Read CMD length (not used)
		_, _ = reader.ReadString('\n')

		// Read command
		cmd, _ := reader.ReadString('\n')
		cmd = strings.TrimSpace(cmd)
		cmd = strings.ToUpper(cmd)

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
	}
}
