package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/debug"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/resp/commands"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type Session struct {
	id            int
	db            *db.Database
	conn          net.Conn
	serverVersion string
	name          string
}

func (s *Session) Id() int {
	return s.id
}

func (s *Session) Db() *db.Database {
	// Session should already have db0 initiated
	return s.db
}

func (s *Session) SetDb(index int) error {
	dbName := fmt.Sprintf("db%d", index)

	var err error
	s.db, err = db.OpenDb(dbName, config.Get().Badger.InMemory)

	return err
}

func (s *Session) SetName(name string) {
	s.name = name
}

func (s *Session) Name() string {
	return s.name
}

func (s *Session) ServerVersion() string {
	return s.serverVersion
}

func (s *Session) Handle() {
	defer func() {
		// A bug in one command must not take down the whole server
		if r := recover(); r != nil {
			logger.Server().Errorf("session %d panicked: %v\n%s", s.id, r, debug.Stack())
		}

		s.conn.Close()
		runtime.GC()
	}()

	reader := bufio.NewReader(s.conn)
	for {
		cmd, err := commands.Parse(reader)

		// The stream can't be resynced after a protocol error, so reply and end the session
		if protoErr, ok := errors.AsType[common.ProtocolError](err); ok {
			_, _ = s.conn.Write([]byte(protoErr.Error()))
			return
		}

		if respErr, ok := errors.AsType[common.RespError](err); ok {
			_, _ = s.conn.Write([]byte(respErr.Error()))
			continue
		}

		// Any other parse error comes from the connection itself
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				logger.Server().Error(err)
			}
			return
		}

		result, err := cmd.Invoke(s)

		if respErr, ok := errors.AsType[common.RespError](err); ok {
			_, _ = s.conn.Write([]byte(respErr.Error()))
			continue
		}

		if err != nil {
			respErr := common.NewRespError("server error")
			_, _ = s.conn.Write([]byte(respErr.Error()))
			logger.Server().Error(err)
			continue
		}

		_, _ = s.conn.Write([]byte(result))
	}
}

func NewSession(id int, conn net.Conn, db *db.Database, serverVersion string) *Session {
	return &Session{
		id:            id,
		conn:          conn,
		db:            db,
		serverVersion: serverVersion,
	}
}
