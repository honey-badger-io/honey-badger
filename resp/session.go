package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
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
	reader := bufio.NewReader(s.conn)
	writer := bufio.NewWriter(s.conn)

	defer func() {
		// A bug in one command must not take down the whole server
		if r := recover(); r != nil {
			logger.Server().Errorf("session %d panicked: %v\n%s", s.id, r, debug.Stack())
		}

		_ = writer.Flush()
		s.conn.Close()
	}()

	for {
		cmd, err := commands.Parse(reader)

		// The stream can't be resynced after a protocol error, so reply and end the session
		if protoErr, ok := errors.AsType[common.ProtocolError](err); ok {
			_, _ = writer.WriteString(protoErr.Error())
			return
		}

		respErr, isRespErr := errors.AsType[common.RespError](err)

		// Any other parse error comes from the connection itself
		if err != nil && !isRespErr {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				logger.Server().Error(err)
			}
			return
		}

		if isRespErr {
			_, _ = writer.WriteString(respErr.Error())
		} else {
			_, _ = writer.WriteString(s.invoke(cmd))
		}

		// Send replies to pipelined commands in one write, once all the commands read so far are done
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Session) invoke(cmd commands.RespCmd) string {
	result, err := cmd.Invoke(s)

	if respErr, ok := errors.AsType[common.RespError](err); ok {
		return respErr.Error()
	}

	if err != nil {
		logger.Server().Error(err)
		return common.NewRespError("server error").Error()
	}

	return string(result)
}

func NewSession(id int, conn net.Conn, db *db.Database, serverVersion string) *Session {
	return &Session{
		id:            id,
		conn:          conn,
		db:            db,
		serverVersion: serverVersion,
	}
}
