package resp

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/resp/commands"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type Session struct {
	id            int
	logger        *logger.Logger
	dbCtx         *db.DbContext
	db            *db.Database
	conn          net.Conn
	serverVersion string
	name          string
}

func (s *Session) Id() int {
	return s.id
}

func (s *Session) Db() *db.Database {
	if s.db != nil {
		return s.db
	}

	if err := s.SetDb(0); err != nil {
		panic(err)
	}

	return s.db
}

func (s *Session) SetDb(index int) error {
	dbName := fmt.Sprintf("db%d", index)
	const inMemory = true

	if s.dbCtx.Exists(dbName) {
		s.db = s.dbCtx.GetDb(dbName)
		return nil
	}

	var err error
	s.db, err = s.dbCtx.CreateDb(dbName, inMemory)

	return err
}

func (s *Session) SetName(name string) {
	s.name = name
}

func (s *Session) ServerVersion() string {
	return s.serverVersion
}

func (s *Session) Handle() {
	defer s.conn.Close()

	reader := bufio.NewReader(s.conn)
	for {
		cmd, err := commands.Parse(reader)
		var respErr common.RespError

		if errors.As(err, &respErr) {
			_, _ = s.conn.Write([]byte(respErr.Error()))
			continue
		}

		if err != nil && err.Error() == "EOF" {
			return
		}

		if err != nil {
			respErr = common.NewRespError("server error")
			_, _ = s.conn.Write([]byte(respErr.Error()))
			s.logger.Error(err)
			return
		}

		result, err := cmd.Invoke(s)

		if errors.As(err, &respErr) {
			_, _ = s.conn.Write([]byte(respErr.Error()))
			continue
		}

		if err != nil {
			respErr = common.NewRespError("server error")
			_, _ = s.conn.Write([]byte(respErr.Error()))
			s.logger.Error(err)
			continue
		}

		_, _ = s.conn.Write([]byte(result))
	}
}

func NewSession(id int, conn net.Conn, logger *logger.Logger, dbCtx *db.DbContext, serverVersion string) *Session {
	return &Session{
		id:            id,
		conn:          conn,
		logger:        logger,
		dbCtx:         dbCtx,
		serverVersion: serverVersion,
	}
}
