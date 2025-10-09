package resp

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"runtime"

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
		s.conn.Close()
		runtime.GC()
	}()

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
			logger.Server().Error(err)
			continue
		}

		result, err := cmd.Invoke(s)

		if errors.As(err, &respErr) {
			_, _ = s.conn.Write([]byte(respErr.Error()))
			continue
		}

		if err != nil {
			respErr = common.NewRespError("server error")
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
