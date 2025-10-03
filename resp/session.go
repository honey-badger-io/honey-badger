package resp

import (
	"bufio"
	"errors"
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
	conn          net.Conn
	serverVersion string
}

func (s *Session) Id() int {
	return s.id
}

func (s *Session) Db() *db.DbContext {
	return s.dbCtx
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
