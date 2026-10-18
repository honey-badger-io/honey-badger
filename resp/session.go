package resp

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"

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
	password      string
	timeout       time.Duration
	authenticated bool
	// Unix nanos of the deadline for remaining replies once Stop was called, 0 while running
	stopAt atomic.Int64
}

// Options are the server settings a session enforces
type Options struct {
	// Empty disables auth
	Password string
	// How long the session may wait for a command and its reply. 0 disables it.
	Timeout time.Duration
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

func (s *Session) PasswordRequired() bool {
	return s.password != ""
}

func (s *Session) Authenticated() bool {
	return s.authenticated
}

func (s *Session) Authenticate(username, password string) bool {
	if username != "default" {
		return false
	}

	// Like Redis' default user without a password, which accepts any password
	if s.password == "" {
		s.authenticated = true
		return true
	}

	// Comparing hashes keeps the comparison constant time whatever the lengths are
	want := sha256.Sum256([]byte(s.password))
	got := sha256.Sum256([]byte(password))

	if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		return false
	}

	s.authenticated = true
	return true
}

// Stop makes the session end once it has replied to the commands it already received, waiting at most timeout for the replies.
// It's safe to call from another goroutine.
func (s *Session) Stop(timeout time.Duration) {
	s.stopAt.Store(time.Now().Add(timeout).UnixNano())
	s.applyStopDeadlines()
}

func (s *Session) applyStopDeadlines() {
	_ = s.conn.SetReadDeadline(time.Now())
	_ = s.conn.SetWriteDeadline(time.Unix(0, s.stopAt.Load()))
}

// resetDeadline gives the next command and its reply a full timeout
func (s *Session) resetDeadline() {
	if s.timeout > 0 {
		_ = s.conn.SetDeadline(time.Now().Add(s.timeout))
	}

	// Stop may have run just before the deadline above and been overridden by it
	if s.stopAt.Load() != 0 {
		s.applyStopDeadlines()
	}
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
		s.resetDeadline()

		limits := commands.DefaultLimits
		if !s.authenticated {
			limits = commands.UnauthenticatedLimits
		}

		cmd, err := commands.Parse(reader, limits)

		// The stream can't be resynced after a protocol error, so reply and end the session
		if protoErr, ok := errors.AsType[common.ProtocolError](err); ok {
			_, _ = writer.WriteString(protoErr.Error())
			return
		}

		respErr, isRespErr := errors.AsType[common.RespError](err)

		// Any other parse error comes from the connection itself. A deadline means the server is stopping.
		if err != nil && !isRespErr {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
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
	if !s.authenticated && !commands.AllowedBeforeAuth(cmd) {
		return common.NoAuthError.Error()
	}

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

func NewSession(id int, conn net.Conn, db *db.Database, serverVersion string, options Options) *Session {
	return &Session{
		id:            id,
		conn:          conn,
		db:            db,
		serverVersion: serverVersion,
		password:      options.Password,
		timeout:       options.Timeout,
		authenticated: options.Password == "",
	}
}
