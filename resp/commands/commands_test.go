package commands

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/resp/common"
	"github.com/stretchr/testify/assert"
)

func TestReadArray(t *testing.T) {
	t.Run("should read args", func(t *testing.T) {
		args, err := readArray(newReader("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))

		assert.Nil(t, err)
		assert.Equal(t, []string{"SET", "key", "value"}, args)
	})

	t.Run("should keep CRLF, whitespace and binary data in values", func(t *testing.T) {
		value := "  x\r\n*1\r\n$4\r\nPING\r\n\x00\xff "

		args, err := readArray(newReader(encode("SET", "k", value)))

		assert.Nil(t, err)
		assert.Equal(t, []string{"SET", "k", value}, args)
	})

	t.Run("should read empty bulk string", func(t *testing.T) {
		args, err := readArray(newReader("*2\r\n$3\r\nGET\r\n$0\r\n\r\n"))

		assert.Nil(t, err)
		assert.Equal(t, []string{"GET", ""}, args)
	})

	t.Run("should read pipelined commands", func(t *testing.T) {
		reader := newReader(encode("PING") + encode("GET", "k"))

		first, err1 := readArray(reader)
		second, err2 := readArray(reader)
		_, err3 := readArray(reader)

		assert.Nil(t, err1)
		assert.Nil(t, err2)
		assert.Equal(t, []string{"PING"}, first)
		assert.Equal(t, []string{"GET", "k"}, second)
		assert.Equal(t, io.EOF, err3)
	})

	t.Run("should return EOF on empty stream", func(t *testing.T) {
		_, err := readArray(newReader(""))

		assert.Equal(t, io.EOF, err)
	})

	t.Run("should return unexpected EOF on truncated command", func(t *testing.T) {
		for _, input := range []string{
			"*2",
			"*2\r\n",
			"*2\r\n$3\r\nGET\r\n",
			"*2\r\n$3\r\nGET\r\n$5\r\nab",
			"*2\r\n$3\r\nGET\r\n$2\r\nab",
			"*2\r\n$3\r\nGET\r\n$2\r\nab\r",
		} {
			_, err := readArray(newReader(input))

			assert.ErrorIs(t, err, io.ErrUnexpectedEOF, "%q", input)
		}
	})

	t.Run("should return protocol error on malformed input", func(t *testing.T) {
		for _, input := range []string{
			"PING\r\n",
			"*0\r\n",
			"*-1\r\n",
			"*abc\r\n",
			"*\r\n",
			"\r\n",
			"*1\n$4\r\nPING\r\n",
			fmt.Sprintf("*%d\r\n", maxArrayLen+1),
			"*1\r\n$-1\r\n",
			"*1\r\n:4\r\nPING\r\n",
			fmt.Sprintf("*1\r\n$%d\r\n", maxBulkLen+1),
			"*1\r\n$4\r\nPINGxx",
			"*1\r\n$4\r\n" + strings.Repeat("1", 5000) + "\r\n",
			"*" + strings.Repeat("1", 5000) + "\r\n",
		} {
			_, err := readArray(newReader(input))

			_, ok := errors.AsType[common.ProtocolError](err)
			assert.True(t, ok, "%q: got %v", input, err)
		}
	})

	t.Run("should not reserve memory for declared lengths before data arrives", func(t *testing.T) {
		allocs := testing.AllocsPerRun(10, func() {
			_, _ = readArray(newReader(fmt.Sprintf("*%d\r\n$%d\r\nab", maxArrayLen, maxBulkLen)))
		})

		// Would be a ~512MB allocation if the bulk length was trusted
		assert.Less(t, allocs, 20.0)
	})
}

func TestParse(t *testing.T) {
	t.Run("should parse command case insensitive", func(t *testing.T) {
		cmd, err := Parse(newReader(encode("get", "Key")))

		assert.Nil(t, err)
		assert.Equal(t, &getCmd{numOfArgs: 1, args: []string{"Key"}}, cmd)
	})

	t.Run("should keep stream in sync after unknown command", func(t *testing.T) {
		reader := newReader(encode("NOPE", "a", "b") + encode("PING"))

		_, err1 := Parse(reader)
		cmd, err2 := Parse(reader)

		_, ok := errors.AsType[common.RespError](err1)
		assert.True(t, ok)
		assert.Nil(t, err2)
		assert.Equal(t, &pingCmd{}, cmd)
	})
}

func FuzzReadArrayRoundTrip(f *testing.F) {
	f.Add("SET", "key", "value")
	f.Add("GET", "", "")
	f.Add("SET", "k", "a\r\nb")
	f.Add("SET", " k ", "\r\n*1\r\n$4\r\nPING\r\n")
	f.Add("\x00", "\xff\xfe", "$-1\r\n")

	f.Fuzz(func(t *testing.T, a, b, c string) {
		want := []string{a, b, c}
		reader := newReader(encode(want...))

		args, err := readArray(reader)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !slices.Equal(want, args) {
			t.Fatalf("got %q, want %q", args, want)
		}

		if _, err := readArray(reader); err != io.EOF {
			t.Fatalf("stream not fully consumed, got %v", err)
		}
	})
}

func FuzzParse(f *testing.F) {
	config.SetDefault()
	session := newFakeSession()

	f.Add([]byte(encode("PING")))
	f.Add([]byte(encode("SET", "k", "v", "EX", "10") + encode("GET", "k") + encode("DEL", "k", "x")))
	f.Add([]byte(encode("SELECT", "3") + encode("SELECT", "-1") + encode("SELECT", "99")))
	f.Add([]byte(encode("CLIENT") + encode("CLIENT", "SETNAME", "n") + encode("CLIENT", "GETNAME")))
	f.Add([]byte(encode("HELLO") + encode("HELLO", "3", "SETNAME", "n") + encode("HELLO", "2")))
	f.Add([]byte("*0\r\n$6\r\nCLIENT\r\n"))
	f.Add([]byte("*1000000\r\n$3\r\nDEL\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$100\r\nshort\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bufio.NewReader(strings.NewReader(string(data)))

		for {
			cmd, err := Parse(reader)

			if _, ok := errors.AsType[common.RespError](err); ok {
				continue
			}

			if err != nil {
				return
			}

			// Must never panic, whatever the args are
			_, _ = cmd.Invoke(session)
		}
	})
}

type fakeSession struct {
	db   *db.Database
	name string
}

func newFakeSession() *fakeSession {
	database, err := db.OpenDb("fuzz", true)
	if err != nil {
		panic(err)
	}

	return &fakeSession{db: database}
}

func (s *fakeSession) Id() int               { return 1 }
func (s *fakeSession) Db() *db.Database      { return s.db }
func (s *fakeSession) Name() string          { return s.name }
func (s *fakeSession) SetDb(index int) error { return nil }
func (s *fakeSession) ServerVersion() string { return "0.0.0" }
func (s *fakeSession) SetName(name string)   { s.name = name }

func encode(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))

	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}

	return b.String()
}

func newReader(input string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(input))
}
