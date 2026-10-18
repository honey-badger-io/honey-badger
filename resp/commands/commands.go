package commands

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/honey-badger-io/honey-badger/resp/common"
)

type RespCmd interface {
	Invoke(session common.Session) (common.RespResult, error)
}

// Same limits as Redis' defaults
const (
	maxArrayLen = 1024 * 1024
	maxBulkLen  = 512 * 1024 * 1024
)

// Caps how much memory a declared length can reserve before its data has actually arrived
const maxPrealloc = 64 * 1024

// Parse reads one RESP array of bulk strings from the reader and builds the command it names.
// A common.ProtocolError means the stream is out of sync and the session must end, any other
// non-RespError is an I/O error from the reader.
func Parse(reader *bufio.Reader) (RespCmd, error) {
	args, err := readArray(reader)
	if err != nil {
		return nil, err
	}

	return newCmd(strings.ToUpper(args[0]), len(args)-1, args[1:])
}

func readArray(reader *bufio.Reader) ([]string, error) {
	n, err := readLength(reader, '*', 1, maxArrayLen)
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, min(n, 64))

	for range n {
		arg, err := readBulkString(reader)
		if err != nil {
			return nil, unexpectedEOF(err)
		}

		args = append(args, arg)
	}

	return args, nil
}

func readBulkString(reader *bufio.Reader) (string, error) {
	n, err := readLength(reader, '$', 0, maxBulkLen)
	if err != nil {
		return "", err
	}

	// Read exactly n bytes, so values may contain CRLF or any other binary data
	var data strings.Builder
	data.Grow(min(n, maxPrealloc))

	if _, err := io.CopyN(&data, reader, int64(n)); err != nil {
		return "", err
	}

	crlf, err := reader.Peek(2)
	if err != nil {
		return "", err
	}

	if crlf[0] != '\r' || crlf[1] != '\n' {
		return "", common.NewProtocolErrorf("expected CRLF after bulk string")
	}

	_, _ = reader.Discard(2)

	return data.String(), nil
}

// readLength reads a "<prefix><int>\r\n" header line and checks that the int is within [lo, hi]
func readLength(reader *bufio.Reader, prefix byte, lo int, hi int) (int, error) {
	line, err := readLine(reader)
	if err != nil {
		return 0, err
	}

	if len(line) == 0 || line[0] != prefix {
		got := "end of line"
		if len(line) > 0 {
			got = strconv.QuoteRune(rune(line[0]))
		}

		return 0, common.NewProtocolErrorf("expected '%c', got %s", prefix, got)
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < lo || n > hi {
		if prefix == '*' {
			return 0, common.NewProtocolErrorf("invalid multibulk length")
		}

		return 0, common.NewProtocolErrorf("invalid bulk length")
	}

	return n, nil
}

// readLine returns the next line without its CRLF. The slice is only valid until the next read.
func readLine(reader *bufio.Reader) ([]byte, error) {
	// ReadSlice is bounded by the reader's buffer size, unlike ReadString
	line, err := reader.ReadSlice('\n')

	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, common.NewProtocolErrorf("too big header line")
	}

	if err == io.EOF && len(line) > 0 {
		return nil, io.ErrUnexpectedEOF
	}

	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, common.NewProtocolErrorf("expected CRLF")
	}

	return line[:len(line)-2], nil
}

// unexpectedEOF reports an EOF in the middle of an array as io.ErrUnexpectedEOF
func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}

	return err
}

func newCmd(cmd string, numOfArguments int, args []string) (RespCmd, error) {
	if cmd == cmdPing {
		return &pingCmd{}, nil
	}

	// TODO: Can we use reflection here?
	if cmd == cmdHello {
		return &helloCmd{
			numOfArgs: numOfArguments,
			args:      args,
		}, nil
	}

	if cmd == cmdSet {
		return &setCmd{
			numOfArgs: numOfArguments,
			args:      args,
		}, nil
	}

	if cmd == cmdGet {
		return &getCmd{
			numOfArgs: numOfArguments,
			args:      args,
		}, nil
	}

	if cmd == cmdDel {
		return &delCmd{
			numOfArgs: numOfArguments,
			args:      args,
		}, nil
	}

	if cmd == cmdSelect {
		return &selectCmd{
			numOfArgs: numOfArguments,
			args:      args,
		}, nil
	}

	if cmd == cmdClient {
		return &clientCmd{
			numOfArgs: numOfArguments,
			args:      args,
		}, nil
	}

	return nil, common.NewRespError("unknown command")
}
