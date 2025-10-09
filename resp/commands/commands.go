package commands

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/honey-badger-io/honey-badger/resp/common"
)

type RespCmd interface {
	Invoke(session common.Session) (common.RespResult, error)
}

func Parse(reader *bufio.Reader) (RespCmd, error) {
	// Read number of strings
	numberOfStrings, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	numberOfStrings = strings.TrimSpace(numberOfStrings)
	numberOfStrings = strings.Trim(numberOfStrings, "*")

	numOfStrings, err := strconv.Atoi(numberOfStrings)
	if err != nil {
		return nil, common.NewRespError("invalid initial message")
	}

	// Read CMD length (not used)
	_, _ = reader.ReadString('\n')

	// Read command
	cmd, _ := reader.ReadString('\n')
	cmd = strings.TrimSpace(cmd)
	cmd = strings.ToUpper(cmd)

	args := make([]string, 0)

	for range numOfStrings - 1 {
		// Read ARG length (not used)
		_, _ = reader.ReadString('\n')
		arg, _ := reader.ReadString('\n')
		arg = strings.TrimSpace(arg)

		args = append(args, arg)
	}

	respCmd, err := newCmd(cmd, numOfStrings-1, args)
	if err != nil {
		return nil, err
	}

	return respCmd, nil
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
