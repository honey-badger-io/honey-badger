package commands

import (
	"strings"

	"github.com/honey-badger-io/honey-badger/resp/common"
)

type clientCmd struct {
	numOfArgs int
	args      []string
}

const (
	cmdClient     = "CLIENT"
	subCmdSetname = "SETNAME"
	subCmdGetname = "GETNAME"
)

func (cmd *clientCmd) Invoke(session common.Session) (common.RespResult, error) {
	if cmd.numOfArgs == 0 {
		return common.ResultNull, common.NewRespError("wrong number of arguments for 'CLIENT' command")
	}

	subCmd := strings.ToUpper(strings.TrimSpace(cmd.args[0]))

	if subCmd == subCmdSetname {
		if cmd.numOfArgs != 2 {
			return common.ResultNull, common.NewRespErrorf("wrong number of arguments for '%s|%s' command", cmdClient, subCmd)
		}

		session.SetName(cmd.args[1])
		return common.ResultOk, nil
	}

	if subCmd == subCmdGetname {
		name := session.Name()
		if name == "" {
			return common.ResultNull, nil
		}

		return common.NewResultBulkString(name), nil
	}

	return common.ResultNull, common.NewRespErrorf("unknown subcommand '%s'", cmd.args[0])
}
