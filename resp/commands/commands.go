package commands

import (
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type RespCmd interface {
	Invoke(dbCtx *db.DbContext) (common.RespResult, error)
}

func NewCmd(cmd string, numOfArguments int, args []string) (RespCmd, error) {
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

	return nil, common.NewRespError("unknown command")
}
