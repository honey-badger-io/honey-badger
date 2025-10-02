package commands

import "github.com/honey-badger-io/honey-badger/resp/common"

type RespCmd interface {
	Invoke() (common.RespResult, error)
}

func NewCmd(cmd string, numOfArguments int) (RespCmd, error) {
	if cmd == cmdPing {
		return &pingCmd{}, nil
	}

	return nil, common.NewRespError("unknown command")
}
