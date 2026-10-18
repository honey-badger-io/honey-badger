package commands

import (
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type authCmd struct {
	numOfArgs int
	args      []string
}

const (
	cmdAuth     = "AUTH"
	defaultUser = "default"
)

func (cmd *authCmd) Invoke(session common.Session) (common.RespResult, error) {
	// AUTH password | AUTH username password
	if cmd.numOfArgs != 1 && cmd.numOfArgs != 2 {
		return common.ResultNull, common.NewRespError("wrong number of arguments for 'auth' command")
	}

	username, password := defaultUser, cmd.args[0]
	if cmd.numOfArgs == 2 {
		username, password = cmd.args[0], cmd.args[1]
	}

	if cmd.numOfArgs == 1 && !session.PasswordRequired() {
		return common.ResultNull, common.NewRespError("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}

	if !session.Authenticate(username, password) {
		return common.ResultNull, common.WrongPassError
	}

	return common.ResultOk, nil
}
