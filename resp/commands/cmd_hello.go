package commands

import (
	"strconv"
	"strings"

	"github.com/honey-badger-io/honey-badger/resp/common"
)

type helloCmd struct {
	numOfArgs int
	args      []string
}

const defaultProto = 3
const cmdHello = "HELLO"

// HELLO [protover [AUTH username password] [SETNAME clientname]]
func (cmd *helloCmd) Invoke(session common.Session) (common.RespResult, error) {
	proto := defaultProto
	var err error

	if cmd.numOfArgs > 0 {
		proto, err = strconv.Atoi(cmd.args[0])
		if err != nil {
			return common.ResultNull, common.NewRespErrorf("Protocol version is not an integer or out of range")
		}

		if proto != defaultProto {
			return common.ResultNull, common.ProtoError
		}
	}

	var username, password, name string
	var hasAuth, hasName bool

	for i := 1; i < cmd.numOfArgs; {
		option := strings.ToUpper(cmd.args[i])

		switch {
		case option == "AUTH" && i+2 < cmd.numOfArgs:
			username, password, hasAuth = cmd.args[i+1], cmd.args[i+2], true
			i += 3
		case option == "SETNAME" && i+1 < cmd.numOfArgs:
			name, hasName = cmd.args[i+1], true
			i += 2
		default:
			return common.ResultNull, common.NewRespErrorf("Syntax error in HELLO option '%s'", cmd.args[i])
		}
	}

	if hasAuth && !session.Authenticate(username, password) {
		return common.ResultNull, common.WrongPassError
	}

	if !session.Authenticated() {
		return common.ResultNull, common.NoAuthHello
	}

	if hasName {
		session.SetName(name)
	}

	data := make(map[string]any)
	data["server"] = "honey-badger"
	data["version"] = session.ServerVersion()
	data["proto"] = proto
	data["mode"] = "standalone"
	data["role"] = "master"
	data["id"] = session.Id()

	return common.NewResultMap(data), nil
}
