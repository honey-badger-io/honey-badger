package commands

import (
	"strconv"

	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type helloCmd struct {
	numOfArgs int
	args      []string
}

const defaultProto = 3
const cmdHello = "HELLO"

func (cmd *helloCmd) Invoke(dbCtx *db.DbContext) (common.RespResult, error) {
	proto := defaultProto
	var err error

	if cmd.numOfArgs == 1 {
		proto, err = strconv.Atoi(cmd.args[0])
		if err != nil {
			return common.ResultNull, common.NewRespErrorf("Protocol version is not an integer or out of range")
		}

		if proto != defaultProto {
			return common.ResultNull, common.ProtoError
		}
	}

	if cmd.numOfArgs > 1 {
		return common.ResultNull, common.NewRespErrorf("Syntax error in HELLO option '%s'", cmd.args[1])
	}

	data := make(map[string]any)
	data["server"] = "honey-badger"
	data["version"] = "0.0.1"
	data["proto"] = proto
	data["mode"] = "standalone"
	data["role"] = "master"
	data["id"] = 0 //TODO: Connection id

	return common.NewResultMap(data), nil
}
