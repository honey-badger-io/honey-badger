package commands

import (
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type getCmd struct {
	numOfArgs int
	args      []string
}

const cmdGet = "GET"

func (cmd *getCmd) Invoke(dbCtx *db.DbContext) (common.RespResult, error) {
	// GET requires exactly 1 argument: key
	if cmd.numOfArgs != 1 {
		return common.ResultNull, common.NewRespErrorf("wrong number of arguments for 'get' command")
	}

	key := cmd.args[0]

	// Get the database
	database, err := dbCtx.GetDefaultDb()
	if err != nil {
		return common.ResultNull, common.NewRespError("database not found")
	}

	// Get the value
	value, exists, err := database.Get(key)
	if err != nil {
		return common.ResultNull, common.NewRespError("server error")
	}

	if !exists {
		return common.ResultNull, nil
	}

	return common.NewResultBulkString(string(value)), nil
}
