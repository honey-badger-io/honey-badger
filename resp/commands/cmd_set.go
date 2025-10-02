package commands

import (
	"strconv"
	"strings"

	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type setCmd struct {
	numOfArgs int
	args      []string
}

const cmdSet = "SET"

func (cmd *setCmd) Invoke(dbCtx *db.DbContext) (common.RespResult, error) {
	// SET requires at least 2 arguments: key and value
	if cmd.numOfArgs < 2 {
		return common.ResultNull, common.NewRespErrorf("wrong number of arguments for 'set' command")
	}

	key := cmd.args[0]
	value := cmd.args[1]

	// Parse optional arguments
	var ttl uint = 0
	i := 2
	for i < cmd.numOfArgs {
		option := strings.ToUpper(cmd.args[i])

		switch option {
		case "EX":
			// EX seconds -- Set the specified expire time, in seconds
			if i+1 >= cmd.numOfArgs {
				return common.ResultNull, common.NewRespError("syntax error")
			}
			seconds, err := strconv.Atoi(cmd.args[i+1])
			if err != nil || seconds <= 0 {
				return common.ResultNull, common.NewRespError("value is not an integer or out of range")
			}
			ttl = uint(seconds)
			i += 2
		case "PX":
			// PX milliseconds
			return common.ResultNull, common.NewRespErrorf("'%s' not supported. Use 'EX' instead", option)
		case "NX", "XX", "GET", "EXAT", "PXAT", "KEEPTTL":
			// These options are not yet implemented
			return common.ResultNull, common.NewRespErrorf("option '%s' not supported", option)
		default:
			return common.ResultNull, common.NewRespErrorf("syntax error")
		}
	}

	// Get the database
	database, err := dbCtx.GetDefaultDb()
	if err != nil {
		return common.ResultNull, common.NewRespError("database not found")
	}

	// Set the key-value pair
	err = database.Set(key, []byte(value), ttl)
	if err != nil {
		return common.ResultNull, common.NewRespError("server error")
	}

	return common.ResultOk, nil
}
