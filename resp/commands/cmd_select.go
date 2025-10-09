package commands

import (
	"strconv"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type selectCmd struct {
	numOfArgs int
	args      []string
}

const cmdSelect = "SELECT"

func (cmd *selectCmd) Invoke(session common.Session) (common.RespResult, error) {
	// SELECT requires exactly 1 argument: database name
	if cmd.numOfArgs != 1 {
		return common.ResultNull, common.NewRespErrorf("wrong number of arguments for 'select' command")
	}

	dbIndex, err := strconv.Atoi(cmd.args[0])
	if err != nil {
		return common.ResultNull, common.NewRespError("value is not an integer or out of range")
	}

	if dbIndex < 0 || dbIndex > config.Get().Badger.MaxDbs-1 {
		return common.ResultNull, common.NewRespError("value is out of range")
	}

	// Set the selected database for this session
	if err := session.SetDb(dbIndex); err != nil {
		// TODO: Logg err
		return common.ResultNull, common.NewRespError("failed to select database")
	}

	return common.ResultOk, nil
}
