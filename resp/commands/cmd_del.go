package commands

import (
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type delCmd struct {
	numOfArgs int
	args      []string
}

const cmdDel = "DEL"

func (cmd *delCmd) Invoke(session common.Session) (common.RespResult, error) {
	// DEL requires at least 1 argument: key(s)
	if cmd.numOfArgs < 1 {
		return common.ResultNull, common.NewRespErrorf("wrong number of arguments for 'del' command")
	}

	// Count only the keys that existed, like Redis
	deletedCount := 0
	for _, key := range cmd.args {
		deleted, err := session.Db().DeleteByKey(key)
		if err != nil {
			return common.ResultNull, err
		}

		if deleted {
			deletedCount++
		}
	}

	return common.NewResultInteger(deletedCount), nil
}
