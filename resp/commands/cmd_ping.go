package commands

import (
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

type pingCmd struct {
}

const cmdPing = "PING"

func (cmd *pingCmd) Invoke(dbCtx *db.DbContext) (common.RespResult, error) {
	return common.NewResultString("PONG"), nil
}
