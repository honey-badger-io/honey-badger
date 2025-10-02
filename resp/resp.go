package resp

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/honey-badger-io/honey-badger/resp/commands"
	"github.com/honey-badger-io/honey-badger/resp/common"
)

func ParseCmd(reader *bufio.Reader) (commands.RespCmd, error) {
	// Read number of strings
	numberOfStrings, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	numberOfStrings = strings.TrimSpace(numberOfStrings)
	numberOfStrings = strings.Trim(numberOfStrings, "*")

	numOfStrings, err := strconv.Atoi(numberOfStrings)
	if err != nil {
		return nil, common.NewRespError("invalid initial message")
	}

	// Read CMD length (not used)
	_, _ = reader.ReadString('\n')

	// Read command
	cmd, _ := reader.ReadString('\n')
	cmd = strings.TrimSpace(cmd)
	cmd = strings.ToUpper(cmd)

	respCmd, err := commands.NewCmd(cmd, numOfStrings-1)
	if err != nil {
		return nil, err
	}

	return respCmd, nil
}
