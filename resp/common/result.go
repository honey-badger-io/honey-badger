package common

import "fmt"

type RespResult string

func NewResultString(data string) RespResult {
	return RespResult(fmt.Sprintf("+%s\r\n", data))
}
