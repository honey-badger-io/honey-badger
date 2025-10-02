package common

import (
	"fmt"
)

type RespResult string

const ResultNull RespResult = "_\r\n"
const ResultOk RespResult = "+OK\r\n"

func NewResultString(data string) RespResult {
	return RespResult(fmt.Sprintf("+%s\r\n", data))
}

func NewResultBulkString(data string) RespResult {
	return RespResult(fmt.Sprintf("$%d\r\n%s\r\n", len(data), data))
}

func NewResultMap(data map[string]any) RespResult {
	result := fmt.Sprintf("%%%d\r\n", len(data))

	for k, v := range data {
		// Serialize key
		result += fmt.Sprintf("+%s\r\n", k)

		// Serialize value
		switch v.(type) {
		case int:
			result += fmt.Sprintf(":%d\r\n", v)
		case string:
			result += fmt.Sprintf("+%s\r\n", v)
		default:
			result += fmt.Sprintf("+%v\r\n", v)
		}
	}

	return RespResult(result)
}
