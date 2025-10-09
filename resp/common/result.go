package common

import (
	"fmt"
)

type RespResult string

const (
	ResultNull RespResult = "_\r\n"
	ResultOk RespResult = "+OK\r\n"
)

func NewResultString(data string) RespResult {
	return RespResult(serializeString(data))
}

func NewResultBulkString(data string) RespResult {
	return RespResult(serializeBulkString(data))
}

func NewResultInteger(data int) RespResult {
	return RespResult(serializeInt(data))
}

func NewResultMap(data map[string]any) RespResult {
	result := fmt.Sprintf("%%%d\r\n", len(data))

	for k, v := range data {
		// Serialize key
		result += serializeString(k)

		// Serialize value
		switch v.(type) {
		case int:
			result += serializeInt(v.(int))
		default:
			result += serializeBulkString(fmt.Sprintf("%v", v))
		}
	}

	return RespResult(result)
}

func serializeBulkString(data string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
}

func serializeString(data string) string {
	return fmt.Sprintf("+%s\r\n", data)
}

func serializeInt(data int) string {
	return fmt.Sprintf(":%d\r\n", data)
}
