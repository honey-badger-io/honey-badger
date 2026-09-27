package common

import (
	"fmt"
	"strings"
)

type RespResult string

const (
	ResultNull RespResult = "_\r\n"
	ResultOk   RespResult = "+OK\r\n"
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
	var result strings.Builder
	fmt.Fprintf(&result, "%%%d\r\n", len(data))

	for k, v := range data {
		// Serialize key
		result.WriteString(serializeString(k))

		// Serialize value
		switch v := v.(type) {
		case int:
			result.WriteString(serializeInt(v))
		default:
			result.WriteString(serializeBulkString(fmt.Sprintf("%v", v)))
		}
	}

	return RespResult(result.String())
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
