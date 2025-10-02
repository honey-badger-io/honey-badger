package common

import "fmt"

type RespError struct {
	msg string
}

func (e RespError) Error() string {
	return fmt.Sprintf("-ERR %s\r\n", e.msg)
}

func NewRespError(msg string) RespError {
	return RespError{msg: msg}
}
