package common

import (
	"fmt"
)

type RespError struct {
	msg     string
	errType string
}

var ProtoError RespError = RespError{errType: "NOPROTO", msg: "unsupported protocol version\r\n"}

func (e RespError) Error() string {
	return fmt.Sprintf("-%s %s\r\n", e.errType, e.msg)
}

func NewRespError(msg string) RespError {
	return RespError{errType: "ERR", msg: msg}
}

func NewRespErrorf(msg string, args ...any) RespError {
	return RespError{errType: "ERR", msg: fmt.Sprintf(msg, args...)}
}
