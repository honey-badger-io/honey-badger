package common

import (
	"fmt"
)

type RespError struct {
	msg     string
	errType string
}

// ProtocolError is a RespError after which the request stream can't be resynced, so the session must end.
type ProtocolError struct {
	RespError
}

var ProtoError RespError = RespError{errType: "NOPROTO", msg: "unsupported protocol version"}

func (e RespError) Error() string {
	return fmt.Sprintf("-%s %s\r\n", e.errType, e.msg)
}

func NewRespError(msg string) RespError {
	return RespError{errType: "ERR", msg: msg}
}

func NewRespErrorf(msg string, args ...any) RespError {
	return RespError{errType: "ERR", msg: fmt.Sprintf(msg, args...)}
}

func NewProtocolErrorf(msg string, args ...any) ProtocolError {
	return ProtocolError{RespError{errType: "ERR", msg: "Protocol error: " + fmt.Sprintf(msg, args...)}}
}
