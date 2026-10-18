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

var (
	ProtoError     = RespError{errType: "NOPROTO", msg: "unsupported protocol version"}
	NoAuthError    = RespError{errType: "NOAUTH", msg: "Authentication required."}
	NoAuthHello    = RespError{errType: "NOAUTH", msg: "HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time"}
	WrongPassError = RespError{errType: "WRONGPASS", msg: "invalid username-password pair or user is disabled."}
)

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
