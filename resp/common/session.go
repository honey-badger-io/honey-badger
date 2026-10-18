package common

import "github.com/honey-badger-io/honey-badger/db"

type Session interface {
	Id() int
	Db() *db.Database
	Name() string
	SetDb(index int) error
	ServerVersion() string
	SetName(name string)
	// PasswordRequired reports whether the server has a password set
	PasswordRequired() bool
	Authenticated() bool
	// Authenticate checks the credentials and marks the session as authenticated if they match
	Authenticate(username, password string) bool
}
