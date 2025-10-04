package common

import "github.com/honey-badger-io/honey-badger/db"

type Session interface {
	Id() int
	Db() *db.Database
	SetDb(index int) error
	ServerVersion() string
	SetName(name string)
}
