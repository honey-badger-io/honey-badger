package db

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/logger"
)

type DbContext struct {
	dbs      map[string]*Database
	gcTicker *time.Ticker
	config   config.BadgerConfig
	logger   *logger.Logger
}

type DbListItem struct {
	Name     string
	InMemory bool
}

func CreateCtx(c config.BadgerConfig) *DbContext {
	ctx := &DbContext{
		dbs:    make(map[string]*Database),
		config: c,
		logger: logger.Get("DbContext"),
	}

	return ctx
}

func (ctx *DbContext) LoadDbs() error {
	entries, err := os.ReadDir(ctx.config.DataDirPath)
	_, ok := err.(*fs.PathError)
	if ok {
		err = os.Mkdir(ctx.config.DataDirPath, 0777)
	}

	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		dbPath := path.Join(ctx.config.DataDirPath, name)

		opt := badger.DefaultOptions(dbPath).
			WithLogger(logger.Badger())

		ctx.logger.Infof("Loading '%s'", name)
		b, err := badger.Open(opt)
		if err != nil {
			return err
		}

		ctx.dbs[name] = &Database{
			b:    b,
			Name: name,
		}
	}

	ctx.gcTicker = time.NewTicker(time.Duration(ctx.config.GCPeriodMin) * time.Minute)

	startGCRoutine(ctx)

	return nil
}

func (ctx *DbContext) GetDb(name string) (*Database, error) {
	db := ctx.dbs[name]
	if db == nil {
		return nil, errors.New("db does not exists")
	}

	return ctx.dbs[name], nil
}

func (ctx *DbContext) DropDb(name string) error {
	db := ctx.dbs[name]
	if db == nil {
		return nil
	}

	dbDir := db.b.Opts().Dir

	// TODO: Block reads and writes
	err := db.b.DropAll()
	if err != nil {
		return err
	}

	err = db.b.Close()
	if err != nil {
		return err
	}

	err = os.RemoveAll(dbDir)
	if err != nil {
		return err
	}

	delete(ctx.dbs, name)

	ctx.logger.Infof("Dropped '%s'", name)

	return nil
}

func (ctx *DbContext) CreateDb(name string, inMemory bool) (*Database, error) {
	if name == "" {
		return nil, errors.New("'name' cannot be empty")
	}

	if ctx.dbs[name] != nil {
		return nil, errors.New("Db already exists")
	}

	var opt badger.Options

	if !inMemory {
		config := config.Get().Badger

		dbPath := path.Join(config.DataDirPath, name)

		opt = badger.DefaultOptions(dbPath)
	} else {
		opt = badger.DefaultOptions("").
			WithInMemory(inMemory)
	}

	opt = opt.WithLogger(logger.Badger())

	bdb, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	ctx.dbs[name] = &Database{
		b:    bdb,
		Name: name,
	}

	return ctx.dbs[name], nil
}

func (ctx *DbContext) Close() {
	if ctx.gcTicker != nil {
		ctx.gcTicker.Stop()
		ctx.logger.Infof("GC ticker closed")
	}

	for name, db := range ctx.dbs {
		ctx.logger.Infof("Closing database '%s'", name)
		if err := db.b.Close(); err != nil {
			ctx.logger.Error(err)
		}
	}
}

func (ctx *DbContext) Exists(name string) bool {
	return ctx.dbs[name] != nil
}

func (ctx *DbContext) List() []*Database {
	result := make([]*Database, len(ctx.dbs))

	i := 0
	for _, db := range ctx.dbs {
		result[i] = db
		i++
	}

	return result
}

func startGCRoutine(ctx *DbContext) {
	period := time.Duration(ctx.config.GCPeriodMin) * time.Minute
	ctx.gcTicker.Reset(period)
	ctx.logger.Infof("GC tick set to: %v\n", period)

	go func() {
		for range ctx.gcTicker.C {
			for name, itm := range ctx.dbs {
				// Do not run GC on in memory databases
				if itm.b.Opts().InMemory {
					continue
				}

				ctx.logger.Infof("Running GC on database '%s'...", name)
				err := itm.b.RunValueLogGC(0.5)
				if err != nil {
					ctx.logger.Warning(err)
				}
			}
		}
	}()
}
