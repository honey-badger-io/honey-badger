package db

import (
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/logger"
)

var (
	dbs      map[string]*Database = make(map[string]*Database)
	gcTicker *time.Ticker
	mux      sync.Mutex
)

func OpenDb(name string, inMemory bool) (*Database, error) {
	mux.Lock()
	defer mux.Unlock()

	if dbs[name] != nil {
		return dbs[name], nil
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

	dbs[name] = &Database{
		b: bdb,
	}

	return dbs[name], nil
}

func CloseAllDbs() {
	if gcTicker != nil {
		gcTicker.Stop()
		logger.Badger().Infof("GC ticker closed")
	}

	for name, db := range dbs {
		logger.Badger().Infof("Closing database '%s'", name)
		if err := db.b.Close(); err != nil {
			logger.Badger().Error(err)
		}
	}
}

func StartGCRoutine() {
	config := config.Get().Badger
	period := time.Duration(config.GCPeriodMin) * time.Minute

	gcTicker = time.NewTicker(time.Duration(config.GCPeriodMin) * time.Minute)
	gcTicker.Reset(period)
	logger.Badger().Infof("GC tick set to: %v\n", period)

	go func() {
		for range gcTicker.C {
			for name, itm := range dbs {
				// Do not run GC on in memory databases
				if itm.b.Opts().InMemory {
					continue
				}

				logger.Badger().Infof("Running GC on database '%s'...", name)
				err := itm.b.RunValueLogGC(0.5)
				if err != nil {
					logger.Badger().Warning(err)
				}
			}
		}
	}()
}
