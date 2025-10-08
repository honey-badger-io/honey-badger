package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/logger"
	"github.com/honey-badger-io/honey-badger/server"
)

var (
	printVersion bool
	version      string
)

func main() {
	flag.BoolVar(&printVersion, "version", false, "-version")
	flag.Parse()

	if printVersion {
		fmt.Printf("%s\n", getVersion())
		return
	}

	if err := config.Init(); err != nil {
		log.Fatal(err)
	}

	if err := logger.Init(); err != nil {
		log.Fatal(err)
	}

	db.StartGCRoutine()
	defer db.CloseAllDbs()

	srv := server.New(config.Get(), getVersion())
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}

func getVersion() string {
	if version == "" {
		return "0.0.0"
	}

	return version
}
