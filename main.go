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
	configPath   string
	printVersion bool
	version      string
)

func main() {
	flag.StringVar(&configPath, "config", "", "-config <path_to_config_file>")
	flag.BoolVar(&printVersion, "version", false, "-version")
	flag.Parse()

	if printVersion {
		fmt.Printf("%s\n", getVersion())
		return
	}

	if err := config.Init(configPath); err != nil {
		log.Fatal(err)
	}

	if err := logger.Init(); err != nil {
		log.Fatal(err)
	}

	dbCtx := db.CreateCtx(config.Get().Badger)
	defer dbCtx.Close()

	if err := dbCtx.LoadDbs(); err != nil {
		log.Fatal(err)
	}

	srv := server.New(config.Get().Server, dbCtx, getVersion())
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
