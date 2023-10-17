package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Badger BadgerConfig
	Server ServerConfig
	Logger LoggerConfig
}

type ServerConfig struct {
	Port             uint16
	MaxRecvMsgSizeMb int
}

type BadgerConfig struct {
	DataDirPath string
	GCPeriodMin int
}

type LoggerConfig struct {
	Sinks map[string]any
}

var current Config
var defaults = Config{
	Badger: BadgerConfig{
		DataDirPath: "data",
		GCPeriodMin: 60,
	},
	Server: ServerConfig{
		Port:             18950,
		MaxRecvMsgSizeMb: 200,
	},
	Logger: LoggerConfig{
		Sinks: map[string]any{
			"console": true,
		},
	},
}

func Init(configFilePath string) error {
	if configFilePath == "" {
		current = defaults
		return nil
	}

	f, err := os.OpenFile(configFilePath, os.O_RDONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer f.Close()

	fileConfing := Config{}
	decoder := json.NewDecoder(f)

	if err := decoder.Decode(&fileConfing); err != nil {
		return err
	}

	setDefaults(&fileConfing)

	current = fileConfing

	return nil
}

func Get() Config {
	return current
}

func setDefaults(config *Config) {
	if config.Server.Port <= 1023 {
		config.Server.Port = defaults.Server.Port
	}

	if config.Server.MaxRecvMsgSizeMb < 4 {
		config.Server.MaxRecvMsgSizeMb = defaults.Server.MaxRecvMsgSizeMb
	}

	if config.Badger.DataDirPath == "" {
		config.Badger.DataDirPath = defaults.Badger.DataDirPath
	}

	if config.Badger.GCPeriodMin <= 0 {
		config.Badger.GCPeriodMin = defaults.Badger.GCPeriodMin
	}

	if len(config.Logger.Sinks) == 0 {
		config.Logger.Sinks = defaults.Logger.Sinks
	}
}
