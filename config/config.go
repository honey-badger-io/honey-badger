package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

const (
	EnvPort       = "HB_PORT"       // int
	EnvDataDir    = "HB_DATA_DIR"   // string
	EnvBackupDir  = "HB_BACKUP_DIR" // stirng
	EnvDbInMem    = "HB_DB_IN_MEM"  // bool
	PermDataDir   = 0700
	PermBackupDir = 0744
)

type Config struct {
	Badger BadgerConfig
	Server ServerConfig
	Logger LoggerConfig
}

type ServerConfig struct {
	Port uint16
}

type BadgerConfig struct {
	DataDirPath   string
	BackupDirPath string
	GCPeriodMin   int
	MaxDbs        int
	InMemory      bool
}

type LoggerConfig struct {
	Sinks map[string]any
}

var current *Config
var defaults = Config{
	Badger: BadgerConfig{
		DataDirPath:   "data",
		BackupDirPath: "data_backup",
		GCPeriodMin:   60,
		MaxDbs:        16,
		InMemory:      false,
	},
	Server: ServerConfig{
		Port: 18950,
	},
	Logger: LoggerConfig{
		Sinks: map[string]any{
			"console": true,
		},
	},
}

func Init() error {
	envConfig := Config{}

	portEnv := os.Getenv(EnvPort)
	inMembDbEnv := strings.TrimSpace(os.Getenv(EnvDbInMem))
	port, _ := strconv.Atoi(portEnv)

	envConfig.Server.Port = uint16(port)
	envConfig.Badger.DataDirPath = os.Getenv(EnvDataDir)
	envConfig.Badger.BackupDirPath = os.Getenv(EnvBackupDir)

	setDefaults(&envConfig, inMembDbEnv)

	if err := ensureDir(envConfig.Badger.DataDirPath, PermDataDir); err != nil {
		return err
	}

	if err := ensureDir(envConfig.Badger.BackupDirPath, PermDataDir); err != nil {
		return err
	}

	current = &envConfig

	return nil
}

func SetDefault() {
	current = &defaults
}

func Get() *Config {
	return current
}

func setDefaults(config *Config, inMembDbEnv string) {
	if config.Server.Port <= 1023 {
		config.Server.Port = defaults.Server.Port
	}

	if config.Badger.DataDirPath == "" {
		config.Badger.DataDirPath = defaults.Badger.DataDirPath
	}

	if config.Badger.BackupDirPath == "" {
		config.Badger.BackupDirPath = defaults.Badger.BackupDirPath
	}

	if config.Badger.GCPeriodMin <= 0 {
		config.Badger.GCPeriodMin = defaults.Badger.GCPeriodMin
	}

	if config.Badger.MaxDbs <= 0 {
		config.Badger.MaxDbs = defaults.Badger.MaxDbs
	}

	config.Badger.InMemory = defaults.Badger.InMemory

	if strings.ToLower(inMembDbEnv) == "true" {
		config.Badger.InMemory = true
	}

	if len(config.Logger.Sinks) == 0 {
		config.Logger.Sinks = defaults.Logger.Sinks
	}
}

func ensureDir(path string, perm os.FileMode) error {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, perm); err != nil {
			return err
		}
	}

	if err != nil {
		return nil
	}

	return nil
}
