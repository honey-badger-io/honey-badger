package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	EnvPort       = "HB_PORT"        // int
	EnvBind       = "HB_BIND"        // string
	EnvPassword   = "HB_PASSWORD"    // string
	EnvTimeout    = "HB_TIMEOUT"     // int, seconds
	EnvMaxClients = "HB_MAX_CLIENTS" // int
	EnvDataDir    = "HB_DATA_DIR"    // string
	EnvDbInMem    = "HB_DB_IN_MEM"   // bool
)

type Config struct {
	Badger BadgerConfig
	Server ServerConfig
	Logger LoggerConfig
}

type ServerConfig struct {
	Port uint16
	Bind string
	// Clients must AUTH with it before running other commands. Empty disables auth.
	Password string
	// How long a session may wait for a command and its reply. 0 disables it.
	Timeout    time.Duration
	MaxClients int
}

type BadgerConfig struct {
	DataDirPath string
	GCPeriodMin int
	MaxDbs      int
	InMemory    bool
}

type LoggerConfig struct {
	Sinks map[string]any
}

var current *Config
var defaults = Config{
	Badger: BadgerConfig{
		DataDirPath: "data",
		GCPeriodMin: 60,
		MaxDbs:      16,
		InMemory:    true,
	},
	Server: ServerConfig{
		Port:       18950,
		Bind:       "127.0.0.1",
		MaxClients: 10000,
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
	envConfig.Server.Bind = strings.TrimSpace(os.Getenv(EnvBind))
	envConfig.Server.Password = os.Getenv(EnvPassword)
	envConfig.Badger.DataDirPath = os.Getenv(EnvDataDir)

	timeout, err := intEnv(EnvTimeout, 0, 0)
	if err != nil {
		return err
	}
	envConfig.Server.Timeout = time.Duration(timeout) * time.Second

	if envConfig.Server.MaxClients, err = intEnv(EnvMaxClients, defaults.Server.MaxClients, 1); err != nil {
		return err
	}

	setDefaults(&envConfig, inMembDbEnv)

	// Create data dir if not exists
	_, err = os.Stat(envConfig.Badger.DataDirPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(envConfig.Badger.DataDirPath, 0o750); err != nil {
			return err
		}
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

	if config.Server.Bind == "" {
		config.Server.Bind = defaults.Server.Bind
	}

	if config.Server.MaxClients <= 0 {
		config.Server.MaxClients = defaults.Server.MaxClients
	}

	if config.Badger.DataDirPath == "" {
		config.Badger.DataDirPath = defaults.Badger.DataDirPath
	}

	if config.Badger.GCPeriodMin <= 0 {
		config.Badger.GCPeriodMin = defaults.Badger.GCPeriodMin
	}

	if config.Badger.MaxDbs <= 0 {
		config.Badger.MaxDbs = defaults.Badger.MaxDbs
	}

	config.Badger.InMemory = defaults.Badger.InMemory

	if strings.ToLower(inMembDbEnv) == "false" {
		config.Badger.InMemory = false
	}

	if len(config.Logger.Sinks) == 0 {
		config.Logger.Sinks = defaults.Logger.Sinks
	}
}

// intEnv parses an int env var. Unlike the port, a bad value is an error rather than silently replaced by a default.
func intEnv(name string, fallback int, min int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(value)
	if err != nil || n < min {
		return 0, fmt.Errorf("%s must be an integer >= %d, got %q", name, min, value)
	}

	return n, nil
}
