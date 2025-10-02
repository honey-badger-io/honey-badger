package commands

import (
	"testing"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/stretchr/testify/assert"
)

func TestSetCmd_Basic(t *testing.T) {
	// Create test database context
	cfg := config.BadgerConfig{
		DataDirPath: t.TempDir(),
		GCPeriodMin: 60,
	}
	dbCtx := db.CreateCtx(cfg)
	defer dbCtx.Close()

	// Create db0
	_, err := dbCtx.CreateDb("db0", true) // in-memory for test
	assert.NoError(t, err)

	// Test basic SET command
	cmd := &setCmd{
		numOfArgs: 2,
		args:      []string{"mykey", "myvalue"},
	}

	result, err := cmd.Invoke(dbCtx)
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", string(result))

	// Verify the value was set
	database, err := dbCtx.GetDb("db0")
	assert.NoError(t, err)

	value, exists, err := database.Get("mykey")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "myvalue", string(value))
}

func TestSetCmd_WithTTL(t *testing.T) {
	// Create test database context
	cfg := config.BadgerConfig{
		DataDirPath: t.TempDir(),
		GCPeriodMin: 60,
	}
	dbCtx := db.CreateCtx(cfg)
	defer dbCtx.Close()

	// Create db0
	_, err := dbCtx.CreateDb("db0", true) // in-memory for test
	assert.NoError(t, err)

	// Test SET command with EX option
	cmd := &setCmd{
		numOfArgs: 4,
		args:      []string{"mykey", "myvalue", "EX", "10"},
	}

	result, err := cmd.Invoke(dbCtx)
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", string(result))

	// Verify the value was set
	database, err := dbCtx.GetDb("db0")
	assert.NoError(t, err)

	value, exists, err := database.Get("mykey")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "myvalue", string(value))
}

func TestSetCmd_MissingArguments(t *testing.T) {
	// Create test database context
	cfg := config.BadgerConfig{
		DataDirPath: t.TempDir(),
		GCPeriodMin: 60,
	}
	dbCtx := db.CreateCtx(cfg)
	defer dbCtx.Close()

	// Create db0
	_, err := dbCtx.CreateDb("db0", true)
	assert.NoError(t, err)

	// Test SET command with only key (missing value)
	cmd := &setCmd{
		numOfArgs: 1,
		args:      []string{"mykey"},
	}

	_, err = cmd.Invoke(dbCtx)
	assert.Error(t, err)
}

func TestSetCmd_InvalidTTL(t *testing.T) {
	// Create test database context
	cfg := config.BadgerConfig{
		DataDirPath: t.TempDir(),
		GCPeriodMin: 60,
	}
	dbCtx := db.CreateCtx(cfg)
	defer dbCtx.Close()

	// Create db0
	_, err := dbCtx.CreateDb("db0", true)
	assert.NoError(t, err)

	// Test SET command with invalid EX value
	cmd := &setCmd{
		numOfArgs: 4,
		args:      []string{"mykey", "myvalue", "EX", "invalid"},
	}

	_, err = cmd.Invoke(dbCtx)
	assert.Error(t, err)
}

func TestSetCmd_SyntaxError(t *testing.T) {
	// Create test database context
	cfg := config.BadgerConfig{
		DataDirPath: t.TempDir(),
		GCPeriodMin: 60,
	}
	dbCtx := db.CreateCtx(cfg)
	defer dbCtx.Close()

	// Create db0
	_, err := dbCtx.CreateDb("db0", true)
	assert.NoError(t, err)

	// Test SET command with EX but missing value
	cmd := &setCmd{
		numOfArgs: 3,
		args:      []string{"mykey", "myvalue", "EX"},
	}

	_, err = cmd.Invoke(dbCtx)
	assert.Error(t, err)
}
