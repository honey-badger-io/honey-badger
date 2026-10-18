package config

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should create data dir writable by owner", func(t *testing.T) {
		dataDir := path.Join(t.TempDir(), "nested", "data")
		t.Setenv(EnvDataDir, dataDir)

		if err := Init(); err != nil {
			panic(err)
		}

		info, err := os.Stat(dataDir)
		if err != nil {
			panic(err)
		}

		assert.True(t, info.IsDir())
		assert.Equal(t, os.FileMode(0o750), info.Mode().Perm())

		// On-disk databases are created inside the data dir
		assert.Nil(t, os.Mkdir(path.Join(dataDir, "db0"), 0o750))
	})

	t.Run("should use secure server defaults", func(t *testing.T) {
		t.Setenv(EnvDataDir, t.TempDir())

		if err := Init(); err != nil {
			panic(err)
		}

		assert.Equal(t, "127.0.0.1", Get().Server.Bind)
		assert.Equal(t, "", Get().Server.Password)
		assert.Equal(t, time.Duration(0), Get().Server.Timeout)
		assert.Equal(t, 10000, Get().Server.MaxClients)
	})

	t.Run("should read server settings from env", func(t *testing.T) {
		t.Setenv(EnvDataDir, t.TempDir())
		t.Setenv(EnvBind, "0.0.0.0")
		t.Setenv(EnvPassword, "s3cret")
		t.Setenv(EnvTimeout, "30")
		t.Setenv(EnvMaxClients, "5")

		if err := Init(); err != nil {
			panic(err)
		}

		assert.Equal(t, "0.0.0.0", Get().Server.Bind)
		assert.Equal(t, "s3cret", Get().Server.Password)
		assert.Equal(t, 30*time.Second, Get().Server.Timeout)
		assert.Equal(t, 5, Get().Server.MaxClients)
	})

	t.Run("should reject invalid server settings", func(t *testing.T) {
		for env, value := range map[string]string{
			EnvTimeout:    "-1",
			EnvMaxClients: "0",
		} {
			t.Setenv(EnvDataDir, t.TempDir())
			t.Setenv(env, value)

			assert.Error(t, Init(), "%s=%s", env, value)

			t.Setenv(env, "")
		}

		t.Setenv(EnvTimeout, "abc")
		assert.Error(t, Init())
	})
}
