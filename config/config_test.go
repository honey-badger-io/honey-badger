package config

import (
	"os"
	"path"
	"testing"

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
}
