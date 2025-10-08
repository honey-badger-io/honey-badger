package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	var ctx = context.Background()
	_, client := startServer(ctx)
	conn := client.Conn()

	defer conn.Close()
	defer db.CloseAllDbs()

	t.Run("should call set", func(t *testing.T) {
		const key = "key"
		const value = "value"

		err := conn.Set(ctx, key, value, 0).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call get", func(t *testing.T) {
		const key = "key"
		const value = "value"

		val, err := conn.Get(ctx, key).Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, value, val)
	})

	t.Run("should call delete", func(t *testing.T) {
		const key = "key"

		err := conn.Del(ctx, key).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call select", func(t *testing.T) {
		err := conn.Select(ctx, 1).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call ping", func(t *testing.T) {
		val, err := conn.Ping(ctx).Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, "PONG", val)
	})

	t.Run("should call hello with empty name", func(t *testing.T) {
		val, err := conn.Hello(ctx, 3, "", "", "").Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, "honey-badger", val["server"])
		assert.Equal(t, "0.0.0", val["version"])
		assert.Equal(t, int64(3), val["proto"].(int64))
		assert.Equal(t, "standalone", val["mode"])
		assert.Equal(t, "master", val["role"])
		assert.Greater(t, val["id"].(int64), int64(0))
	})

	t.Run("should call hello with name", func(t *testing.T) {
		const connectionName = "test"
		err := conn.Hello(ctx, 3, "", "", connectionName).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call client setname", func(t *testing.T) {
		const connectionName = "test1"
		err := conn.ClientSetName(ctx, connectionName).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call client getname", func(t *testing.T) {
		const connectionName = "test1"

		conn.ClientSetName(ctx, connectionName).Err()
		val, err := conn.ClientGetName(ctx).Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, connectionName, val)
	})
}

func startServer(ctx context.Context) (*Server, *redis.Client) {
	config.SetDefault()
	cfg := config.Get()
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", cfg.Server.Port),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	server := New(cfg, "0.0.0")

	go server.Start()

	for {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			break
		}
	}

	return server, rdb
}
