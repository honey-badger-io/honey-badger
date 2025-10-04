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
	server, client := startServer(ctx)
	defer server.Stop()
	defer client.Close()

	t.Run("should call set", func(t *testing.T) {
		const key = "key"
		const value = "value"

		err := client.Set(ctx, key, value, 0).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call get", func(t *testing.T) {
		const key = "key"
		const value = "value"

		val, err := client.Get(ctx, key).Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, value, val)
	})

	t.Run("should call delete", func(t *testing.T) {
		const key = "key"

		err := client.Del(ctx, key).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call select", func(t *testing.T) {
		err := client.Conn().Select(ctx, 1).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call ping", func(t *testing.T) {
		val, err := client.Ping(ctx).Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, "PONG", val)
	})

	t.Run("should call hello with empty name", func(t *testing.T) {
		val, err := client.Conn().Hello(ctx, 3, "", "", "").Result()

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
		err := client.Conn().Hello(ctx, 3, "", "", connectionName).Err()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})
}

func startServer(ctx context.Context) (*Server, *redis.Client) {
	port := 18950
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", port),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	dbCtx := db.CreateCtx(config.BadgerConfig{
		DataDirPath: "data",
		GCPeriodMin: 60,
	})
	server := New(config.ServerConfig{
		Port: uint16(port),
	}, dbCtx, "0.0.0")

	go server.Start()

	for {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			break
		}
	}

	return server, rdb
}
