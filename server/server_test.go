package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

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

	t.Run("should count only deleted keys", func(t *testing.T) {
		conn.Set(ctx, "del-1", "a", 0)
		conn.Set(ctx, "del-2", "b", 0)

		count, err := conn.Del(ctx, "del-1", "del-missing", "del-2", "del-1").Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, int64(2), count)
		assert.Equal(t, int64(0), conn.Exists(ctx, "del-1", "del-2").Val())
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

func TestStop(t *testing.T) {
	var ctx = context.Background()
	config.SetDefault()
	cfg := *config.Get()
	cfg.Server.Port = 18951
	addr := fmt.Sprintf("localhost:%d", cfg.Server.Port)

	server := New(&cfg, "0.0.0")
	stopped := make(chan error, 1)
	go func() { stopped <- server.Start() }()

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	for client.Ping(ctx).Err() != nil {
	}

	// A session blocked waiting for its next command
	idle, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer idle.Close()

	reply := make([]byte, 7)
	_, _ = idle.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if _, err := io.ReadFull(idle, reply); err != nil {
		panic(err)
	}

	server.Stop()

	t.Run("should close idle sessions", func(t *testing.T) {
		_ = idle.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := idle.Read(reply)

		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("should return from Start once sessions have ended", func(t *testing.T) {
		select {
		case err := <-stopped:
			assert.Nil(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Start did not return")
		}
	})

	t.Run("should refuse new connections", func(t *testing.T) {
		_, err := net.Dial("tcp", addr)

		assert.NotNil(t, err)
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
