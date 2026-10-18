package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	var ctx = context.Background()
	server, client := startServer(ctx)
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

	t.Run("should listen on loopback by default", func(t *testing.T) {
		assert.True(t, isLoopback(server.listener.Addr()), server.listener.Addr().String())
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
	// Stop must end sessions right away even though their own deadline is much later
	server, stopped, addr := startTestServer(18951, func(c *config.ServerConfig) { c.Timeout = time.Minute })

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

func TestAuth(t *testing.T) {
	var ctx = context.Background()
	const password = "s3cret"
	server, _, addr := startTestServer(18952, func(c *config.ServerConfig) { c.Password = password })
	defer server.Stop()

	t.Run("should reject commands before AUTH", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Equal(t, "-NOAUTH Authentication required.", client.Do("GET", "k"))
		assert.Equal(t, "-NOAUTH Authentication required.", client.Do("PING"))
		assert.Equal(t, "-NOAUTH Authentication required.", client.Do("SELECT", "1"))
	})

	t.Run("should reject wrong credentials", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Equal(t, "-WRONGPASS invalid username-password pair or user is disabled.", client.Do("AUTH", "wrong"))
		assert.Equal(t, "-WRONGPASS invalid username-password pair or user is disabled.", client.Do("AUTH", "admin", password))
		assert.Equal(t, "-NOAUTH Authentication required.", client.Do("GET", "k"))
	})

	t.Run("should run commands after AUTH", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Equal(t, "+OK", client.Do("AUTH", password))
		assert.Equal(t, "+OK", client.Do("SET", "auth-key", "v"))
		assert.Equal(t, "v", client.Do("GET", "auth-key"))
	})

	t.Run("should run commands after AUTH with username", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Equal(t, "+OK", client.Do("AUTH", "default", password))
		assert.Equal(t, "+PONG", client.Do("PING"))
	})

	t.Run("should require auth in HELLO", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Contains(t, client.Do("HELLO", "3"), "-NOAUTH HELLO must be called with the client already authenticated")
		assert.Equal(t, "-WRONGPASS invalid username-password pair or user is disabled.", client.Do("HELLO", "3", "AUTH", "default", "wrong"))
	})

	t.Run("should authenticate go-redis client", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: addr, Password: password, ClientName: "authed"})
		defer client.Close()

		err := client.Set(ctx, "auth-key", "go-redis", 0).Err()
		val, _ := client.Get(ctx, "auth-key").Result()
		name, _ := client.ClientGetName(ctx).Result()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, "go-redis", val)
		assert.Equal(t, "authed", name)
	})

	t.Run("should fail go-redis client with wrong password", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: addr, Password: "wrong", MaxRetries: -1})
		defer client.Close()

		err := client.Ping(ctx).Err()

		assert.ErrorContains(t, err, "WRONGPASS")
	})

	t.Run("should limit command size before AUTH", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Equal(t, "-ERR Protocol error: invalid bulk length", client.Do("AUTH", strings.Repeat("x", 17*1024)))
		assert.True(t, client.Closed())
	})

	t.Run("should allow large commands after AUTH", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		assert.Equal(t, "+OK", client.Do("AUTH", password))
		assert.Equal(t, "+OK", client.Do("SET", "auth-big", strings.Repeat("x", 17*1024)))
	})
}

func TestMaxClients(t *testing.T) {
	server, _, addr := startTestServer(18953, func(c *config.ServerConfig) { c.MaxClients = 2 })
	defer server.Stop()

	waitForSessions(server, 0)

	first := dialRaw(addr)
	defer first.Close()
	second := dialRaw(addr)
	defer second.Close()

	// Makes sure both sessions are running before the third client connects
	first.Do("PING")
	second.Do("PING")

	t.Run("should reject client over the limit", func(t *testing.T) {
		third := dialRaw(addr)
		defer third.Close()

		assert.Equal(t, "-ERR max number of clients reached", third.ReadLine())
		assert.True(t, third.Closed())
	})

	t.Run("should accept client once another has left", func(t *testing.T) {
		first.Close()
		waitForSessions(server, 1)

		fourth := dialRaw(addr)
		defer fourth.Close()

		assert.Equal(t, "+PONG", fourth.Do("PING"))
	})
}

func TestTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	server, _, addr := startTestServer(18954, func(c *config.ServerConfig) { c.Timeout = timeout })
	defer server.Stop()

	t.Run("should close idle session", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()
		start := time.Now()

		assert.True(t, client.Closed())
		assert.Less(t, time.Since(start), 3*timeout)
	})

	t.Run("should keep active session open", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()

		for range 6 {
			assert.Equal(t, "+PONG", client.Do("PING"))
			time.Sleep(timeout / 3)
		}
	})

	t.Run("should close session that sends a command too slowly", func(t *testing.T) {
		client := dialRaw(addr)
		defer client.Close()
		start := time.Now()

		var err error
		for _, b := range []byte("*1\r\n$4\r\nPING\r\n") {
			if _, err = client.conn.Write([]byte{b}); err != nil {
				break
			}
			time.Sleep(timeout / 4)
		}

		assert.True(t, client.Closed())
		assert.Less(t, time.Since(start), 14*timeout/4)
	})
}

// startTestServer starts a server with the default config, changed by configure, and waits until it accepts connections
func startTestServer(port uint16, configure func(*config.ServerConfig)) (*Server, <-chan error, string) {
	config.SetDefault()
	cfg := *config.Get()
	cfg.Server.Port = port
	if configure != nil {
		configure(&cfg.Server)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server := New(&cfg, "0.0.0")
	stopped := make(chan error, 1)
	go func() { stopped <- server.Start() }()

	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
	}

	return server, stopped, addr
}

func waitForSessions(server *Server, n int) {
	for {
		server.mux.Lock()
		count := len(server.sessions)
		server.mux.Unlock()

		if count == n {
			return
		}

		time.Sleep(time.Millisecond)
	}
}

// rawClient sends RESP commands without a client library, so tests control exactly what goes over the wire
type rawClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func dialRaw(addr string) *rawClient {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}

	return &rawClient{conn: conn, reader: bufio.NewReader(conn)}
}

// Do sends a command and returns the reply line, or the data of a bulk string reply
func (c *rawClient) Do(args ...string) string {
	var cmd strings.Builder
	fmt.Fprintf(&cmd, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&cmd, "$%d\r\n%s\r\n", len(arg), arg)
	}

	if _, err := c.conn.Write([]byte(cmd.String())); err != nil {
		return err.Error()
	}

	line := c.ReadLine()
	if strings.HasPrefix(line, "$") {
		return c.ReadLine()
	}

	return line
}

func (c *rawClient) ReadLine() string {
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return err.Error()
	}

	return strings.TrimSuffix(line, "\r\n")
}

// Closed reports whether the server closes the connection within 2 seconds
func (c *rawClient) Closed() bool {
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := c.reader.ReadByte()

	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)
}

func (c *rawClient) Close() {
	c.conn.Close()
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
