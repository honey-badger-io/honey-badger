package server

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/db"
	"github.com/honey-badger-io/honey-badger/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DbName = "test-db"
)

func TestDbServer(t *testing.T) {
	conn, server := startServer()
	defer conn.Close()
	defer server.Stop()

	client := pb.NewDbClient(conn)
	const db = "test-create-db"

	t.Run("should call create database", func(t *testing.T) {
		_, err := client.Create(context.TODO(), &pb.CreateDbReq{
			Name: db,
			Opt: &pb.CreateDbOpt{
				InMemory: true,
			},
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call drop database", func(t *testing.T) {
		_, err := client.Drop(context.TODO(), &pb.DropDbRequest{
			Name: db,
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call exists database", func(t *testing.T) {
		res, err := client.Exists(context.TODO(), &pb.ExistsDbReq{
			Name: "test-db-exists",
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.False(t, res.Exists)
	})

	t.Run("should call ensure db", func(t *testing.T) {
		_, err := client.EnsureDb(context.TODO(), &pb.CreateDbReq{
			Name: "test-db-ensure",
			Opt: &pb.CreateDbOpt{
				InMemory: true,
			},
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})
}

func TestDataServer(t *testing.T) {
	conn, server := startServer()
	defer conn.Close()
	defer server.Stop()

	client := pb.NewDataClient(conn)
	db := pb.NewDbClient(conn)

	_, err := db.Create(context.TODO(), &pb.CreateDbReq{
		Name: DbName,
		Opt: &pb.CreateDbOpt{
			InMemory: true,
		},
	})
	if err != nil {
		panic(err)
	}

	t.Run("should call set", func(t *testing.T) {
		_, err := client.Set(context.TODO(), &pb.SetRequest{
			Db:   DbName,
			Key:  "test-key",
			Data: []byte("test"),
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call get", func(t *testing.T) {
		_, err := client.Get(context.TODO(), &pb.KeyRequest{
			Db:  DbName,
			Key: "test-key",
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call delete", func(t *testing.T) {
		_, err := client.Delete(context.TODO(), &pb.KeyRequest{
			Db:  DbName,
			Key: "test-test",
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call delete by prefix", func(t *testing.T) {
		_, err := client.DeleteByPrefix(context.TODO(), &pb.PrefixRequest{
			Db:     DbName,
			Prefix: "test-",
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
	})

	t.Run("should call create send stream", func(t *testing.T) {
		stream, err := client.CreateSendStream(context.TODO())
		if err != nil {
			panic(err)
		}

		sendErr := stream.Send(&pb.SendStreamReq{
			Db: DbName,
		})

		_, err = stream.CloseAndRecv()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Nil(t, sendErr, fmt.Sprintf("%v", err))
	})

	t.Run("should call get data stream", func(t *testing.T) {
		prefix := "data-stream-"
		res, err := client.CreateReadStream(context.TODO(), &pb.ReadStreamReq{
			Db:     DbName,
			Prefix: &prefix,
		})

		_, errRecv := res.Recv()

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, io.EOF, errRecv)
	})
}

func startServer() (*grpc.ClientConn, *Server) {
	port := 18950
	target := fmt.Sprintf("127.0.0.1:%d", port)

	dbCtx := db.CreateCtx(config.BadgerConfig{
		DataDirPath: "data",
		GCPeriodMin: 60,
	})
	server := New(config.ServerConfig{
		Port:             uint16(port),
		MaxRecvMsgSizeMb: 4,
	}, dbCtx)

	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)

	}

	sysClient := pb.NewSysClient(conn)

	go server.Start()

	for {
		_, err := sysClient.Ping(context.TODO(), &pb.PingRequest{})
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return conn, server
}
