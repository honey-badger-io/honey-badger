package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Command interface {
	Run() error
}

type MeasurableCmd interface {
	Duration() time.Duration
}

type WithDuration struct {
	duration time.Duration
}

func ParseCommand(ctx context.Context, cmdText string, conn *grpc.ClientConn) Command {
	if cmdText == "quit" {
		return &QuitCmd{}
	}

	if cmdText == "ls" {
		return &DbListCmd{
			client: pb.NewDbClient(conn),
			ctx:    ctx,
		}
	}

	if cmdText == "ping" {
		return &PingCmd{
			client: pb.NewSysClient(conn),
			ctx:    ctx,
		}
	}

	return nil
}

func (cmd *WithDuration) Duration() time.Duration {
	return cmd.duration
}

type DbListCmd struct {
	WithDuration
	client pb.DbClient
	ctx    context.Context
}

type QuitCmd struct{}

type PingCmd struct {
	WithDuration
	client pb.SysClient
	ctx    context.Context
}

func (cmd *DbListCmd) Run() error {
	start := time.Now()
	res, err := cmd.client.List(cmd.ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	cmd.duration = time.Since(start)

	if len(res.Dbs) == 0 {
		fmt.Println("Empty")
		return nil
	}

	for _, db := range res.Dbs {
		fmt.Printf("%s\t%v\n", db.Name, db.InMemory)
	}

	return nil
}

func (cmd *QuitCmd) Run() error {
	os.Exit(0)
	return nil
}

func (cmd *PingCmd) Run() error {
	start := time.Now()
	res, err := cmd.client.Ping(cmd.ctx, &pb.PingRequest{})
	if err != nil {
		return err
	}
	cmd.duration = time.Since(start)

	fmt.Printf("%s\n", res.Mesage)

	return nil
}
