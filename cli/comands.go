package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Command interface {
	Run() error
	Duration() time.Duration
}

func ParseCommand(ctx context.Context, cmdText string, dbClient pb.DbClient) Command {
	if cmdText == "quit" {
		return &QuitCmd{}
	}

	if cmdText == "ls" {
		return &DbListCmd{
			client: dbClient,
			ctx:    ctx,
		}
	}

	return nil
}

type DbListCmd struct {
	client pb.DbClient
	ctx    context.Context
	start  time.Time
}

type QuitCmd struct{}

func (cmd *DbListCmd) Run() error {
	cmd.start = time.Now()
	res, err := cmd.client.List(cmd.ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	if len(res.Dbs) == 0 {
		fmt.Println("Empty")
		return nil
	}

	for _, db := range res.Dbs {
		fmt.Printf("%s\t%v\n", db.Name, db.InMemory)
	}

	return nil
}

func (cmd *DbListCmd) Duration() time.Duration {
	return time.Since(cmd.start)
}

func (cmd *QuitCmd) Run() error {
	os.Exit(0)
	return nil
}

func (cmd *QuitCmd) Duration() time.Duration {
	return 0
}
