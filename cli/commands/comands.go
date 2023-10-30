package commands

import (
	"context"
	"os"
	"time"

	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/grpc"
)

type Cmd interface {
	Run(ctx context.Context) error
}

type TimedCmd interface {
	Duration() time.Duration
}

type withDuration struct {
	duration time.Duration
}

type quitCmd struct{}

func (cmd *withDuration) Duration() time.Duration {
	return cmd.duration
}

func (cmd *quitCmd) Run(ctx context.Context) error {
	os.Exit(0)
	return nil
}

func Parse(cmdText string, conn *grpc.ClientConn) Cmd {
	if cmdText == "quit" {
		return &quitCmd{}
	}

	if cmdText == "ls" {
		return &dbListCmd{
			client: pb.NewDbClient(conn),
		}
	}

	if cmdText == "ping" {
		return &pingCmd{
			client: pb.NewSysClient(conn),
		}
	}

	return nil
}
