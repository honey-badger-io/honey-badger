package commands

import (
	"context"
	"strings"
	"time"

	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/grpc"
)

type Cmd interface {
	Run(ctx context.Context, db *string) error
}

type TimedCmd interface {
	Duration() time.Duration
}

type withDuration struct {
	duration time.Duration
}

func (cmd *withDuration) Duration() time.Duration {
	return cmd.duration
}

func Parse(cmdText string, conn *grpc.ClientConn) (Cmd, error) {
	if cmdText == "ls" {
		return &dbListCmd{
			client: pb.NewDbClient(conn),
		}, nil
	}

	if cmdText == "ping" {
		return &pingCmd{
			client: pb.NewSysClient(conn),
		}, nil
	}

	if strings.Index(cmdText, "use ") == 0 {
		return &useDbCmd{
			client: pb.NewDbClient(conn),
			params: strings.Split(cmdText, " ")[1:],
		}, nil
	}

	if strings.Index(cmdText, "create ") == 0 {
		return &createDbCmd{
			client: pb.NewDbClient(conn),
			params: strings.Split(cmdText, " ")[1:],
		}, nil
	}

	if cmdText == "drop" {
		return &dropDbCmd{
			client: pb.NewDbClient(conn),
			params: make([]string, 0),
		}, nil
	}

	return nil, nil
}
