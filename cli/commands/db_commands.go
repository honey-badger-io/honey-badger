package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type dbListCmd struct {
	withDuration
	client pb.DbClient
}

type pingCmd struct {
	withDuration
	client pb.SysClient
}

func (cmd *dbListCmd) Run(ctx context.Context) error {
	start := time.Now()
	res, err := cmd.client.List(ctx, &emptypb.Empty{})
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

func (cmd *pingCmd) Run(ctx context.Context) error {
	start := time.Now()
	res, err := cmd.client.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		return err
	}
	cmd.duration = time.Since(start)

	fmt.Printf("%s\n", res.Mesage)

	return nil
}
