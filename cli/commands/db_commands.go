package commands

import (
	"context"
	"errors"
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

type useDbCmd struct {
	client pb.DbClient
	params []string
}

type createDbCmd struct {
	withDuration
	client pb.DbClient
	params []string
}

type dropDbCmd struct {
	withDuration
	client pb.DbClient
	params []string
}

func (cmd *dbListCmd) Run(ctx context.Context, db *string) error {
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

func (cmd *pingCmd) Run(ctx context.Context, db *string) error {
	start := time.Now()
	res, err := cmd.client.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		return err
	}
	cmd.duration = time.Since(start)

	fmt.Printf("%s\n", res.Mesage)

	return nil
}

func (cmd *useDbCmd) Run(ctx context.Context, db *string) error {
	if len(cmd.params) > 1 {
		return errors.New("invalid command parameter")
	}

	res, err := cmd.client.Exists(ctx, &pb.ExistsDbReq{
		Name: cmd.params[0],
	})
	if err != nil {
		return err
	}

	if !res.Exists {
		return errors.New("db does not exists")
	}

	// Set current db
	*db = cmd.params[0]

	return nil
}

func (cmd *createDbCmd) Run(ctx context.Context, db *string) error {
	options := &pb.CreateDbOpt{
		InMemory: true,
	}

	start := time.Now()
	_, err := cmd.client.Create(ctx, &pb.CreateDbReq{
		Name: cmd.params[0],
		Opt:  options,
	})
	if err != nil {
		return err
	}

	cmd.duration = time.Since(start)

	*db = cmd.params[0]

	fmt.Printf("Db created\n")
	return nil
}

func (cmd *dropDbCmd) Run(ctx context.Context, db *string) error {
	if db == nil || *db == "" {
		return errors.New("no db selected")
	}

	start := time.Now()
	_, err := cmd.client.Drop(ctx, &pb.DropDbRequest{
		Name: *db,
	})
	if err != nil {
		return err
	}
	cmd.duration = time.Since(start)

	*db = ""

	return nil
}
