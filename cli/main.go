package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	target string = "127.0.0.1:18950"
)

func main() {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	sysClient := pb.NewSysClient(conn)
	_, err = sysClient.Ping(context.Background(), &pb.PingRequest{})
	if err != nil {
		log.Fatalln(err)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s> ", target)
		cmdText, _ := reader.ReadString('\n')

		cmd := ParseCommand(context.Background(), strings.Trim(cmdText, "\n"), conn)

		if cmd == nil {
			fmt.Println("Invalid command")
			continue
		}

		if err := cmd.Run(); err != nil {
			fmt.Printf("ERR: %v", err)
			continue
		}

		timedCmd, ok := cmd.(MeasurableCmd)
		if ok {
			fmt.Printf("\nDone in %s\n", timedCmd.Duration())
		}
	}
}
