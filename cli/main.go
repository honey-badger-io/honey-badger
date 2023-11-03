package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"

	"github.com/honey-badger-io/honey-badger/cli/commands"
	"github.com/honey-badger-io/honey-badger/pb"
	"github.com/manifoldco/promptui"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	target string = "127.0.0.1:18950"
	db     string
)

var promptTemplates = &promptui.PromptTemplates{
	Prompt:  "{{ . }}",
	Valid:   "{{ . }}",
	Invalid: "{{ . }}",
	Success: "{{ . }}",
}

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

	for {
		prompt := promptui.Prompt{
			Label:     fmt.Sprintf("%s %s> ", conn.Target(), db),
			Templates: promptTemplates,
		}

		// Wait for command text
		cmdText, err := prompt.Run()
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(int(syscall.SIGINT))
			return
		}

		if cmdText == "" {
			continue
		}

		cmd, err := commands.Parse(strings.Trim(cmdText, "\n"), conn)
		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}

		if cmd == nil {
			fmt.Println("Invalid command")
			continue
		}

		if err := cmd.Run(context.Background(), &db); err != nil {
			fmt.Printf("%v\n", err)
			continue
		}

		timedCmd, ok := cmd.(commands.TimedCmd)
		if ok && timedCmd.Duration() > 0 {
			fmt.Printf("\nDone in %s\n", timedCmd.Duration())
		}
	}
}
