package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/chzyer/readline"
	"github.com/honey-badger-io/honey-badger/cli/commands"
	"github.com/honey-badger-io/honey-badger/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	target string = "127.0.0.1:18950"
	db     string
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

	l, err := readline.NewEx(&readline.Config{
		Prompt:          "» ",
		AutoComplete:    completer,
		HistoryFile:     "/tmp/hb-cli-history.tmp",
		InterruptPrompt: "^C",
		EOFPrompt:       "quit",

		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()
	l.CaptureExitSignal()
	log.SetOutput(l.Stderr())

	for {
		cmdText, err := l.Readline()
		if err == readline.ErrInterrupt {
			break
		}

		if err == io.EOF {
			break
		}

		cmdText = strings.TrimSpace(cmdText)
		if cmdText == "" {
			continue
		}

		if cmdText == "quit" {
			break
		}

		cmd, err := commands.Parse(cmdText, conn)
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

var completer = readline.NewPrefixCompleter(
	readline.PcItem("quit"),
	readline.PcItem("ls"),
)
