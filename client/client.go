package main

import (
	"bufio"
	"log"
	"os"
	"raftkv/kv"
	"strings"
)

const (
	GET    = "get"
	PUT    = "put"
	DELETE = "delete"
	EXIT   = "exit"
)

type Command struct {
	Name   string
	Num    int
	Action func(args []string)
}

func readLine() string {
	reader := bufio.NewReader(os.Stdin)
	print(">> ")
	line, _, err := reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	return strings.TrimSpace(string(line))
}

func parseArgs(line string) []string {
	argsRead := strings.Split(line, " ")
	args := make([]string, 0)
	for _, str := range argsRead {
		if !strings.EqualFold(str, " ") && !strings.EqualFold(str, "") {
			args = append(args, str)
		}
	}
	return args
}

func main() {
	var path string
	if len(os.Args) < 2 {
		log.Fatal("Missing server config file")
	} else {
		path = os.Args[1]
	}
	clientEnds := kv.GetClientEnds(path)
	client := kv.MakeKVClient(clientEnds)

	commands := []*Command{
		{
			Name: GET,
			Num:  2,
			Action: func(args []string) {
				println(client.Get(args[1]))
			},
		},
		{
			Name: PUT,
			Num:  3,
			Action: func(args []string) {
				client.Put(args[1], args[2])
			},
		},
		{
			Name: DELETE,
			Num:  2,
			Action: func(args []string) {
				client.Delete(args[1])
			},
		},
	}

	for line := readLine(); !strings.EqualFold(line, EXIT); {
		args := parseArgs(line)
		if len(args) > 0 {
			name := args[0]
			done := false
			for _, command := range commands {
				if command.Name != name {
					continue
				}

				done = true
				if len(args) != command.Num {
					println("wrong arguments")
					continue
				}
				command.Action(args)
			}

			if !done {
				println("command not found")
			}
		}
		line = readLine()
	}
}
