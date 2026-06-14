package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"yashwanthreddy-909/go-raft/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: client <host:port>")
		return
	}

	c, err := client.Connect(os.Args[1])
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		return
	}
	defer c.Close()

	fmt.Println("Connected to", os.Args[1])
	fmt.Println("Commands: get <key> | put <key> <value> | exit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}

		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		switch strings.ToLower(fields[0]) {
		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}

			value, err := c.Get(fields[1])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(value)
		case "put":
			if len(fields) != 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}

			if err := c.Put(fields[1], fields[2]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command:", fields[0])
		}
	}
}
