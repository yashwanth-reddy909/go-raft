package server

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
)

const (
	minElectionIntervalSec = 3
	maxElectionIntervalSec = 10
)


func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: server <port>")
		return
	}

	serverName := os.Args[1]
	serverPort := os.Args[2]

	if serverName == "" || serverPort == "" {
		fmt.Println("Usage: server <name> <port>")
		return
	}

	if _, err := strconv.Atoi(serverPort); err != nil {
		fmt.Printf("Invalid port number: %s\n", serverPort)
		return
	}

	// start the server
	listener, err := net.Listen("tcp", "localhost:"+serverPort)
	if err != nil {
		fmt.Printf("Error starting server: %v\n", err)
		return
	}

	defer listener.Close()

	// start the election ticker
	// randomize the election interval between min and max - so not all servers start the election at the same time
	electionInterval := rand.Intn(maxElectionIntervalSec-minElectionIntervalSec) + int(minElectionIntervalSec)
	election := NewElection(electionInterval)
	
	server := &Server{
		Name: serverName,
		Port: serverPort,
		electionModule: election,
	}

	// start the election ticker
	go server.goElection()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go server.handleConnection(conn)
	}
}
