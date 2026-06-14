package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"

	"yashwanthreddy-909/go-raft/constants"
	"yashwanthreddy-909/go-raft/server"
	"yashwanthreddy-909/go-raft/server/db"
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
	election := server.NewElection(electionInterval)

	fmt.Println("Election time set to:", electionInterval)

	database, err := db.NewDatabase()
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}

	server := &server.Server{
		Name:           serverName,
		Port:           serverPort,
		Role:           constants.Follower,
		ElectionModule: election,
		PeerData:       server.NewPeerData(),
		Db:             database,
	}

	// start the election ticker
	go server.GoElection()

	// register
	err = server.Register()
	if err != nil {
		fmt.Printf("registering server issue: %v\n", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go server.HandleConnection(conn)
	}
}
