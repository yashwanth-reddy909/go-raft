package server

import (
	"fmt"
	"strings"
	"yashwanthreddy-909/go-raft/utils"
)

const (
	ServerFile = "server.txt"
)

func RegisterServer(serverName, serverPort string) error {
	// write the server details to the file
	err := utils.CreateFileIfNotExists(ServerFile)
	if err != nil {
		return err
	}

	serverText := fmt.Sprintf("%s:%s\n", serverName, serverPort)
	err = utils.WriteToFile(ServerFile, serverText)
	if err != nil {
		return err
	}

	fmt.Printf("Server %s registered on port %s\n", serverName, serverPort)
	return nil
}

func GetRegisteredServersAddr() ([]string, error) {
	// read the server details from the file
	lines, err := utils.ReadFile(ServerFile)
	if err != nil {
		return nil, err
	}

	hosts := make([]string, 0)
	// convert to local host:port format
	for _, line := range lines {
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}

		hostPort := fmt.Sprintf("localhost:%s", parts[1])
		hosts = append(hosts, hostPort)
	}

	return hosts, nil
}
