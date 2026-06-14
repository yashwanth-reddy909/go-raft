package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"yashwanthreddy-909/go-raft/constants"
	"yashwanthreddy-909/go-raft/server"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) sendCommand(cmd server.ClientCommand) (server.ClientResponse, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return server.ClientResponse{}, err
	}

	if _, err := fmt.Fprintf(c.conn, "%s:%s\n", constants.ClientCommand, payload); err != nil {
		return server.ClientResponse{}, err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return server.ClientResponse{}, err
	}

	_, respPayload, found := strings.Cut(strings.TrimSpace(line), ":")
	if !found {
		return server.ClientResponse{}, fmt.Errorf("unexpected response: %s", line)
	}

	var resp server.ClientResponse
	if err := json.Unmarshal([]byte(respPayload), &resp); err != nil {
		return server.ClientResponse{}, err
	}

	if !resp.Success {
		return server.ClientResponse{}, fmt.Errorf("%s", resp.Error)
	}

	return resp, nil
}

// Get reads the value stored for key from the cluster leader.
func (c *Client) Get(key string) (string, error) {
	resp, err := c.sendCommand(server.ClientCommand{CommandType: string(constants.Get), Key: key})
	if err != nil {
		return "", err
	}

	return resp.Value, nil
}

// Put writes key=value through the cluster leader and waits for it to commit.
func (c *Client) Put(key, value string) error {
	_, err := c.sendCommand(server.ClientCommand{CommandType: string(constants.Put), Key: key, Value: value})
	return err
}
