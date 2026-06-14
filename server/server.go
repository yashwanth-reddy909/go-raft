package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
	"yashwanthreddy-909/go-raft/constants"
	"yashwanthreddy-909/go-raft/utils"
)

const (
	BroadcastPeriod = 3
)

type PeerData struct {
	VotesReceived map[string]bool
}

type Server struct {
	Name            string
	Port            string
	Role            constants.Role
	LeaderNodeID    string
	ElectionModule  *Election
	LogIndex        int
	Logs            []string
	PeerConnections map[string]net.Conn
	peerData        PeerData
	Term            int
	CommitLogsIndex int
	VotedFor        string
}

func (s *Server) GetPeerAddr() []string {
	addrs, err := GetRegisteredServersAddr()
	if err != nil {
		fmt.Printf("Getting Addr issue %v", err)
		return make([]string, 0)
	}

	uniqueAddrs := utils.RemoveDuplicates(addrs)
	return utils.RemoveElement(uniqueAddrs, fmt.Sprintf("localhost:%s", s.Port))
}

func (s *Server) BuildConn(peerAddr string) (net.Conn, error) {
	if conn, ok := s.PeerConnections[peerAddr]; ok {
		return conn, nil
	}

	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, err
	}

	s.PeerConnections[peerAddr] = conn
	return conn, nil
}

func (s *Server) DestoryConn(peerAddr string) {
	if conn, ok := s.PeerConnections[peerAddr]; ok {
		conn.Close()
		delete(s.PeerConnections, peerAddr)
	}
}

func (s *Server) HandleSyncRequest(payload string) SyncResponse {
	fmt.Printf("Received sync request: %s\n", payload)
	s.ElectionModule.ElectionResetChan <- true
	var syncPayload SyncRequest
	err := json.Unmarshal([]byte(payload), &syncPayload)
	if err != nil {
		fmt.Printf("Error unmarshaling sync payload: %v\n", err)
		return SyncResponse{Success: false}
	}

	if syncPayload.CurrentTerm < s.Term {
		fmt.Printf("Received sync request with lower term: %d, current term: %d\n", syncPayload.CurrentTerm, s.Term)
		go s.StartElection()
		return SyncResponse{Success: false}
	}

	if syncPayload.CurrentTerm > s.Term {
		s.Term = syncPayload.CurrentTerm
		s.Role = constants.Follower
		s.VotedFor = ""
	}

	if s.Role == constants.Leader {
		fmt.Printf("Received sync request from leader %s, but current server is already a leader\n", syncPayload.LeaderId)
		return SyncResponse{Success: false}
	}

	// fetch those logs which are not present in the current server
	// this need to be implemented or get the records on the same log
	// go s.FetchMissingLogs(syncPayload)

	return SyncResponse{Success: true}
}

func (s *Server) HandleVoteRequest(payload string) VoteResponse {
	fmt.Printf("Received vote request: %s\n", payload)
	var voteReq VoteRequest
	err := json.Unmarshal([]byte(payload), &voteReq)
	if err != nil {
		fmt.Printf("Error unmarshaling vote request payload: %v\n", err)
		return VoteResponse{VoteGranted: false}
	}

	if voteReq.Term < s.Term {
		fmt.Printf("Received vote request with lower term: %d, current term: %d\n", voteReq.Term, s.Term)
		return VoteResponse{VoteGranted: false}
	}

	if voteReq.LogIndex < s.LogIndex {
		fmt.Printf("Received vote request with lower log index: %d, current log index: %d\n", voteReq.LogIndex, s.LogIndex)
		return VoteResponse{VoteGranted: false}
	}

	return VoteResponse{VoteGranted: true}
}

func (s *Server) HandleVoteResponse(payload string) {
	fmt.Printf("Received vote request: %s\n", payload)
	var voteReq VoteResponse
	err := json.Unmarshal([]byte(payload), &voteReq)
	if err != nil {
		fmt.Printf("Error unmarshaling vote request payload: %v\n", err)
		return
	}

	s.peerData.VotesReceived[voteReq.NodeId] = voteReq.VoteGranted
}

func (s *Server) CheckElectionResults() {
	if s.Role == constants.Leader {
		return
	}

	var totalVotes = 1
	for server := range s.peerData.VotesReceived {
		if s.peerData.VotesReceived[server] {
			totalVotes += 1
		}
	}
	allNodes := s.GetPeerAddr()
	if totalVotes >= (len(allNodes)+1)/2 || len(allNodes) == 1 {
		fmt.Println("I won the election. New leader: ", s.Name, " Votes received: ", totalVotes)
		s.Role = "leader"
		s.LeaderNodeID = s.Name
		s.peerData.VotesReceived = make(map[string]bool)
		s.ElectionModule.ElectionTicker.Stop()
		s.syncUp()
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// read the message from the conn
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading from connection: %v\n", err)
			return
		}

		message := strings.TrimSpace(string(data))
		command, payload, _ := strings.Cut(message, ":")

		// check the incoming message
		switch command {
		case string(constants.SyncCommand):
			fmt.Printf("Received sync command from %s\n", payload)
			resp := s.HandleSyncRequest(payload)
			respPayload, err := json.Marshal(resp)
			if err != nil {
				fmt.Printf("Error marshaling sync response: %v\n", err)
				continue
			}
			fmt.Fprintf(conn, "%s:%s\n", constants.SyncResponse, respPayload)
		case string(constants.VoteRequest):
			fmt.Printf("Received request vote command from %s\n", payload)
			resp := s.HandleVoteRequest(payload)
			respPayload, err := json.Marshal(resp)
			if err != nil {
				fmt.Printf("Error marshaling vote response: %v\n", err)
				continue
			}
			fmt.Fprintf(conn, "%s:%s\n", constants.VoteResponse, respPayload)
		case string(constants.VoteResponse):
			fmt.Printf("Received response vote command from %s\n", payload)
			s.HandleVoteResponse(payload)
		default:
			fmt.Printf("Unknown message type: %s\n", command)
		}
	}
}

// 1. node converts to candidate
// 2. node increments its term
// 3. node votes for itself
// 4. node sends request vote to all other nodes
func (s *Server) StartElection() {
	s.Role = constants.Candidate
	fmt.Printf("Server %s:%s starting election\n", s.Name, s.Port)
	peers := s.GetPeerAddr()

	s.Term++
	s.VotedFor = s.Name
	s.peerData.VotesReceived = make(map[string]bool)

	fmt.Printf("Requesting votes for %d\n", len(peers))

	// send vote request to all peers
	for _, peer := range peers {
		go func(peerAddr string) {
			conn, err := s.BuildConn(peerAddr)
			if err != nil {
				fmt.Printf("Error building connection to peer %s: %v\n", peerAddr, err)
				return
			}

			voteReq := VoteRequest{
				CandidateId: s.Name,
				Term:        s.Term,
				LogIndex:    s.LogIndex,
			}

			payload, err := json.Marshal(voteReq)
			if err != nil {
				fmt.Printf("Error marshaling vote request: %v\n", err)
				return
			}
			fmt.Fprintf(conn, "%s:%s\n", constants.VoteRequest, payload)
		}(peer)
	}

	s.CheckElectionResults()
}

func (s *Server) GoElection() {
	for {
		select {
		case <-s.ElectionModule.ElectionTicker.C:
			if s.Role == constants.Follower {
				fmt.Printf("Election tick for server %s:%s\n", s.Name, s.Port)
				s.StartElection()
			}
		case <-s.ElectionModule.ElectionResetChan:
			fmt.Printf("Election reset for server %s:%s\n", s.Name, s.Port)
			s.ElectionModule.ElectionTicker.Reset(s.ElectionModule.ElectionDuration)
		}
	}
}

func (s *Server) sendSyncRequest(shost string) {
	conn, err := s.BuildConn(shost)
	if err != nil {
		fmt.Printf("connection could not establish %v\n", err)
		return
	}

	syncReq := SyncRequest{
		LeaderId:      fmt.Sprintf("%s:%s", s.Name, s.Port),
		CurrentTerm:   s.Term,
		CurrentCommit: s.CommitLogsIndex,
	}

	payload, err := json.Marshal(syncReq)
	if err != nil {
		fmt.Printf("Error marshaling sync request: %v\n", err)
		return
	}
	fmt.Fprintf(conn, "%s:%s\n", constants.SyncCommand, payload)
}

func (s *Server) syncUp() {
	ticker := time.NewTicker(BroadcastPeriod * time.Second)
	for range ticker.C {
		allServers := s.GetPeerAddr()
		for _, shost := range allServers {
			s.sendSyncRequest(shost)
		}
	}
}

func (s *Server) Register() error {
	return RegisterServer(s.Name, s.Port)
}
