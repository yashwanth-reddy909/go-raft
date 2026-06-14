package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"yashwanthreddy-909/go-raft/constants"
	"yashwanthreddy-909/go-raft/server/db"
	"yashwanthreddy-909/go-raft/utils"
)

const (
	BroadcastPeriod = 3
)

type PeerData struct {
	VotesReceived   map[string]bool
	PeerConnections map[string]net.Conn
	AckLength       map[string]int
}

func NewPeerData() PeerData {
	return PeerData{
		VotesReceived:   make(map[string]bool),
		PeerConnections: make(map[string]net.Conn),
		AckLength:       make(map[string]int),
	}
}

type Server struct {
	Name            string
	Port            string
	Role            constants.Role
	LeaderNodeID    string
	ElectionModule  *Election
	Logs            []string
	PeerData        PeerData
	Term            int
	CommitLogsIndex int
	VotedFor        string
	Db              *db.Database

	mu           sync.Mutex
	commitSignal chan struct{}
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
	if conn, ok := s.PeerData.PeerConnections[peerAddr]; ok {
		return conn, nil
	}

	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, err
	}

	s.PeerData.PeerConnections[peerAddr] = conn
	go s.HandleConnection(conn)
	return conn, nil
}

func (s *Server) DestoryConn(peerAddr string) {
	if conn, ok := s.PeerData.PeerConnections[peerAddr]; ok {
		conn.Close()
		delete(s.PeerData.PeerConnections, peerAddr)
	}
}

func (s *Server) FollowerAddr() string {
	return fmt.Sprintf("localhost:%s", s.Port)
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
		return SyncResponse{FollowerAddr: s.FollowerAddr(), Term: s.Term, Success: false}
	}

	if syncPayload.CurrentTerm > s.Term {
		s.Term = syncPayload.CurrentTerm
		s.Role = constants.Follower
		s.VotedFor = ""
	}

	if s.Role == constants.Leader {
		fmt.Printf("Received sync request from leader %s, but current server is already a leader\n", syncPayload.LeaderId)
		return SyncResponse{FollowerAddr: s.FollowerAddr(), Term: s.Term, Success: false}
	}

	s.Term = syncPayload.CurrentTerm
	s.Role = constants.Follower
	s.appendEntries(syncPayload.PrefixLength, syncPayload.CurrentCommit, syncPayload.Suffix)

	return SyncResponse{FollowerAddr: s.FollowerAddr(), Term: s.Term, Ack: len(s.Logs), Success: true}
}

func (s *Server) HandleSyncResponse(payload string) {
	fmt.Printf("Received sync response: %s\n", payload)
	var resp SyncResponse
	err := json.Unmarshal([]byte(payload), &resp)
	if err != nil {
		fmt.Printf("Error unmarshaling sync response payload: %v\n", err)
		return
	}

	if resp.Term > s.Term {
		s.Term = resp.Term
		s.Role = constants.Follower
		s.VotedFor = ""
		s.ElectionModule.ElectionTicker.Reset(s.ElectionModule.ElectionDuration)
		return
	}

	if s.Role != constants.Leader || resp.Term != s.Term || resp.FollowerAddr == "" {
		return
	}

	if resp.Success {
		s.PeerData.AckLength[resp.FollowerAddr] = resp.Ack
		s.advanceCommitIndex()
		return
	}

	if s.PeerData.AckLength[resp.FollowerAddr] > 0 {
		s.PeerData.AckLength[resp.FollowerAddr]--
	}
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

	if voteReq.LogIndex < len(s.Logs) {
		fmt.Printf("Received vote request with lower log index: %d, current log index: %d\n", voteReq.LogIndex, len(s.Logs))
		return VoteResponse{VoteGranted: false}
	}

	s.VotedFor = voteReq.CandidateId
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

	s.CheckElectionResults()
	s.PeerData.VotesReceived[voteReq.NodeId] = voteReq.VoteGranted
}

// majority returns the number of votes/acks needed for quorum,
// counting this node plus its peers.
func (s *Server) majority() int {
	return (len(s.GetPeerAddr())+1)/2 + 1
}

func (s *Server) CheckElectionResults() {
	if s.Role == constants.Leader {
		return
	}

	var totalVotes = 1
	for server := range s.PeerData.VotesReceived {
		if s.PeerData.VotesReceived[server] {
			totalVotes += 1
		}
	}
	if totalVotes >= s.majority() {
		fmt.Println("I won the election. New leader: ", s.Name, " Votes received: ", totalVotes)
		s.Role = constants.Leader
		s.LeaderNodeID = s.Name
		s.PeerData.VotesReceived = make(map[string]bool)
		s.PeerData.AckLength = make(map[string]int)
		s.ElectionModule.ElectionTicker.Stop()
		s.syncUp()
	}
}

// prefixLength - index up to which logs assumed match (consistency check)
func (s *Server) appendEntries(prefixLength int, commitLength int, suffix []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(suffix) > 0 && prefixLength < len(s.Logs) {
		s.Logs = s.Logs[:prefixLength]
	}

	if prefixLength+len(suffix) > len(s.Logs) {
		newEntries := suffix[len(s.Logs)-prefixLength:]
		s.Logs = append(s.Logs, newEntries...)
	}

	if commitLength > s.CommitLogsIndex {
		s.CommitLogsIndex = commitLength
		s.notifyCommitLocked()
	}
}

// advanceCommitIndex moves CommitLogsIndex forward while a majority of
// peers have acknowledged a log length beyond the current commit point.
func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.CommitLogsIndex < len(s.Logs) {
		acked := 1 // self
		for _, ack := range s.PeerData.AckLength {
			if ack > s.CommitLogsIndex {
				acked++
			}
		}
		if acked < s.majority() {
			break
		}
		s.CommitLogsIndex++
		s.notifyCommitLocked()
	}
}

// notifyCommitLocked wakes any goroutines blocked in waitForCommit.
// Caller must hold s.mu.
func (s *Server) notifyCommitLocked() {
	if s.commitSignal != nil {
		close(s.commitSignal)
		s.commitSignal = nil
	}
}

// waitForCommit blocks until the log entry at index (1-based, i.e.
// len(s.Logs) at append time) has been replicated to a majority of nodes.
func (s *Server) waitForCommit(index int) {
	for {
		s.mu.Lock()
		if s.CommitLogsIndex >= index {
			s.mu.Unlock()
			return
		}
		if s.commitSignal == nil {
			s.commitSignal = make(chan struct{})
		}
		ch := s.commitSignal
		s.mu.Unlock()
		<-ch
	}
}

// replicateLog sends the current log suffix to every peer via SyncCommand.
// Acks are processed asynchronously by HandleSyncResponse, which advances
// CommitLogsIndex once a majority of peers have caught up.
func (s *Server) replicateLog() {
	for _, peer := range s.GetPeerAddr() {
		go s.sendSyncRequest(peer)
	}
}

func (s *Server) HandleCommand(payload string) (string, error) {
	fmt.Printf("Received client command: %s\n", payload)
	var req ClientCommand
	err := json.Unmarshal([]byte(payload), &req)
	if err != nil {
		fmt.Printf("Error unmarshaling client request payload: %v\n", err)
		return "", err
	}

	switch req.coomandType {
	case string(constants.Get):
		value, err := s.Db.GetKey(req.key)
		if err != nil {
			fmt.Printf("Error reading from database: %v\n", err)
			return "", err
		}

		return value, nil
	case string(constants.Put):
		log, err := ToLog(req)
		if err != nil {
			fmt.Printf("converting to log: %v\n", err)
			return "", err
		}

		s.mu.Lock()
		s.Logs = append(s.Logs, log)
		index := len(s.Logs)
		s.mu.Unlock()

		s.replicateLog()
		s.advanceCommitIndex() // covers single-node clusters, where self is already a majority
		s.waitForCommit(index)

		err = s.Db.SetKey(req.key, req.value)
		if err != nil {
			fmt.Printf("Error writing to database: %v\n", err)
			return "", err
		}
		return "", nil
	}

	return "", fmt.Errorf("unknown command type: %s", req.coomandType)
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
		case string(constants.SyncResponse):
			s.HandleSyncResponse(payload)
		case string(constants.ClientCommand):
			if s.Role != constants.Leader {
				fmt.Fprintf(conn, "executing on wrong role")
				continue
			}

			s.HandleCommand(payload)
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
	s.PeerData.VotesReceived = make(map[string]bool)

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
				LogIndex:    len(s.Logs),
			}

			payload, err := json.Marshal(voteReq)
			if err != nil {
				fmt.Printf("Error marshaling vote request: %v\n", err)
				return
			}
			fmt.Fprintf(conn, "%s:%s\n", constants.VoteRequest, payload)
		}(peer)
	}

	// if there is only one node
	// it can directly become the master
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

	prefixLength := min(s.PeerData.AckLength[shost], len(s.Logs))

	syncReq := SyncRequest{
		LeaderId:      fmt.Sprintf("%s:%s", s.Name, s.Port),
		CurrentTerm:   s.Term,
		CurrentCommit: s.CommitLogsIndex,
		PrefixLength:  prefixLength,
		Suffix:        s.Logs[prefixLength:],
	}

	payload, err := json.Marshal(syncReq)
	if err != nil {
		fmt.Printf("Error marshaling sync request: %v\n", err)
		return
	}
	fmt.Fprintf(conn, "%s:%s\n", constants.SyncCommand, payload)
}

// ticker that leader need to call
// to keep every follower intact as a heart beat
func (s *Server) syncUp() {
	ticker := time.NewTicker(BroadcastPeriod * time.Second)
	for range ticker.C {
		if s.Role != constants.Leader {
			ticker.Stop()
			return
		}

		allServers := s.GetPeerAddr()
		for _, shost := range allServers {
			s.sendSyncRequest(shost)
		}
	}
}

func (s *Server) Register() error {
	return RegisterServer(s.Name, s.Port)
}
