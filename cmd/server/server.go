package server

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
	"yashwanthreddy-909/go-raft/constants"
)

type PeerData struct {
	VotesReceived map[string]bool
}

type Server struct {
	Name            string
	Port            string
	Role            constants.Role
	electionModule  *Election
	LogIndex        int
	Logs            []string
	peerConnections map[string]net.Conn
	peerData        PeerData
	Term            int
	CommitLogsIndex int
	VotedFor        string
}

func (s *Server) BuildConn(peerAddr string) (net.Conn, error) {
	if conn, ok := s.peerConnections[peerAddr]; ok {
		return conn, nil
	}

	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, err
	}

	s.peerConnections[peerAddr] = conn
	return conn, nil
}

func (s *Server) DestoryConn(peerAddr string) {
	if conn, ok := s.peerConnections[peerAddr]; ok {
		conn.Close()
		delete(s.peerConnections, peerAddr)
	}
}

func (s *Server) HandleSyncRequest(payload any) SyncResponse {
	fmt.Printf("Received sync request: %s\n", payload)
	s.electionModule.ElectionResetChan <- true
	var syncPayload SyncRequest
	err := json.Unmarshal([]byte(payload.(string)), &syncPayload)
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

func (s *Server) HandleVoteRequest(payload any) VoteResponse {
	fmt.Printf("Received vote request: %s\n", payload)
	var voteReq VoteRequest
	err := json.Unmarshal([]byte(payload.(string)), &voteReq)
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

func (s *Server) HandleVoteResponse(payload any) {
	fmt.Printf("Received vote request: %s\n", payload)
	var voteReq VoteResponse
	err := json.Unmarshal([]byte(payload.(string)), &voteReq)
	if err != nil {
		fmt.Printf("Error unmarshaling vote request payload: %v\n", err)
		return
	}

	s.peerData.VotesReceived[voteReq.NodeId] = voteReq.VoteGranted
}

func (s *Server) checkElectionResults() {
	if s.Role == constants.Leader {
		return
	}

	var totalVotes = 0
	for server := range s.peerData.VotesReceived {
		if s.peerData.VotesReceived[server] {
			totalVotes += 1
		}
	}
	allNodes, _ := GetRegisteredServersAddr()
	if totalVotes >= (len(allNodes)+1)/2 {
		fmt.Println("I won the election. New leader: ", s.Name, " Votes received: ", totalVotes)
		s.CurrentRole = "leader"
		s.LeaderNodeId = s.ServerState.Name
		s.Peerdata.VotesReceived = make(map[string]bool)
		s.ElectionModule.ElectionTimeout.Stop()
		s.syncUp()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// read the message from the conn
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
		return
	}

	message := string(buf[:n])
	fmt.Printf("Received message: %s\n", message)
	var payload Request
	err = json.Unmarshal([]byte(message), &payload)
	if err != nil {
		fmt.Printf("Error unmarshaling message: %v\n", err)
		return
	}

	// check the incoming message
	switch payload.Type {
	case string(constants.SyncCommand):
		fmt.Printf("Received sync command from %s\n", payload.Payload)
		resp := s.HandleSyncRequest(payload.Payload)
		conn.Write([]byte(fmt.Sprintf("SyncResponse:%+v\n", resp)))
	case string(constants.VoteRequest):
		fmt.Printf("Received request vote command from %s\n", payload.Payload)
		resp := s.HandleVoteRequest(payload.Payload)
		conn.Write([]byte(fmt.Sprintf("VoteResponse:%+v\n", resp)))
	case string(constants.VoteResponse):
		fmt.Printf("Received response vote command from %s\n", payload.Payload)

	default:
		fmt.Printf("Unknown message type: %s\n", payload.Type)
	}

	// send a response back to the client
	response := fmt.Sprintf("Hello, you sent: %s", message)
	_, err = conn.Write([]byte(response))
	if err != nil {
		fmt.Printf("Error writing to connection: %v\n", err)
		return
	}
}

// 1. node converts to candidate
// 2. node increments its term
// 3. node votes for itself
// 4. node sends request vote to all other nodes
func (s *Server) StartElection() {
	s.Role = constants.Candidate
	fmt.Printf("Server %s:%s starting election\n", s.Name, s.Port)
	peers, err := GetRegisteredServersAddr()
	if err != nil {
		fmt.Printf("Error getting registered servers: %v\n", err)
		return
	}

	s.Term++
	s.VotedFor = s.Name
	s.peerData.VotesReceived = make(map[string]bool)

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
			fmt.Fprintf(conn, "RequestVote:%+v\n", voteReq)
		}(peer)
	}

	fmt.Printf("Sent the vote requests")
}

func (s *Server) goElection() {
	for {
		select {
		case <-s.electionModule.ElectionTicker.C:
			if s.Role == constants.Follower {
				fmt.Printf("Election tick for server %s:%s at %v\n", s.Name, s.Port, time.Now())
				fmt.Printf("Server %s:%s starting election\n", s.Name, s.Port)
				s.StartElection()
			}
		case <-s.electionModule.ElectionResetChan:
			fmt.Printf("Election reset for server %s:%s at %v\n", s.Name, s.Port, time.Now())
			s.electionModule.ElectionTicker.Reset(s.electionModule.ElectionDuration)
		}
	}
}
