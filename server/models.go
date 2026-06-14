package server

type Request struct {
	Type    string
	Payload any
}

type SyncRequest struct {
	LeaderId      string
	CurrentTerm   int
	CurrentCommit int
	PrefixLength  int
	Suffix        []string
}

type SyncResponse struct {
	FollowerAddr string
	Term         int
	Ack          int
	Success      bool
}

type VoteRequest struct {
	CandidateId string
	Term        int
	LogIndex    int
}

type VoteResponse struct {
	NodeId      string
	VoteGranted bool
}

type ClientCommand struct {
	coomandType string
	key         string
	value       string
}

type Log struct {
	key   string
	value string
}
