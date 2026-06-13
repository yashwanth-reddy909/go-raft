package server

type Request struct {
	Type    string
	Payload any
}

type SyncRequest struct {
	LeaderId      string
	CurrentTerm   int
	CurrentCommit int
	suffix        []string
}

type SyncResponse struct {
	Success bool
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
