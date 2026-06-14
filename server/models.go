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
	CommandType string `json:"commandType"`
	Key         string `json:"key"`
	Value       string `json:"value"`
}

type ClientResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value"`
	Error   string `json:"error"`
}

type Log struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
