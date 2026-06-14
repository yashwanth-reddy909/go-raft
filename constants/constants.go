package constants

// type ClientType string

// const (
// 	Server ClientType = "server"
// 	Client ClientType = "client"
// )

type Command string

const (
	ClientCommand  string = "client_command"
	ClientResponse string = "client_response"
	SyncCommand    string = "sync"
	VoteRequest    string = "vote_request"
	VoteResponse   string = "vote_response"
	SyncResponse   string = "sync_response"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type ClientCommandType string

const (
	Get ClientCommandType = "get"
	Put ClientCommandType = "put"
)
