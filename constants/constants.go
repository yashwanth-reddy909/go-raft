package constants

// type ClientType string

// const (
// 	Server ClientType = "server"
// 	Client ClientType = "client"
// )

type Command string

const (
	RegisterCommand string = "register"
	SyncCommand     string = "sync"
	VoteRequest     string = "vote_request"
	VoteResponse    string = "vote_response"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)
