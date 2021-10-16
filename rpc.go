package azalea

// Raft RPC requests

type RPCId int

const (
	CMRequestVote RPCId = iota
	CMRequestVoteReply
	CMAppendEntries
	CMAppendEntriesReply
)

// RequestVoteArgs represents the data needed when we become a candidate to be the next leader
type RequestVoteArgs struct {
	Term         int
	Candidate    string // ip addr
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the reply that we expect from our peers after our election campaign
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntries represents the data we send to add entries to the followers
type AppendEntries struct {
	Term   int
	Leader string // ip addr
}

// AppendEntriesReply represents the data we receive from an AppendEntries request
type AppendEntriesReply struct {
	Term    int
	Success bool
}
