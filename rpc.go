package azalea

// Raft RPC requests

type rpcCommand string

const (
	CMRequestVote        rpcCommand = "ConsensusModule.RequestVote"
	CMRequestVoteReply              = "ConsensusModule.RequestVoteReply"
	CMAppendEntries                 = "ConsensusModule.AppendEntries"
	CMAppendEntriesReply            = "ConsensusModule.AppendEntriesReply"
)

// RequestVoteArgs represents the data needed when we become a candidate to be the next leader
type RequestVoteArgs struct {
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the reply that we expect from our peers after our election campaign
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs represents the data we send to add entries to the followers
type AppendEntriesArgs struct {
	Term   int
	Leader int
}

// AppendEntriesReply represents the data we receive from an AppendEntries request
type AppendEntriesReply struct {
	Term    int
	Success bool
}
