package azalea

import (
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

// LogEntry represents the commands sent by a client to the cluster.
type LogEntry struct {
	Command interface{}
	Term    int
}

// CMState tracks which state we are currently in (the CM is itself a state machine):
type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// ConsensusModule (CM) implements a single node of Raft consensus.
//
type ConsensusModule struct {
	mu sync.Mutex // protects CM

	ip    net.IP   // ip address of this CM
	peers []net.IP // list of the ip addresses of our peers in the cluster

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    net.IP
	log         []LogEntry

	// Volatile Raft state on all servers
	state              CMState
	electionResetEvent time.Time
}

// runElectionTimer runs in the background constantly to determine if we should try to run a new election. The current
// leader of the cluster will send heartbeats to reset the timer, and if we don't hear from them in time we trigger a new
// election and offer ourselves as a candidate.
func (cm *ConsensusModule) runElectionTimer() {
	timeout := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	Log(ConsensusModuleId, Debug, "election timer started (%v), term=%d", timeout, termStarted)

	// Loop until either:
	// 1. the election timer is no longer needed, or
	// 2. the election timer expires and this CM becomes a candidate
	// In a follower, this typically keeps running in the background for the duration of the CM's lifetime
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			Log(ConsensusModuleId, Debug, "in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			Log(ConsensusModuleId, Debug, "in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for someone for the duration of the
		// timeout.
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeout {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

//RequestVote RPC. This code is requested by some other server who is running an election
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	Log(ConsensusModuleId, Debug, "RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		Log(ConsensusModuleId, Debug, "... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == nil || cm.votedFor.String() == args.Candidate) {
		reply.VoteGranted = true
		cm.votedFor = net.ParseIP(args.Candidate)
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	Log(ConsensusModuleId, Debug, "... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntries RPC. The leader is sending us a new log entry or a heartbeat
func (cm *ConsensusModule) AppendEntries(args AppendEntries, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	Log(ConsensusModuleId, Debug, "AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		Log(ConsensusModuleId, Debug, "... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	Log(ConsensusModuleId, Debug, "... AppendEntries reply: %+v", reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// The election timeout is randomized to avoid multiple servers declaring themselves candidates at the same time.
	// It is possible for that to occur but becomes less likely as subsequent elections are requested.
	if len(os.Getenv(RaftForceMoreReelection)) > 0 && rand.Intn(3) == 0 {
		return 150 * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// startElection has three jobs:
// 1. Switch the state to candidate and increment the term
// 2. Request votes from all known peers
// 3. Wait for replies to see if we got enough votes to become the leader
// expects mu to be locked
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.ip
	Log(ConsensusModuleId, Debug, "becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	votesReceived := 1

	// Send RequestVote RPCs to peers
	for _, peerIp := range cm.peers {
		go func(peerIp net.IP) {
			args := RequestVoteArgs{
				Term:      savedCurrentTerm,
				Candidate: cm.ip.String(),
			}
			var reply RequestVoteReply

			Log(ConsensusModuleId, Debug, "sending RequestVote to %d: %+v", peerIp, args)
			if err := cm.server.Call(peerIp, CMRequestVote, args, &reply); err != nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				Log(ConsensusModuleId, Debug, "received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					Log(ConsensusModuleId, Debug, "while waiting for reply, state = %v", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					Log(ConsensusModuleId, Debug, "term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					votesReceived++
					if votesReceived*2 > len(cm.peers)+1 {
						// Won the election!
						Log(ConsensusModuleId, Debug, "wins election with %d votes", votesReceived)
						cm.becomeLeader()
						return
					}
				}
			}
		}(peerIp)
	}

	// Run another election timer in case this election is not successful
	go cm.runElectionTimer()
}

// becomeFollower makes this cm a follower and resets its state.
// Expects mu to be locked.
func (cm *ConsensusModule) becomeFollower(term int) {
	Log(ConsensusModuleId, Info, "becomes Follower")
	Log(ConsensusModuleId, Debug, "term=%d, log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = nil
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// becomeLeader makes this cm a leader and begins sending heartbeats.
// Expects mu to be locked.
func (cm *ConsensusModule) becomeLeader() {
	Log(ConsensusModuleId, Info, "becomes Leader")
	Log(ConsensusModuleId, Debug, "term=%d, log=%v", cm.currentTerm, cm.log)
	cm.state = Leader

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.sendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// sendHeartbeats sends a round of heartbeats to all peers, collects their replies, and adjusts the cm's state
func (cm *ConsensusModule) sendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerIp := range cm.peers {
		args := AppendEntries{
			Term:   cm.currentTerm,
			Leader: cm.ip.String(),
		}
		go func(peerIp net.IP) {
			Log(ConsensusModuleId, Debug, "sending heartbeat to %v: ni=%d, args=%+v", peerIp.String(), 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerIp, CMAppendEntries, args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					Log(ConsensusModuleId, Debug, "term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerIp)
	}
}
