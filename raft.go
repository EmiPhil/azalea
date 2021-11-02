package azalea

import (
	"fmt"
	"log"
	"math/rand"
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

const DebugCM = 1

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

	id    int   // id address of this CM
	peers []int // list of the id addresses of our peers in the cluster

	server *Server

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers
	state              CMState
	electionResetEvent time.Time
}

// dlog logs a debugging message is DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and server. The ready channel signals the CM
// that all peers are connected, and it's safe to start its state machine.
func NewConsensusModule(id int, peers []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peers = peers
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		// The CM is quiescent until ready is signaled; then, it starts a countdown for leader election.
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

// runElectionTimer runs in the background constantly to determine if we should try to run a new election. The current
// leader of the cluster will send heartbeats to reset the timer, and if we don't hear from them in time we trigger a new
// election and offer ourselves as a candidate.
func (cm *ConsensusModule) runElectionTimer() {
	timeout := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeout, termStarted)

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
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
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

// Report reports the state of the CM
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop stops this CM, cleaning up its state. The method returns quickly, but it
// may take up to ~election timeout for all goroutines to exit.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
}

//RequestVote RPC. This code is requested by some other server who is running an election
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.Candidate) {
		reply.VoteGranted = true
		cm.votedFor = args.Candidate
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntries RPC. The leader is sending us a new log entry or a heartbeat
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
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
	cm.dlog("... AppendEntries reply: %+v", reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// The election timeout is randomized to avoid multidle servers declaring themselves candidates at the same time.
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
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	votesReceived := 1

	// Send RequestVote RPCs to peers
	for _, peerId := range cm.peers {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:      savedCurrentTerm,
				Candidate: cm.id,
			}
			var reply RequestVoteReply

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			if err := cm.server.Call(peerId, CMRequestVote, args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived++
						if votesReceived*2 > len(cm.peers)+1 {
							// Won the election!
							cm.dlog("wins election with %d votes", votesReceived)
							cm.becomeLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// Run another election timer in case this election is not successful
	go cm.runElectionTimer()
}

// becomeFollower makes this cm a follower and resets its state.
// Expects mu to be locked.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower")
	cm.dlog("term=%d, log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// becomeLeader makes this cm a leader and begins sending heartbeats.
// Expects mu to be locked.
func (cm *ConsensusModule) becomeLeader() {
	cm.dlog("becomes Leader")
	cm.dlog("term=%d, log=%v", cm.currentTerm, cm.log)
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

	for _, peerId := range cm.peers {
		args := AppendEntriesArgs{
			Term:   cm.currentTerm,
			Leader: cm.id,
		}
		go func(peerId int) {
			cm.dlog("sending heartbeat to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, CMAppendEntries, args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}
