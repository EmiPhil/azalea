package azalea

import (
	"bytes"
	"encoding/gob"
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

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command, and it can
// be applied to the client's state machine
type CommitEntry struct {
	// Command is the client command being committed
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index int

	// Term is the Raft term at which the client command is committed.
	Term int
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

	// storage is used to persist state
	storage Storage

	// commitChan is the channel where this CM is going to report commited log
	// entries. It's passed in by the client during construction
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines that
	// commit new entries to the log to notify that these entries may be sent on
	// commitChan
	newCommitReadyChan chan struct{}

	// triggerAEChan is an optimization to send new entries immediately instead of
	// waiting in 50ms loops
	triggerAEChan chan struct{}

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
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
func NewConsensusModule(id int, peers []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peers = peers
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage(cm.storage)
	}

	go func() {
		// The CM is quiescent until ready is signaled; then, it starts a countdown for
		// leader election.
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

// Submit appends the command to our log and returns true if we are the leader.
// Return false otherwise.
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()

	cm.dlog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}

// commitChanSender is used by the CM to signal that new entries are ready to be
// sent on the commit channel to the client. It's run in a goroutine on CM
// start-up. It updates the lastApplied state variable to know which entries were
// already sent to the client, and sends only new ones.
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// Find which entries we have to apply
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

// runElectionTimer runs in the background constantly to determine if we should
// try to run a new election. The current leader of the cluster will send
// heartbeats to reset the timer, and if we don't hear from them in time we
// trigger a new election and offer ourselves as a candidate.
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
	close(cm.newCommitReadyChan)
}

//RequestVote RPC. This code is requested by some other server who is running an election
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.Candidate) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.Candidate
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntries RPC. The leader is sending us a new log entry or a heartbeat.
// Follows figure 2 of the Raft paper.
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

		// Does our log contain an entry at PrevLogIndex whose term matches PrevLogTerm?
		// Note that in the extreme case of PrevLogIndex=-1 this is vacuously true.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between the existing
			// log starting at PrevLogIndex+1 and the new entries sent in the RPC
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			//
			// - logInsertIndex points at the end of the log, or an
			//   index where the term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the term
			//   mismatched with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			// Set commit index.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
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

// lastLogIndexAndTerm is a getter that returns the last index and term (or -1 if
// there's no log) of the cm for this server. Expects cm.mu to be locked.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
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
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				Candidate:    cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply

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
	cm.state = Leader

	for _, peerId := range cm.peers {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}

	cm.dlog("becomes Leader")
	cm.dlog("term=%d, log=%v", cm.currentTerm, cm.log)

	go func(timeout time.Duration) {
		// immediately send AEs to peers
		cm.sendAppendEntries()

		t := time.NewTimer(timeout)
		defer t.Stop()

		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				// Reset timer
				t.Stop()
				t.Reset(timeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				if !t.Stop() {
					<-t.C
				}
				t.Reset(timeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.sendAppendEntries()
			}
		}
	}(50 * time.Millisecond)
}

// sendAppendEntries sends a round of heartbeats to all peers, collects their replies, and adjusts the cm's state
func (cm *ConsensusModule) sendAppendEntries() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peers {
		go func(peerId int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				Leader:       cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()

			cm.dlog("sending append entries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			err := cm.server.Call(peerId, CMAppendEntries, args, &reply)
			if err != nil {
				cm.dlog("call to peer %v failed", peerId)
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()

			if reply.Term > savedCurrentTerm {
				cm.dlog("term out of date in heartbeat reply")
				cm.becomeFollower(reply.Term)
				return
			}

			if cm.state == Leader && savedCurrentTerm == reply.Term {
				if !reply.Success {
					// failure case first since the successful case requires much more work
					cm.nextIndex[peerId] = ni - 1
					cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
				} else {
					cm.nextIndex[peerId] = ni + len(entries)
					cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
					cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

					savedCommitIndex := cm.commitIndex
					for i := cm.commitIndex + 1; i < len(cm.log); i++ {
						if cm.log[i].Term == cm.currentTerm {
							matchCount := 1
							for _, peerId := range cm.peers {
								if cm.matchIndex[peerId] >= i {
									matchCount++
								}
							}

							// check for majority
							if matchCount*2 > len(cm.peers)+1 {
								cm.commitIndex = i
							}
						}
					}

					if cm.commitIndex != savedCommitIndex {
						cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
						cm.newCommitReadyChan <- struct{}{}
						cm.triggerAEChan <- struct{}{}
					}
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}

	if voteData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(voteData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}

	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedFor bytes.Buffer
	if err := gob.NewEncoder(&votedFor).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedFor.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}
