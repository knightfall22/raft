package raft

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

const DebugCM = 1

// Contains state machince commands and log entry term
type LogEntry struct {
	Command any
	Term    int
}

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

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command and it can
// be applied to the client's state machine.
type CommitEntry struct {
	//Command being committed
	Command any

	//Index at which command is committed
	Index int

	//Term at which command is committed
	Term int
}

// Consensus module implements a single node of Raft consensus
type ConsensusModule struct {
	//Protect CM from concurrent access
	mu sync.Mutex

	// the server id of the CM
	id int

	//Ids of peers in our cluster
	peerIds []int

	//server is the server containing this CM. It is used to send RPC calls
	server *Server

	// storage is used to persist state.
	storage Storage

	// commitChan is the channel where this CM is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan. A goroutine monitors this channel and sends entries on
	// newCommitReadyChanWg when notified; commitChanWg is used to wait for this
	// goroutine to exit, to ensure a clean shutdown.
	newCommitReadyChan   chan struct{}
	newCommitReadyChanWg sync.WaitGroup

	// triggerAEChan is an internal notification channel used to trigger
	// sending new AEs to followers when interesting changes occurred.
	triggerAEChan chan struct{}

	//Persist Raft state on all server
	votedFor    int
	currentTerm int
	log         []LogEntry

	//volatile raft state on all servers
	commitIndex        int
	electionResetEvent time.Time
	lastApplied        int
	state              CMState

	// Volatile Raft state on leaders
	//for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex map[int]int
	//for each server, index of highest log entry known to be replicated on server
	matchIndex map[int]int
}

// Create a new CM with an id, list of peerIds, and server.
// Uses a ready channel to signal that all peers are connected and it's safe to start the state machine
func NewConsensusModule(id int, peerIds []int, server *Server, ready chan any, storage Storage, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.storage = storage
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)

	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	cm.newCommitReadyChanWg.Add(1)
	go cm.commitChanSender()
	return cm
}

// Figure 2 in paper or "Condensed summary of Raft" in notes
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

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
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {

		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()

	} else {
		reply.VoteGranted = false
	}

	cm.persistToStorage()
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)

	return nil
}

// See figure 2 in the paper "Condensed summary of Raft" in notes
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	cm.dlog("Received AppendEntries: %+v", args)

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

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm?
		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertionIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertionIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}

				if cm.log[logInsertionIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}

				logInsertionIndex++
				newEntriesIndex++
			}

			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertionIndex)
				cm.log = append(cm.log[:logInsertionIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			//Set commit log
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// No match for PrevLogIndex/PrevLogTerm. Populate
			// ConflictIndex/ConflictTerm to help the leader bring us up to date
			// quickly.
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// cm.log[PrevLogIndex].
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}

				reply.ConflictIndex = i + 1
			}
		}
	}

	cm.persistToStorage()
	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// Generate report about module. Returns id, term and Leader status
func (cm *ConsensusModule) Report() (int, int, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop stops this CM, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
func (cm *ConsensusModule) Stop() {
	cm.dlog("CM.Stop called")
	cm.mu.Lock()
	cm.state = Dead
	cm.mu.Unlock()
	cm.dlog("becomes Dead")

	// Close the commit notification channel, and wait for the goroutine that
	// monitors it to exit.
	close(cm.newCommitReadyChan)
	cm.newCommitReadyChanWg.Wait()

}

// dlog logs a debugging message if DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// dlog logs a debugging message if DebugCM > 0.
func (cm *ConsensusModule) dfatalLog(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Fatalf(format, args...)
	}
}

// Generate a random election timeout within the provided range
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.

	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// This function implements and election timer
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the CM state changes from follower/candidate or the term changes.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()

	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	//This loop continues until
	//	- We discover that the election timeout is no longer needed
	//	- the election timer expires and this CM becomes the candidate
	//This is ran in the background throughout a follower's lifetime
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
			cm.dlog("in election timer term change from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		//start an election if CM hasn't heard from the leader or voter for a candidate
		if elasped := time.Since(cm.electionResetEvent); elasped >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("becomes the candidate for term %d; log=%v", savedCurrentTerm, cm.log)

	votesReceived := 1

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply

			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dlog("state changed while waiting for reply, state = %v", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if savedCurrentTerm == reply.Term && reply.VoteGranted {
					votesReceived += 1
					if votesReceived*2 > len(cm.peerIds)+1 {
						//Majority votes gotten; Election won
						cm.dlog("wins election with %d votes", votesReceived)
						cm.startLeader()
						return
					}
				}

			}
		}()
	}

	// Run another election timer, in case this election is not successful.
	go cm.runElectionTimer()

}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastLogIndex := len(cm.log) - 1
		return lastLogIndex, cm.log[lastLogIndex].Term
	}
	return -1, -1
}

// becomeFollower makes cm a follower and resets its state.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.currentTerm = term
	cm.state = Follower
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// Converts cm to leader and send heart beats
// Expects cm.mu to be locked.
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	// This goroutine runs in the background and sends AEs to peers:
	// * Whenever something is sent on triggerAEChan
	// * ... Or every 50 ms, if no events occur on triggerAEChan
	go func(heartbeat time.Duration) {
		cm.leaderSendHeartbeats()

		t := time.NewTimer(50 * time.Millisecond)
		defer t.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			doSend := false

			select {
			case <-t.C:
				doSend = true
				t.Stop()
				// Reset timer to fire again after heartbeatTimeout.
				t.Reset(heartbeat)
			case _, ok := <-cm.triggerAEChan:
				if ok {

					doSend = true
				} else {
					return
				}

				// Reset timer for heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeat)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}

				cm.mu.Unlock()
				cm.leaderSendHeartbeats()
			}
		}
	}(50 * time.Millisecond)
}

// Let the client submit new commands
func (cm *ConsensusModule) Submit(command any) int {
	cm.mu.Lock()

	cm.dlog("Submit received by %v: %v", cm.state, command)

	if cm.state == Leader {
		submittedIndex := len(cm.log)
		cm.log = append(cm.log, LogEntry{
			Command: command,
			Term:    cm.currentTerm,
		})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return submittedIndex
	}

	cm.mu.Unlock()
	return -1
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}

	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1

			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}

			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         cm.currentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply

			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

						savedCommitedIndex := cm.commitIndex

						//Update commit index using a peerIds matchIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm { //Matches with leader
								matchCount := 1

								//If Peers match index is >= our current commit index
								//Increment matchCount. If matchCounts matches with a majority update commit index
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}

								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}

						if cm.commitIndex != savedCommitedIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexTerm := -1

							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexTerm = i
									break
								}
							}

							if lastIndexTerm >= 0 {
								cm.nextIndex[peerId] = lastIndexTerm + 1
							} else {
								cm.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
						cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)

					}
				}
			}
		}()
	}
}

// commitChanSender is responsible for sending committed entries on
// cm.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; cm.commitChan may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newCommitReadyChan is
// closed.
func (cm *ConsensusModule) commitChanSender() {
	defer cm.newCommitReadyChanWg.Done()

	for range cm.newCommitReadyChan {
		//find which entries to apply
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

		cm.persistToStorage()
	}

	cm.dlog("commitChanSender done")
}

// restoreFromStorage restores the persistent state of this CM from storage.
// It should be called during constructor, before any concurrency concerns.
func (cm *ConsensusModule) restoreFromStorage() {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))

		if err := d.Decode(&cm.currentTerm); err != nil {
			cm.dfatalLog("An error has occured: %v", err)
		}
	} else {
		cm.dfatalLog("currentTerm not found in storage")
	}

	if votedFor, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedFor))

		if err := d.Decode(&cm.votedFor); err != nil {
			cm.dfatalLog("An error has occured: %v", err)
		}
	} else {
		cm.dfatalLog("votedFor not found in storage")
	}

	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))

		if err := d.Decode(&cm.log); err != nil {
			cm.dfatalLog("An error has occured: %v", err)
		}
	} else {
		cm.dfatalLog("log not found in storage")
	}
}

// persistToStorage saves all of CM's persistent state in cm.storage.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		cm.dfatalLog("An error has occured: %v", err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedFor bytes.Buffer
	if err := gob.NewEncoder(&votedFor).Encode(cm.votedFor); err != nil {
		cm.dfatalLog("An error has occured: %v", err)
	}
	cm.storage.Set("votedFor", votedFor.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		cm.dfatalLog("An error has occured: %v", err)
	}
	cm.storage.Set("log", logData.Bytes())

}
