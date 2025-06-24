package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu sync.Mutex

	// cluster is a list of all the raft servers participating in a cluster.
	cluster []*Server

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	// commitChans contains a comit channel per server in cluster
	commitChans []chan CommitEntry

	// commits at index i holds the sequence of commits made by server i so far.
	// It is populated by goroutines that listen on the corresponding commitChans
	// channel.
	commits [][]CommitEntry

	n int
	t *testing.T
}

// Creates a new test harness, initializes n server connected to each other
func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	connected := make([]bool, n)
	ready := make(chan any)

	// Create all Servers in this cluster, assign ids and peer ids.
	for id := range n {
		peerIds := make([]int, 0)

		for p := range n {
			if p != id {
				peerIds = append(peerIds, p)
			}
		}

		commitChans[id] = make(chan CommitEntry)
		ns[id] = NewServer(id, peerIds, ready, commitChans[id])
		ns[id].Serve()
	}

	for id := range n {
		for p := range n {
			if p != id {
				ns[id].ConnectToPeer(p, ns[p].GetListenAddr())
			}
		}
		connected[id] = true
	}

	close(ready)

	h := &Harness{
		cluster:     ns,
		t:           t,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		n:           n,
	}

	for id := range n {
		go h.collectCommits(id)
	}

	return h
}

// Shutdown shuts down all the servers in the harness and waits for them to
// stop running.
func (h *Harness) Shutdown() {
	for id := range h.n {
		h.cluster[id].DisconnectAll()
		h.connected[id] = false
	}

	for id := range h.n {
		h.cluster[id].Shutdown()
	}
}

// DisconnectPeer disconnects a server from all other servers in the cluster.
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)

	h.cluster[id].DisconnectAll()
	for p := range h.n {
		if p != id {
			h.cluster[p].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

// ReconnectPeer connects a server to all other servers in the cluster.
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for p := range h.cluster {
		if p != id {
			//Connect sever at `id` to a peer `p`
			if err := h.cluster[id].ConnectToPeer(p, h.cluster[p].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}

			//connect peer at`p` to `id`
			if err := h.cluster[p].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (h *Harness) CheckSingleLeader(retries int) (int, int) {
	for range retries {
		leaderId := -1
		leaderTerm := -1

		for id := range h.cluster {
			if h.connected[id] {
				_, term, isLeader := h.cluster[id].cm.Report()

				if isLeader {
					if leaderId < 0 {
						leaderId = id
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, id)
					}
				}
			}
		}

		if leaderId >= 0 {
			return leaderId, leaderTerm
		}

		//Given time for a leader to be elected before retrying
		time.Sleep(150 * time.Millisecond)
	}
	h.t.Fatalf("leader not found")
	return -1, -1
}

// CheckNoLeader checks that no connected server considers itself the leader.
func (h *Harness) CheckNoLeader() {
	for id := range h.cluster {
		if h.connected[id] {
			_, _, isLeader := h.cluster[id].cm.Report()

			if isLeader {
				h.t.Fatalf("server %d leader; want none", id)
			}
		}
	}
}

// CheckCommitted verifies that all connected servers have cmd committed with
// the same index. It also verifies that all commands *before* cmd in
// the commit sequence match. For this to work properly, all commands submitted
// to Raft should be unique positive ints.
// Returns the number of servers that have this command committed, and its
// log index.
func (h *Harness) CheckCommitted(cmd int) (nc, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Find the length of the commits slice for connected servers.
	commitLen := -1

	for id := range h.commits {
		if h.connected[id] {
			if commitLen >= 0 {
				// If this was set already, expect the new length to be the same.
				if len(h.commits[id]) != commitLen {
					h.t.Fatalf("commits[%d] = %d, commitsLen = %d", id, h.commits[id], commitLen)
				}
			} else {
				commitLen = len(h.commits[id])
			}
		}
	}

	// Check consistency of commits from the start and to the command we're asked
	// about. This loop will return once a command=cmd is found.
	for c := range commitLen {
		cmdAtC := -1

		for id := range h.commits {
			if h.connected[id] {
				cmdOfN := h.commits[id][c].Command.(int)
				if cmdAtC >= 0 {
					if cmdAtC != cmdOfN {
						h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, id, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}

		if cmdAtC == cmd {
			index := -1

			for id := range h.commits {
				if h.connected[id] {
					if index >= 0 && h.commits[id][c].Index != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%d][%d]", h.commits[id][c].Index, index, id, c)
					} else {
						index = h.commits[id][c].Index
					}
					nc++
				}
			}

			return nc, index
		}
	}

	// If there's no early return, we haven't found the command we were looking
	// for.
	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN verifies that cmd was committed by exactly n connected
// servers.
func (h *Harness) CheckCommittedN(cmd int, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// CheckNotCommitted verifies that no command equal to cmd has been committed
// by any of the active servers yet.
func (h *Harness) CheckNotCommitted(cmd int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id := range h.n {
		if h.connected[id] {
			for c := range len(h.commits[id]) {
				gotCmd := h.commits[id][c].Command.(int)

				if gotCmd == cmd {
					h.t.Errorf("found %d at commits[%d][%d], expected none", cmd, id, c)
				}
			}
		}
	}
}

// SubmitToServer submits the command to serverId.
func (h *Harness) SubmitToServer(serverId int, cmd any) bool {
	return h.cluster[serverId].cm.Submit(cmd)
}

// collectCommits reads channel commitChans[i] and adds all received entries
// to the corresponding commits[i]. It's blocking and should be run in a
// separate goroutine. It returns when commitChans[i] is closed.
func (h *Harness) collectCommits(id int) {
	for c := range h.commitChans[id] {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", id, c)
		h.commits[id] = append(h.commits[id], c)
		h.mu.Unlock()
	}
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
