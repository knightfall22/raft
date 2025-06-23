package raft

import (
	"log"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	// cluster is a list of all the raft servers participating in a cluster.
	cluster []*Server

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	n int
	t *testing.T
}

// Creates a new test harness, initializes n server connected to each other
func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
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

		ns[id] = NewServer(id, peerIds, ready)
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

	return &Harness{
		cluster:   ns,
		t:         t,
		connected: connected,
		n:         n,
	}
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

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
