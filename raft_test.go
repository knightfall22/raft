package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader(5)
}

func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	ogLeader, ogTerm := h.CheckSingleLeader(5)

	h.DisconnectPeer(ogLeader)
	sleepMs(350)

	newLeader, newTerm := h.CheckSingleLeader(5)

	if newLeader == ogLeader {
		t.Errorf("want new leader to be different from original leader")
	}

	if ogTerm == newTerm {
		t.Errorf("want newTerm <= originalTerm, got %d and %d", newTerm, ogTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	ogLeader, _ := h.CheckSingleLeader(5)

	h.DisconnectPeer(ogLeader)
	otherId := (ogLeader + 1) % 3
	h.DisconnectPeer(otherId)

	//no qurom(no majority)
	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect one other server; now we'll have quorum.
	h.ReconnectPeer(otherId)
	h.CheckSingleLeader(5)
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)
	//	Disconnect all servers from the start. There will be no leader.
	for id := range 3 {
		h.DisconnectPeer(id)
	}

	h.CheckNoLeader()

	// Reconnect all servers. A leader will be found.
	for id := range 3 {
		h.ReconnectPeer(id)
	}

	h.CheckSingleLeader(5)
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	ogLeader, _ := h.CheckSingleLeader(5)

	h.DisconnectPeer(ogLeader)
	sleepMs(350)

	newLeader, newTerm := h.CheckSingleLeader(5)

	h.ReconnectPeer(ogLeader)
	sleepMs(150)

	againLeader, againTerm := h.CheckSingleLeader(5)

	if newLeader != againLeader {
		t.Errorf("again leader id got %d; want %d", againLeader, newLeader)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}

}
func TestElectionLeaderDisconnectThenReconnectWithLeak(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	ogLeader, _ := h.CheckSingleLeader(5)

	h.DisconnectPeer(ogLeader)
	sleepMs(350)

	newLeader, newTerm := h.CheckSingleLeader(5)

	h.ReconnectPeer(ogLeader)
	sleepMs(150)

	againLeader, againTerm := h.CheckSingleLeader(5)

	if newLeader != againLeader {
		t.Errorf("again leader id got %d; want %d", againLeader, newLeader)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}

}

func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	ogLeader, ogTerm := h.CheckSingleLeader(5)

	otherId := (ogLeader + 1) % 3
	h.DisconnectPeer(otherId)
	sleepMs(650)
	h.ReconnectPeer(otherId)
	sleepMs(150)

	// We can't have an assertion on the new leader id here because it depends
	// on the relative election timeouts. We can assert that the term changed,
	// however, which implies that re-election has occurred.
	_, newTerm := h.CheckSingleLeader(5)
	if newTerm <= ogTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, ogTerm)
	}
}

func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	for range 5 {
		leader, _ := h.CheckSingleLeader(5)

		h.DisconnectPeer(leader)
		otherId := (leader + 1) % 3
		h.DisconnectPeer(otherId)
		sleepMs(350)
		h.CheckNoLeader()

		//Reconnecte both
		h.ReconnectPeer(leader)
		h.ReconnectPeer(otherId)
		sleepMs(150)

	}
}

func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(3)

	tlog("submitting 42 to %d", leader)
	isLeader := h.SubmitToServer(leader, 42)

	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", leader)
	}

	sleepMs(150)

	h.CheckCommittedN(42, 3)
}

func TestSubmitNonLeader(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(3)

	sid := (leader + 1) % 3

	isLeader := h.SubmitToServer(sid, 42)
	tlog("submitting 42 to %d", sid)
	if isLeader {
		t.Errorf("want id=%d !leader, but it is", sid)
	}

	sleepMs(10)

}

func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(3)

	values := []int{10, 20, 55}

	for _, v := range values {
		isLeader := h.SubmitToServer(leader, v)
		tlog("submitting %d to %d", v, leader)

		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", leader)
		}

		sleepMs(100)
	}

	sleepMs(150)

	nc, i1 := h.CheckCommitted(10)
	_, i2 := h.CheckCommitted(20)

	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}

	if i1 > i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}

	_, i3 := h.CheckCommitted(55)
	if i2 > i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}

func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(3)

	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 20)

	sleepMs(250)
	h.CheckCommittedN(20, 3)

	//Disconnect a random server from the cluster
	randServer := (leader + 1) % 3
	h.DisconnectPeer(randServer)
	sleepMs(250)

	//Submit a new command to the leader; then check n - 1 servers if the command committed
	h.SubmitToServer(leader, 30)
	sleepMs(250)
	h.CheckCommittedN(30, 2)

	//Reconnect peer then wait a bit; note: if 30 is commited given Raft's log completion property 10,20 and 30 are committed.
	h.ReconnectPeer(randServer)
	sleepMs(200)
	h.CheckSingleLeader(10)

	sleepMs(150)
	h.CheckCommittedN(30, 3)
}

func TestNoCommitWithNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, leaderTerm := h.CheckSingleLeader(3)

	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 20)

	sleepMs(250)
	h.CheckCommittedN(20, 3)

	peer1 := (leader + 1) % 3
	peer2 := (leader + 2) % 3

	h.DisconnectPeer(peer1)
	h.DisconnectPeer(peer2)
	sleepMs(250)

	h.SubmitToServer(leader, 30)
	sleepMs(200)
	h.CheckNotCommitted(30)

	// Reconnect both other servers, we'll have quorum now.
	h.ReconnectPeer(peer1)
	h.ReconnectPeer(peer2)
	sleepMs(600)

	h.CheckNotCommitted(30)

	// A new leader will be elected. It could be a different leader, even though
	// the original's log is longer, because the two reconnected peers can elect
	// each other.
	newLeader, newLeaderTerm := h.CheckSingleLeader(3)
	if newLeaderTerm == leaderTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", leaderTerm)
	}

	// But new values will be committed for sure...
	values := []int{40, 50, 60}

	for _, v := range values {
		h.SubmitToServer(newLeader, v)
	}
	sleepMs(300)

	for _, v := range values {
		h.CheckCommittedN(v, 3)
	}
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(3)

	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 20)

	sleepMs(150)
	h.CheckCommittedN(20, 5)

	// Leader disconnected...
	h.DisconnectPeer(leader)
	sleepMs(100)

	h.SubmitToServer(leader, 30)
	sleepMs(100)

	h.CheckNotCommitted(30)

	newLeader, _ := h.CheckSingleLeader(3)

	h.SubmitToServer(newLeader, 40)
	sleepMs(250)
	h.CheckCommittedN(40, 4)

	// Reconnect old leader and let it settle. The old leader shouldn't be the one
	// winning.
	h.ReconnectPeer(leader)
	sleepMs(600)

	newLeader, _ = h.CheckSingleLeader(3)
	if newLeader == leader {
		t.Errorf("got finalLeaderId==origLeaderId==%d, want them different", leader)
	}

	h.SubmitToServer(newLeader, 50)
	sleepMs(150)
	h.CheckCommittedN(40, 5)
	h.CheckCommittedN(50, 5)
	sleepMs(150)

	h.CheckNotCommitted(30)
}
