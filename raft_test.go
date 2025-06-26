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

	if isLeader == -1 {
		t.Errorf("want id=%d leader, but it's not", leader)
	}

	sleepMs(150)

	h.CheckCommittedN(42, 3)
}

func TestCommitAfterCallDrops(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader(10)
	h.PeerDropCallsAfterN(lid, 2)
	h.SubmitToServer(lid, 99)
	sleepMs(30)
	h.PeerDontDropCalls(lid)

	sleepMs(60)
	h.CheckCommittedN(99, 3)
}

func TestSubmitNonLeader(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(3)

	sid := (leader + 1) % 3

	isLeader := h.SubmitToServer(sid, 42)
	tlog("submitting 42 to %d", sid)
	if isLeader != -1 {
		t.Errorf("want !leader, but it is=%d", sid)
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

		if isLeader == -1 {
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

func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 10)
	sleepMs(350)

	h.CheckCommitted(10)

	follower := (leader + 1) % 3
	h.CrashPeer(follower)

	sleepMs(350)
	h.CheckCommitted(10)

}

func TestCrashThenRestartFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 20)
	h.SubmitToServer(leader, 30)

	vals := []int{10, 20, 30}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	//crash a peer
	h.CrashPeer((leader + 1) % 3)
	sleepMs(250)

	//make a new submission
	h.SubmitToServer(leader, 40)
	sleepMs(350)
	h.CheckCommittedN(40, 2)

	//restart peer and check if 40 is committed on all servers in the cluster
	h.RestartPeer((leader + 1) % 3)

	sleepMs(650)

	h.CheckCommittedN(40, 3)

}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 20)
	h.SubmitToServer(leader, 30)

	vals := []int{10, 20, 30}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	//crash a peer
	h.CrashPeer(leader)
	sleepMs(250)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}

	h.RestartPeer(leader)
	sleepMs(650)

	h.CheckCommittedN(30, 3)

}

func TestCrashThenRestartAll(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 20)
	h.SubmitToServer(leader, 30)

	vals := []int{10, 20, 30}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	for id := range 3 {
		h.CrashPeer(id)
	}
	sleepMs(650)

	for id := range 3 {
		h.RestartPeer(id)
	}

	sleepMs(750)

	leader, _ = h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 40)
	sleepMs(350)

	h.CheckCommittedN(40, 3)

}

func TestReplaceMultipleLogEntries(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	ogLeader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(ogLeader, 10)
	h.SubmitToServer(ogLeader, 20)

	sleepMs(350)
	h.CheckCommittedN(20, 3)

	//disconnect original leader
	h.DisconnectPeer(ogLeader)
	sleepMs(10)

	// Submit a few entries to the original leader; it's disconnected, so they
	// won't be replicated.
	h.SubmitToServer(ogLeader, 21)
	sleepMs(5)
	h.SubmitToServer(ogLeader, 22)
	sleepMs(5)
	h.SubmitToServer(ogLeader, 23)
	sleepMs(5)
	h.SubmitToServer(ogLeader, 24)
	sleepMs(5)

	newLeader, _ := h.CheckSingleLeader(5)
	// Submit entries to new leader -- these will be replicated.
	h.SubmitToServer(newLeader, 30)
	sleepMs(5)
	h.SubmitToServer(newLeader, 40)
	sleepMs(5)
	h.SubmitToServer(newLeader, 50)
	sleepMs(5)
	h.SubmitToServer(newLeader, 60)
	sleepMs(250)

	h.CheckNotCommitted(21)
	h.CheckCommittedN(60, 2)

	// Crash/restart new leader to reset its nextIndex, to ensure that the new
	// leader of the cluster (could be the third server after elections) tries
	// to replace the original's servers unreplicated entries from the very end.
	h.CrashPeer(newLeader)
	sleepMs(60)
	h.RestartPeer(newLeader)
	sleepMs(350)

	finalLeader, _ := h.CheckSingleLeader(5)
	h.ReconnectPeer(ogLeader)
	sleepMs(400)

	// Submit another entry; this is because leaders won't commit entries from
	// previous terms (paper 5.4.2) so the 40,50,60 may not be committed everywhere
	// after the restart before a new command comes it.
	h.SubmitToServer(finalLeader, 70)
	sleepMs(250)

	// At this point, 70 and 60 should be replicated everywhere; 21 won't be.
	h.CheckNotCommitted(21)
	h.CheckCommittedN(70, 3)
	h.CheckCommittedN(60, 3)
}

// Tests cases when a command is replicated to a majority of servers but not committed
func TestCrashAfterSubmit(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Wait for a leader to emerge, and submit a command - then immediately
	// crash; the leader should have no time to send an updated LeaderCommit
	// to followers. It doesn't have time to get back AE responses either, so
	// the leader itself won't send it on the commit channel.

	leader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 5)
	sleepMs(1)
	h.CrashPeer(leader)

	// Make sure 5 is not committed when a new leader is elected. Leaders won't
	// commit commands from previous terms.
	sleepMs(10)
	h.CheckSingleLeader(5)
	sleepMs(300)
	h.CheckNotCommitted(5)

	// The old leader restarts. After a while, 5 is still not committed.
	h.RestartPeer(leader)
	sleepMs(300)
	leader, _ = h.CheckSingleLeader(5)
	h.CheckNotCommitted(5)

	// When we submit a new command, it will be submitted, and so will 5, because
	// it appears in everyone's logs.
	h.SubmitToServer(leader, 6)
	sleepMs(300)
	h.CheckCommitted(5)
	h.CheckCommitted(6)

}

func TestDisconnectAfterSubmit(t *testing.T) {
	// Similar to TestCrashAfterSubmit, but the leader is disconnected - rather
	// than crashed - shortly after submitting the first command.
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader(5)
	h.SubmitToServer(leader, 5)
	sleepMs(1)
	h.DisconnectPeer(leader)

	// Make sure 5 is not committed when a new leader is elected. Leaders won't
	// commit commands from previous terms.
	sleepMs(10)
	h.CheckSingleLeader(5)
	sleepMs(300)
	h.CheckNotCommitted(5)

	h.ReconnectPeer(leader)
	sleepMs(300)
	leader, _ = h.CheckSingleLeader(5)
	h.CheckNotCommitted(5)

	// When we submit a new command, it will be submitted, and so will 5, because
	// it appears in everyone's logs.
	h.SubmitToServer(leader, 6)
	sleepMs(300)
	h.CheckCommitted(5)
	h.CheckCommitted(6)

}
