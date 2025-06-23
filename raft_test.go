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
