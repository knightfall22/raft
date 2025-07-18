package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestHarness(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()
	sleepMs(80)
}

func TestClientRequestBeforeConsensus(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()
	sleepMs(10)

	// The client will keep cycling between the services until a leader is found.
	c1 := h.NewClient()
	h.CheckPut(c1, "mf", "doom")
	sleepMs(80)
}

func TestBasicPutGetSingleClient(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	h.CheckPut(c1, "mf", "doom")

	h.CheckGet(c1, "mf", "doom")
	sleepMs(80)

}

func TestPutPrevValue(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	prev, found := h.CheckPut(c1, "mf", "doom")

	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	prev, found = h.CheckPut(c1, "mf", "doomsday")
	if !found || prev != "doom" {
		t.Errorf(`got found=%v, prev=%v, want true/"doom"`, found, prev)
	}

	prev, found = h.CheckPut(c1, "czar", "face")

	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

}

func TestBasicAppendSameClient(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	prev, found := h.CheckPut(c1, "mf", "doom")

	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	prev, found = h.CheckAppend(c1, "mf", "villain")
	if !found || prev != "doom" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	h.CheckGet(c1, "mf", "doomvillain")

	prev, found = h.CheckAppend(c1, "Publius", "Cornellius")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	h.CheckGet(c1, "Publius", "Cornellius")
}

func TestBasicPutGetDifferentClients(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	h.CheckPut(c1, "mf", "doom")

	c2 := h.NewClient()
	h.CheckGet(c2, "mf", "doom")
	sleepMs(80)
}

func TestBasicAppendGetDifferentClients(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	prev, found := h.CheckPut(c1, "mf", "doom")

	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	c2 := h.NewClient()
	prev, found = h.CheckAppend(c2, "mf", "villain")
	if !found || prev != "doom" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	h.CheckGet(c2, "mf", "doomvillain")

	prev, found = h.CheckAppend(c2, "Publius", "Cornellius")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	h.CheckGet(c2, "Publius", "Cornellius")
}

func TestBasicCAS(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	h.CheckPut(c1, "mf", "doom")

	if pv, f := h.CheckCAS(c1, "mf", "doom", "bloom"); !f {
		t.Errorf("got %s,%v, want replacement", pv, f)
	}
}

func TestCASConcurrently(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader(10)

	c1 := h.NewClient()
	h.CheckPut(c1, "mf", "fume")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for range 20 {
			h.CheckCAS(c1, "mf", "foom", "doom")
		}
	}()

	// Once a client homes in on the right leader, it takes 4-5 ms to roundtrip
	// a command. For the first 50 ms after launching the CAS goroutines, 'foo'
	// has the wrong value so the CAS doesn't work, but then it will...
	sleepMs(50)

	c2 := h.NewClient()
	h.CheckPut(c2, "mf", "foom")
	sleepMs(300)
	h.CheckGet(c2, "mf", "doom")
	wg.Wait()
}

func TestConcurrentClientsPutsAndGets(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	// Test that we can submit multiple PUT and GET requests concurrently, with
	// one goroutine per request launching at the same time.
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader(5)

	n := 9
	for i := range n {
		go func() {
			c := h.NewClient()
			_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
			if f {
				t.Errorf("got key found for %d, want false", i)
			}
		}()
	}
	sleepMs(150)

	for i := range n {
		go func() {
			c := h.NewClient()
			h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		}()
	}
	sleepMs(150)
}

func TestDisconnectLeaderAfterPuts(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader(5)

	lID := h.CheckSingleLeader(5)

	for i := range 4 {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.DisconnectServiceFromPeers(lID)
	sleepMs(300)
	newLID := h.CheckSingleLeader(5)

	if newLID == lID {
		t.Errorf("same leader")
	}

	// Trying to contact the disconnected leader will time out.
	nc := h.NewClientSingleService(lID)
	h.CheckGetTimesOut(nc, "key1")

	// GET commands expecting to get the right values
	for range 5 {
		c := h.NewClientWithRandomAddrsOrder()

		for j := range 4 {
			h.CheckGet(c, fmt.Sprintf("key%v", j), fmt.Sprintf("value%v", j))
		}
	}

	// At the end of the test, reconnect the peers to avoid a goroutine leak.
	// In real scenarios, we expect that services will eventually be reconnected,
	// and if not - a single goroutine leaked is not an issue since the server
	// will end up being killed anyway.
	h.ReconnectPeer(lID)
	sleepMs(200)
}

func TestAppendDifferentLeaders(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader(5)

	lID := h.CheckSingleLeader(5)

	c1 := h.NewClient()
	h.CheckAppend(c1, "mf", "doom")
	h.CheckGet(c1, "mf", "doom")

	// Crash a leader and wait for the cluster to establish a new leader.
	h.CrashPeer(lID)
	sleepMs(300)
	h.CheckSingleLeader(5)

	c2 := h.NewClient()
	h.CheckAppend(c2, "mf", "villain")
	h.CheckGet(c2, "mf", "doomvillain")

	h.RestartPeer(lID)
	sleepMs(300)
	c3 := h.NewClient()
	h.CheckGet(c3, "mf", "doomvillain")
}

func TestDisconnectLeaderAndFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	// Test that we can submit multiple PUT and GET requests concurrently, with
	// one goroutine per request launching at the same time.
	h := NewHarness(t, 3)
	defer h.Shutdown()
	lID := h.CheckSingleLeader(5)

	n := 9
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}
	sleepMs(150)

	// Disconnect leader and one other server; the cluster loses consensus
	// and client requests should now time out.
	h.DisconnectServiceFromPeers(lID)
	otherId := (lID + 1) % 3
	h.DisconnectServiceFromPeers(otherId)

	c := h.NewClient()
	h.CheckGetTimesOut(c, "key0")

	// Reconnect one server, but not the old leader. We should still get all
	// the right data back.
	h.ReconnectPeer(otherId)
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	sleepMs(100)
}

func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lID := h.CheckSingleLeader(5)

	n := 9
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}
	sleepMs(150)

	// Crash a non-leader
	otherId := (lID + 1) % 3
	h.CrashPeer(otherId)

	// Talking directly to the leader should still work...
	for i := range n {
		c := h.NewClientSingleService(lID)
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	// Talking to the remaining live servers should also work
	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
}

func TestCrashLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lID := h.CheckSingleLeader(5)

	n := 9
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}
	sleepMs(150)

	// Crash a leader and wait for the cluster to establish a new leader.
	h.CrashPeer(lID)
	h.CheckSingleLeader(5)

	// Talking to the remaining live servers should also work
	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lID := h.CheckSingleLeader(5)

	n := 9
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}
	sleepMs(150)

	// Crash a leader and wait for the cluster to establish a new leader.
	h.CrashPeer(lID)
	h.CheckSingleLeader(5)

	// Talking to the remaining live servers should also work
	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.RestartPeer(lID)

	// Get data from services in different orders.
	for range 5 {
		c := h.NewClientWithRandomAddrsOrder()

		for j := range 4 {
			h.CheckGet(c, fmt.Sprintf("key%v", j), fmt.Sprintf("value%v", j))
		}
	}
}

func TestAppendLinearizableAfterDelay(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lID := h.CheckSingleLeader(5)

	// A sequence of put+append, check we get the right result.
	c1 := h.NewClient()
	h.CheckPut(c1, "mf", "doom")
	h.CheckAppend(c1, "mf", "the")
	h.CheckGet(c1, "mf", "doomthe")

	// Ask the service to delay the response to the next request, and send
	// an append. The client will retry this append, so the system has to be
	// resilient to this. It will report a duplicate because of the retries,
	// but the append will be applied successfully.
	h.DelayNextHTTPResponseFromService(lID)

	prev, found, err := c1.Append(context.Background(), "mf", "villain")

	if err != nil {
		t.Errorf("an error has occured: %v", err)
	}

	if !found || prev != "doomthe" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	// Make sure the append was applied successfully, and just once.
	sleepMs(300)
	h.CheckGet(c1, "mf", "doomthevillain")
}

func TestAppendLinearizableAfterCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lID := h.CheckSingleLeader(5)

	// A sequence of put+append, check we get the right result.
	c1 := h.NewClient()
	h.CheckAppend(c1, "mf", "doom")
	h.CheckGet(c1, "mf", "doom")

	// // Delay response from the leader and then crash it. When a new leader is
	// // selected, we expect to see one append committed (but only one!)
	h.DelayNextHTTPResponseFromService(lID)

	go func() {
		ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
		defer cancel()
		prev, found, err := c1.Append(ctx, "mf", "villain")

		if err != nil {
			t.Errorf("an error has occured: %v", err)
		}

		if !found || prev != "doom" {
			t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
		}
	}()

	// Make sure the append was applied successfully, and just once.
	sleepMs(50)
	h.CrashPeer(lID)
	h.CheckSingleLeader(5)

	c2 := h.NewClient()
	v, f := h.CheckGet(c2, "mf", "doomvillain")
	log.Printf("This is a goal value:%s, found: %t\n", v, f)
}
