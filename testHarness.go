package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knightfall22/raft/kvclient"
	"github.com/knightfall22/raft/kvservice"
	"github.com/knightfall22/raft/raft"
)

const servePort = 14200

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu sync.Mutex

	// kvCluster is a list of all KVService instances participating in a cluster.
	// A service's index into this list is its ID in the cluster.
	kvCluster []*kvservice.KVService

	// kvServiceAddrs is a list of HTTP addresses (localhost:<PORT>) the KV
	// services are accepting client commands on.
	kvServiceAddrs []string

	storage []*raft.MapStorage

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	// alive has a bool per server in cluster, specifying whether this server is
	// currently alive (false means it has crashed and wasn't restarted yet).
	// connected implies alive.
	alive []bool

	// ctx is context used for the HTTP client commands used by tests.
	// ctxCancel is its cancellation function.
	ctx       context.Context
	ctxCancel func()

	n int
	t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	kvs := make([]*kvservice.KVService, n)
	storage := make([]*raft.MapStorage, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	ready := make(chan any)

	// Create all Servers in this cluster, assign ids and peer ids.
	for id := range n {
		peerIds := make([]int, 0)

		for p := range n {
			if p != id {
				peerIds = append(peerIds, p)
			}
		}

		storage[id] = raft.NewMapStorage()
		kvs[id] = kvservice.NewKVService(id, peerIds, storage[id], ready)
		alive[id] = true
	}

	for id := range n {
		for p := range n {
			if p != id && alive[id] {
				kvs[id].ConnectToRaftPeers(p, kvs[p].GetListenAddr())
			}
		}

		connected[id] = true
	}

	close(ready)

	// Each KVService instance serves a REST API on a different port
	kvsServicePorts := make([]string, n)

	for i := range n {
		port := servePort + i

		kvs[i].ServerHTTP(port)

		kvsServicePorts[i] = fmt.Sprintf("localhost:%d", port)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	h := &Harness{
		kvCluster:      kvs,
		t:              t,
		alive:          alive,
		storage:        storage,
		ctx:            ctx,
		ctxCancel:      ctxCancel,
		kvServiceAddrs: kvsServicePorts,
		connected:      connected,
		n:              n,
	}

	return h
}

// DelayNextHTTPResponseFromService delays the next HTTP response from this
// service to a client.
func (h *Harness) DelayNextHTTPResponseFromService(id int) {
	tlog("Delaying next HTTP response from %d", id)
	h.kvCluster[id].DelayNextHTTPResponse()
}

func (h *Harness) DisconnectServiceFromPeers(id int) {
	tlog("Disconnect %d", id)

	h.kvCluster[id].DisconnectFromAllRaftPeers()

	for p := range h.n {
		if p != id {
			h.kvCluster[p].DisconnectFromRaftPeer(id)
		}
	}

	h.connected[id] = false
}

// ReconnectPeer connects a server to all other servers in the cluster.
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for p := range h.kvCluster {
		if p != id && h.alive[p] {
			//Connect sever at `id` to a peer `p`
			if err := h.kvCluster[id].ConnectToRaftPeers(p, h.kvCluster[p].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}

			//connect peer at`p` to `id`
			if err := h.kvCluster[p].ConnectToRaftPeers(id, h.kvCluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}

	h.connected[id] = true
}

// CrashService "crashes" a service by disconnecting it from all peers and
// then asking it to shut down. We're not going to be using the same service
// instance again.
func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	h.DisconnectServiceFromPeers(id)
	h.alive[id] = false

	if err := h.kvCluster[id].Shutdown(); err != nil {
		h.t.Errorf("error while shutting down service %d: %v", id, err)
	}

}

// RestartPeer "restarts" a server by creating a new Server instance and giving
// it the appropriate storage, reconnecting it to peers.
func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		h.t.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := range h.n {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan any)
	h.kvCluster[id] = kvservice.NewKVService(id, peerIds, h.storage[id], ready)
	h.kvCluster[id].ServerHTTP(servePort + id)
	h.ReconnectPeer(id)
	close(ready)
	h.alive[id] = true
	sleepMs(20)
}

// DisableHTTPResponsesFromService causes the given service to stop responding
// to HTTP request from clients (though it will still perform the requested
// operations).
func (h *Harness) DisableHTTPResponsesFromService(id int) {
	tlog("Disabling HTTP responses from %d", id)
	h.kvCluster[id].ToggleHttpResponses(true)
}

func (h *Harness) Shutdown() {
	for id := range h.n {
		h.kvCluster[id].DisconnectFromAllRaftPeers()
		h.connected[id] = false
	}

	// These help the HTTP server in KVService shut down properly.
	http.DefaultClient.CloseIdleConnections()
	h.ctxCancel()

	for id := range h.n {
		if h.alive[id] {
			h.alive[id] = false
			if err := h.kvCluster[id].Shutdown(); err != nil {
				h.t.Errorf("error while shutting down service %d: %v", id, err)
			}
		}

	}
}

// NewClient creates a new client that will contact all the existing live
// services.
func (h *Harness) NewClient() *kvclient.KVClient {
	var addrs []string

	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}

	return kvclient.NewKVClient(addrs)
}

// NewClientWithRandomAddrsOrder creates a new client that will contact all
// the existing live services, but in a randomized order.
func (h *Harness) NewClientWithRandomAddrsOrder() *kvclient.KVClient {
	var addrs []string

	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}

	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})

	return kvclient.NewKVClient(addrs)
}

// NewClientSingleService creates a new client that will contact only a single
// service (specified by id). Note that if this isn't the leader, the client
// may get stuck in retries.
func (h *Harness) NewClientSingleService(id int) *kvclient.KVClient {
	addr := h.kvServiceAddrs[id : id+1]
	return kvclient.NewKVClient(addr)
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (h *Harness) CheckSingleLeader(retries int) int {
	for range retries {
		leaderId := -1

		for id := range h.kvCluster {
			if h.connected[id] {
				isLeader := h.kvCluster[id].IsLeader()

				if isLeader {
					if leaderId < 0 {
						leaderId = id
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, id)
					}
				}
			}
		}

		if leaderId >= 0 {
			return leaderId
		}

		//Given time for a leader to be elected before retrying
		time.Sleep(150 * time.Millisecond)
	}
	h.t.Fatalf("leader not found")
	return -1
}

// CheckPut sends a Put request through client c, and checks there are no
// errors. Returns (prevValue, keyFound).
func (h *Harness) CheckPut(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()

	pv, f, err := c.Put(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}

	return pv, f
}

// CheckPut sends a Append request through client c, and checks there are no
// errors. Returns (prevValue, keyFound).
func (h *Harness) CheckAppend(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()

	pv, f, err := c.Append(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}

	return pv, f
}

// CheckGet sends a Get request through client c, and checks there are
// no errors; it also checks that the key was found, and has the expected
// value.
func (h *Harness) CheckGet(c *kvclient.KVClient, key, wantValue string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()

	gv, f, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}

	if !f {
		h.t.Errorf("got found=false, want true for key=%s", key)
	}

	if gv != wantValue {
		h.t.Errorf("got value=%v, want %v", gv, wantValue)
	}

	return gv, f
}

// CheckCAS sends a CAS request through client c, and checks there are no
// errors. Returns (prevValue, keyFound).
func (h *Harness) CheckCAS(c *kvclient.KVClient, key, compare, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()

	pv, f, err := c.CAS(ctx, key, compare, value)
	if err != nil {
		h.t.Error(err)
	}

	return pv, f
}

// CheckGetNotFound sends a Get request through client c, and checks there are
// no errors, but the key isn't found in the service.
func (h *Harness) CheckGetNotFound(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()

	_, f, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}

	if f {
		h.t.Errorf("got found=true, want false for key=%s", key)
	}
}

// CheckGetTimesOut checks that a Get request with the given client will
// time out if we set up a context with a deadline, because the client is
// unable to get the service to commit its command.
func (h *Harness) CheckGetTimesOut(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(h.ctx, 300*time.Millisecond)
	defer cancel()

	_, _, err := c.Get(ctx, key)

	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		h.t.Errorf("got err %v; want 'deadline exceeded'", err)
	}
}
func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
