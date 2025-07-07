package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/knightfall22/raft/api"
	"github.com/knightfall22/raft/raft"
)

const DebugKV = 1

type KVService struct {
	sync.Mutex

	// id is the service ID in a Raft cluster.
	id int

	// rs is the Raft server that contains a CM
	rs *raft.Server

	// commitChan is the commit channel passed to the Raft server; when commands
	// are committed, they're sent on this channel.
	commitChan chan raft.CommitEntry

	// commitSubs are the commit subscriptions currently active in this service.
	commitSubs map[int]chan Command

	// ds is the underlying data store implementing the KV DB.
	ds *Datastore

	// srv is the HTTP server exposed by the service to the external world.
	srv *http.Server

	// httpResponsesEnabled controls whether this service returns HTTP responses
	// to the client. It's only used for testing and debugging.
	httpResponsesEnabled bool

	// lastRequestIDPerClient helps de-duplicate client requests. It stores the
	// last request ID that was applied by the updater per client; the assumption
	// is that client IDs are unique (keys in this map), and for each client the
	// requests IDs (values in this map) are unique and monotonically increasing.
	lastRequestIDPerClient map[uint64]map[uint64]Command

	// delayNextHTTPResponse will be on when the service was requested to
	// delay its next HTTP response to the client. This flips back to off after
	// use.
	delayNextHTTPResponse atomic.Bool
}

// New creates a new KVService
//
//   - id: this service's ID within its Raft cluster
//   - peerIds: the IDs of the other Raft peers in the cluster
//   - storage: a raft.Storage implementation the service can use for
//     durable storage to persist its state.
//   - readyChan: notification channel that has to be closed when the Raft
//     cluster is ready (all peers are up and connected to each other).
func NewKVService(id int, peerIds []int, storage raft.Storage, readyChan chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	// raft.Server handles the Raft RPCs in the cluster; after Serve is called,
	// it's ready to accept RPC connections from peers.
	rs := raft.NewServer(id, peerIds, readyChan, storage, commitChan)
	rs.Serve()

	kvs := &KVService{
		id:                     id,
		rs:                     rs,
		commitChan:             commitChan,
		commitSubs:             make(map[int]chan Command),
		lastRequestIDPerClient: make(map[uint64]map[uint64]Command),
		ds:                     NewDatastore(),
		httpResponsesEnabled:   true,
	}

	go kvs.runUpdater()
	return kvs
}

// runUpdater runs the "updater" goroutine that reads the commit channel
// from Raft and updates the data store; this is the Replicated State Machine
// part of distributed consensus!
// It also notifies subscribers (registered with createCommitSubscription).
func (kvs *KVService) runUpdater() {
	for entry := range kvs.commitChan {
		cmd := entry.Command.(Command)

		// Duplicate command detection.
		// Only accept this request if its ID is higher than the last request from
		// this client.
		// on the off chance that a duplicated request commits
		kvs.Lock()
		lastCmd, ok := kvs.lastRequestIDPerClient[cmd.ClientID][cmd.RequestID]
		if ok && lastCmd.RequestID >= cmd.RequestID {
			cmd = Command{
				Kind:        lastCmd.Kind,
				ServiceID:   lastCmd.ServiceID,
				RequestID:   lastCmd.RequestID,
				ResultValue: lastCmd.ResultValue,
				ResultFound: lastCmd.ResultFound,
				IsDuplicate: true,
			}

		} else {
			kvs.kvlog("Commit Index: %d", entry.Index)

			kvs.kvlog("This cmd: %+v has been logged", cmd)

			switch cmd.Kind {
			case CommandGet:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
			case CommandPut:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
			case CommandAppend:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Append(cmd.Key, cmd.Value)
			case CommandCAS:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
			default:
				panic(fmt.Errorf("unexpected command %v", cmd))
			}

			if kvs.lastRequestIDPerClient[cmd.ClientID] == nil {
				kvs.lastRequestIDPerClient[cmd.ClientID] = make(map[uint64]Command)
			}

			kvs.lastRequestIDPerClient[cmd.ClientID][cmd.RequestID] = Command{
				Kind:        cmd.Kind,
				ServiceID:   cmd.ServiceID,
				RequestID:   cmd.RequestID,
				ResultValue: cmd.ResultValue,
				ResultFound: cmd.ResultFound,
			}

		}

		kvs.Unlock()
		// Forward this entry to the subscriber interested in its index, and
		// close the subscription - it's single-use.
		sub := kvs.popCommitSubscription(entry.Index)

		if sub != nil {
			kvs.kvlog("Hello angel:%t", sub != nil)
			sub <- cmd
			close(sub)
		}

	}

}

func (kvs *KVService) ServerHTTP(port int) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)
	mux.HandleFunc("POST /cas/", kvs.handleCAS)
	mux.HandleFunc("POST /append/", kvs.handleAppend)

	kvs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		kvs.kvlog("serving HTTP on %s", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

// ToggleHTTPResponsesEnabled controls whether this service returns HTTP
// responses to clients. It's always enabled during normal operation.
// For testing and debugging purposes, this method can be called with false;
// then, the service will not respond to clients over HTTP.
func (kvs *KVService) ToggleHttpResponses(enable bool) {
	kvs.Lock()
	defer kvs.Unlock()
	kvs.httpResponsesEnabled = enable
}

func (kvs *KVService) sendHTTPResponse(w http.ResponseWriter, v any) {
	if kvs.delayNextHTTPResponse.Load() {
		kvs.delayNextHTTPResponse.Store(false)
		time.Sleep(300 * time.Millisecond)
	}

	if kvs.httpResponsesEnabled {
		renderJson(w, v)
	}
}

func (kvs *KVService) handlePut(w http.ResponseWriter, r *http.Request) {
	pr := &api.PutRequest{}

	if err := readRequestJson(r, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kvs.kvlog("HTTP PUT %v", pr)

	kvs.Lock()
	//Check if its a duplicated Request
	if lastCmd, ok := kvs.lastRequestIDPerClient[pr.ClientID][pr.RequestID]; ok {
		kvs.kvlog("Duplicate encountered")
		kvs.sendHTTPResponse(w, api.PutResponse{
			RespStatus: api.StatusOk,
			PrevValue:  lastCmd.ResultValue,
			KeyFound:   lastCmd.ResultFound,
		})
		kvs.Unlock()
		return
	}
	kvs.Unlock()

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind:  CommandPut,
		Key:   pr.Key,
		Value: pr.Value,

		ServiceID: kvs.id,
		ClientID:  pr.ClientID,
		RequestID: pr.RequestID,
	}

	logIndex := kvs.rs.Submit(cmd)

	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index. Then wait for it to
	// be delivered.
	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			kvs.kvlog("[%s] Commit has been made", commitCmd.Kind)
			kvs.sendHTTPResponse(w, api.PutResponse{
				RespStatus: api.StatusOk,
				PrevValue:  commitCmd.ResultValue,
				KeyFound:   commitCmd.ResultFound,
			})

		} else {
			kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-r.Context().Done():
		return
	}

}

func (kvs *KVService) handleAppend(w http.ResponseWriter, r *http.Request) {
	ar := &api.AppendRequest{}

	if err := readRequestJson(r, ar); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kvs.kvlog("HTTP PUT %v", ar)

	kvs.Lock()
	//Check if its a duplicated Request
	if lastCmd, ok := kvs.lastRequestIDPerClient[ar.ClientID][ar.RequestID]; ok {
		kvs.kvlog("Duplicate encountered")
		kvs.sendHTTPResponse(w, api.AppendResponse{
			RespStatus: api.StatusOk,
			PrevValue:  lastCmd.ResultValue,
			KeyFound:   lastCmd.ResultFound,
		})
		kvs.Unlock()
		return
	}
	kvs.Unlock()

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind:  CommandAppend,
		Key:   ar.Key,
		Value: ar.Value,

		ServiceID: kvs.id,
		ClientID:  ar.ClientID,
		RequestID: ar.RequestID,
	}

	logIndex := kvs.rs.Submit(cmd)

	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index. Then wait for it to
	// be delivered.
	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			kvs.kvlog("[%s] Commit has been made", commitCmd.Kind)
			kvs.sendHTTPResponse(w, api.AppendResponse{
				RespStatus: api.StatusOk,
				PrevValue:  commitCmd.ResultValue,
				KeyFound:   commitCmd.ResultFound,
			})

		} else {
			kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-r.Context().Done():
		return
	}

}

// The details of these handlers are very similar to handlePut
func (kvs *KVService) handleGet(w http.ResponseWriter, r *http.Request) {
	gr := &api.GetRequest{}

	if err := readRequestJson(r, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kvs.kvlog("HTTP GET %v", gr)

	kvs.Lock()
	//Check if its a duplicated Request
	if lastCmd, ok := kvs.lastRequestIDPerClient[gr.ClientID][gr.RequestID]; ok {
		kvs.sendHTTPResponse(w, api.GetResponse{
			RespStatus: api.StatusOk,
			Value:      lastCmd.ResultValue,
			KeyFound:   lastCmd.ResultFound,
		})
		kvs.Unlock()
		return
	}
	kvs.Unlock()

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind: CommandGet,
		Key:  gr.Key,

		ServiceID: kvs.id,
		ClientID:  gr.ClientID,
		RequestID: gr.RequestID,
	}

	logIndex := kvs.rs.Submit(cmd)

	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index. Then wait for it to
	// be delivered.
	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			kvs.kvlog("[%s] Commit has been made", commitCmd.Kind)
			kvs.sendHTTPResponse(w, api.GetResponse{
				RespStatus: api.StatusOk,
				Value:      commitCmd.ResultValue,
				KeyFound:   commitCmd.ResultFound,
			})

		} else {
			kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-r.Context().Done():
		return
	}
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, r *http.Request) {
	cr := &api.CASRequest{}

	if err := readRequestJson(r, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kvs.kvlog("HTTP PUT %v", cr)

	kvs.Lock()
	//Check if its a duplicated Request
	if lastCmd, ok := kvs.lastRequestIDPerClient[cr.ClientID][cr.RequestID]; ok {
		kvs.kvlog("Duplicate encountered")
		kvs.sendHTTPResponse(w, api.CASResponse{
			RespStatus: api.StatusOk,
			PrevValue:  lastCmd.ResultValue,
			KeyFound:   lastCmd.ResultFound,
		})
		kvs.Unlock()
		return
	}
	kvs.Unlock()

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		Value:        cr.Value,
		CompareValue: cr.Compare,

		ServiceID: kvs.id,
		ClientID:  cr.ClientID,
		RequestID: cr.RequestID,
	}

	logIndex := kvs.rs.Submit(cmd)

	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index. Then wait for it to
	// be delivered.
	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			kvs.kvlog("[%s] Commit has been made", commitCmd.Kind)
			kvs.sendHTTPResponse(w, api.CASResponse{
				RespStatus: api.StatusOk,
				PrevValue:  commitCmd.ResultValue,
				KeyFound:   commitCmd.ResultFound,
			})

		} else {
			kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-r.Context().Done():
		return
	}
}

// Shutdown performs a proper shutdown of the service: shuts down the Raft RPC
// server, and shuts down the main HTTP service. It only returns once shutdown
// is complete.
// Note: DisconnectFromRaftPeers on all peers in the cluster should be done
// before Shutdown is called.
func (kvs *KVService) Shutdown() error {
	kvs.kvlog("shutting down Raft server")
	kvs.rs.Shutdown()
	kvs.kvlog("closing commitChan")
	close(kvs.commitChan)

	kvs.kvlog("is not nil? :%t", kvs.srv != nil)

	if kvs.srv != nil {
		kvs.kvlog("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kvs.srv.Shutdown(ctx)
		kvs.kvlog("HTTP shutdown complete")
		return nil

	}

	return nil

}

func (kvs *KVService) IsLeader() bool {
	return kvs.rs.IsLeader()
}

// DelayNextHTTPResponse instructs the service to delay the response to the
// next HTTP request from the client. The service still acts on the request
// as usual, just the HTTP response is delayed. This only applies to a single
// response - the bit flips back to off after use.
func (kvs *KVService) DelayNextHTTPResponse() {
	kvs.delayNextHTTPResponse.Store(true)
}

// createCommitSubscription creates a "commit subscription" for a certain log
// index. It's used by client request handlers that submit a command to the
// Raft CM. createCommitSubscription(index) means "I want to be notified when
// an entry is committed at this index in the Raft log". The entry is delivered
// on the returend (buffered) channel by the updater goroutine, after which
// the channel is closed and the subscription is automatically canceled.
func (kvs *KVService) createCommitSubscription(logIndex int) chan Command {
	kvs.kvlog("creating commit subscription for logIndex=%d", logIndex)
	kvs.Lock()
	defer kvs.Unlock()

	if _, exists := kvs.commitSubs[logIndex]; exists {
		panic(fmt.Sprintf("duplicate commit subscription for logIndex=%d", logIndex))
	}

	ch := make(chan Command, 1)
	kvs.commitSubs[logIndex] = ch
	return ch
}

func (kvs *KVService) popCommitSubscription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	ch := kvs.commitSubs[logIndex]
	delete(kvs.commitSubs, logIndex)

	return ch
}

// kvlog logs a debugging message if DebugKV > 0
func (kvs *KVService) kvlog(format string, args ...any) {
	if DebugKV > 0 {
		format := fmt.Sprintf("[kv %d], ", kvs.id) + format
		log.Printf(format, args...)
	}
}

// The following functions exist for testing purposes, to simulate faults.

func (kvs *KVService) ConnectToRaftPeers(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) DisconnectFromAllRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) DisconnectFromRaftPeer(peerId int) error {
	return kvs.rs.DisconnectPeer(peerId)
}

func (kvs *KVService) GetListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}
