package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server wraps a raft.ConsensusModule along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for
// presentation purposes. raft.ConsensusModule has a *Server to do its peer
// communication and doesn't have to worry about the specifics of running an
// RPC server.
type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	cm       *ConsensusModule
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	storage Storage

	commitChan chan<- CommitEntry

	peerClients map[int]*rpc.Client

	ready chan any
	quit  chan any
	wg    sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, ready chan any, storage Storage, commitChan chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverId = serverId
	s.storage = storage
	s.peerIds = peerIds
	s.ready = ready
	s.peerClients = make(map[int]*rpc.Client)
	s.quit = make(chan any)
	s.commitChan = commitChan

	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready, s.storage, s.commitChan)

	// Create a RPC server and register an RPCProxy that forwards methods to cm
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{cm: s.cm}
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())

	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					log.Printf("closing server connection")
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()
}

func (s *Server) Submit(cmd any) int {
	return s.cm.Submit(cmd)
}

// DisconnectAll closes all the client connections to peers for this server.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly.
func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}

		s.peerClients[peerId] = client
	}

	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId.
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		log.Printf("[%d] Peer disconnected", peerId)
		return err
	}

	return nil
}

func (s *Server) Call(id int, serviceMethod string, args, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

// IsLeader checks if s thinks it's the leader in the Raft cluster.
func (s *Server) IsLeader() bool {
	_, _, isLeader := s.cm.Report()
	return isLeader
}

// Proxy provides access to the RPC proxy this server is using; this is only
// for testing purposes to simulate faults.
func (s *Server) Proxy() *RPCProxy {
	return s.rpcProxy
}

// RPCProxy is a pass-thru proxy server for ConsensusModule's RPC methods. It
// serves RPC requests made to a CM and manipulates them before forwarding to
// the CM itself.
//
// It's useful for things like:
//   - Simulating dropping of RPC calls
//   - Simulating a small delay in RPC transmission.
//   - Simulating possible unreliable connections by delaying some messages
//     significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
type RPCProxy struct {
	mu sync.Mutex
	cm *ConsensusModule

	// numCallsBeforeDrop is used to control dropping RPC calls:
	//   -1: means we're not dropping any calls
	//    0: means we're dropping all calls now
	//   >0: means we'll start dropping calls after this number is made
	numCallsBeforeDrop int
}

func NewRPCProxy(cm *ConsensusModule) *RPCProxy {
	return &RPCProxy{
		cm:                 cm,
		numCallsBeforeDrop: -1,
	}
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)

		if dice == 9 {
			rpp.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay RequestVote")
			time.Sleep(time.Millisecond * 75)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)

		if dice == 9 {
			rpp.cm.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay AppendEntries")
			time.Sleep(time.Millisecond * 75)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}

func (rpp *RPCProxy) Call(peer *rpc.Client, method string, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rpp.mu.Lock()

	if rpp.numCallsBeforeDrop == 0 {
		rpp.mu.Unlock()
		rpp.cm.dlog("drop Call %s: %v", method, args)
		return fmt.Errorf("RPC failed")
	} else {
		if rpp.numCallsBeforeDrop > 0 {
			rpp.numCallsBeforeDrop--
		}
		rpp.mu.Unlock()
		return peer.Call(method, args, reply)
	}
}

// DropCallsAfterN instruct the proxy to drop calls after n are made from this
// point.
func (rpp *RPCProxy) DropCallsAfterN(n int) {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()
	rpp.numCallsBeforeDrop = n
}

func (rpp *RPCProxy) DontDropCalls() {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()
	rpp.numCallsBeforeDrop = -1
}
