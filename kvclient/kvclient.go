package kvclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/knightfall22/raft/api"
)

// DebugClient enables debug output
const DebugClient = 1

type KVClient struct {
	mu    sync.Mutex
	addrs []string

	// assumedLeader is the index (in addrs) of the service we assume is the
	// current leader. It is zero-initialized by default, without loss of
	// generality.
	assumedLeader int

	clientId uint64

	// requestID is a unique identifier for a request a specific client makes;
	// each client manages its own requestID, and increments it monotonically and
	// atomically each time the user asks to send a new request.
	requestId atomic.Uint64
}

// clientCount is used to assign unique identifiers to distinct clients.
var clientCount atomic.Uint64

func NewKVClient(serviceAddrs []string) *KVClient {
	return &KVClient{
		addrs:         serviceAddrs,
		assumedLeader: 0,
		clientId:      clientCount.Add(1),
	}
}

// Put the key=value pair into the store. Returns an error, or
// (prevValue, keyFound, false), where keyFound specifies whether the key was
// found in the store prior to this command, and prevValue is its previous
// value if it was found.
func (c *KVClient) Put(ctx context.Context, key, value string) (string, bool, error) {

	// Each request gets a unique ID, which is a combination of client ID and
	// request ID within this client. The struct with this ID is passed to s.send,
	// which may retry the request multiple times until succeeding. The unique ID
	// within each request helps the service de-duplicate requests that may
	// arrive multiple times due to network issues and client retries.
	putReq := api.PutRequest{
		Key:   key,
		Value: value,

		ClientID:  c.clientId,
		RequestID: c.requestId.Add(1),
	}

	var putResp api.PutResponse
	err := c.send(ctx, "put", putReq, &putResp)
	return putResp.PrevValue, putResp.KeyFound, err
}

// Append the value to the key in the store. Returns an error, or
// (prevValue, keyFound, false), where keyFound specifies whether the key was
// found in the store prior to this command, and prevValue is its previous
// value if it was found.
func (c *KVClient) Append(ctx context.Context, key, value string) (string, bool, error) {
	appendReq := api.AppendRequest{
		Key:   key,
		Value: value,

		ClientID:  c.clientId,
		RequestID: c.requestId.Add(1),
	}

	var appendResp api.AppendResponse
	err := c.send(ctx, "append", appendReq, &appendResp)
	return appendResp.PrevValue, appendResp.KeyFound, err
}

// Get the value of key from the store. Returns an error, or
// (value, found, false), where found specifies whether the key was found in
// the store, and value is its value.
func (c *KVClient) Get(ctx context.Context, key string) (string, bool, error) {
	getReq := api.GetRequest{
		Key: key,

		ClientID:  c.clientId,
		RequestID: c.requestId.Add(1),
	}

	var getResp api.GetResponse
	err := c.send(ctx, "get", getReq, &getResp)
	return getResp.Value, getResp.KeyFound, err
}

// CAS operation: if prev value of key == compare, assign new value. Returns an
// error, or (prevValue, keyFound, false), where keyFound specifies whether the
// key was found in the store prior to this command, and prevValue is its
// previous value if it was found.
func (c *KVClient) CAS(ctx context.Context, key, compare, value string) (string, bool, error) {
	casReq := api.CASRequest{
		Key:     key,
		Compare: compare,
		Value:   value,

		ClientID:  c.clientId,
		RequestID: c.requestId.Add(1),
	}

	var casResp api.CASResponse
	err := c.send(ctx, "cas", casReq, &casResp)
	return casResp.PrevValue, casResp.KeyFound, err
}

func (c *KVClient) send(ctx context.Context, route string, req any, resp api.Response) error {

	// This loop rotates through the list of service addresses until we get
	// a response that indicates we've found the leader of the cluster. It
	// starts at c.assumedLeader
FindLeader:
	for {
		// There's a two-level context tree here: we have the user context - ctx,
		// and we create our own context to impose a timeout on each request to
		// the service. If our timeout expires, we move on to try the next service.
		// In the meantime, we have to keep an eye on the user context - if that's
		// canceled at any time (due to timeout, explicit cancellation, etc), we
		// bail out.
		retryCtx, retryCtxCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		path := fmt.Sprintf("http://%s/%s/", c.addrs[c.assumedLeader], route)

		c.clientlog("sending %#v to %v", req, path)

		if err := sendJSONRequest(retryCtx, path, req, resp); err != nil {
			// Since the contexts are nested, the order of testing here matters.
			// We have to check the parent context first - if it's done, it means
			// we have to return.
			if contextDone(ctx) {
				c.clientlog("parent context done; bailing out")
				retryCtxCancel()
				return err
			} else if contextDeadlineExceeded(retryCtx) {
				// If the parent context is not done, but our retry context is done,
				// it's time to retry a different service.
				c.clientlog("timed out: will try next address")
				c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
				retryCtxCancel()
				continue FindLeader
			}

			c.clientlog("We have encountered an error: %v", err)
			retryCtxCancel()
			return err
		}

		c.clientlog("received response %#v", resp)
		switch resp.Status() {
		case api.StatusNotLeader:
			c.clientlog("%s not leader: will try next address", path)
			c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
			retryCtxCancel()
			continue FindLeader
		case api.StatusOk:
			c.clientlog("Request processed successfully")
			retryCtxCancel()
			return nil
		case api.StatusFailedCommit:
			retryCtxCancel()
			return fmt.Errorf("commit failed; please retry")
		default:
			panic("unreachable")
		}
	}

}

func (c *KVClient) clientlog(format string, args ...any) {
	if DebugClient > 0 {
		format := fmt.Sprintf("[client%03d], ", c.clientId) + format
		log.Printf(format, args...)
	}
}

func sendJSONRequest(ctx context.Context, path string, reqData, respData any) error {
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)

	if err := enc.Encode(reqData); err != nil {
		return fmt.Errorf("JSON-encoding request data: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, body)
	if err != nil {
		return fmt.Errorf("creating HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(resp.Body)

	if err := dec.Decode(respData); err != nil {
		return fmt.Errorf("JSON-decoding response data: %w", err)
	}

	return nil
}

// contextDone checks whether ctx is done for any reason. It doesn't block.
func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// contextDeadlineExceeded checks whether ctx is done because of an exceeded
// deadline. It doesn't block.
func contextDeadlineExceeded(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return true
		}
	default:
	}
	return false
}
