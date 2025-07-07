package api

// Defines the data structures used in the REST API between kvservice and
// clients. These structs are JSON-encoded into the body of HTTP requests
// and responses passed between services and clients.
// Uses bespoke ResponseStatus per response instead of HTTP status
// codes because some statuses like "not leader" or "failed commit" don't have a
// good match in standard HTTP status codes.

type Response interface {
	Status() ResponseStatus
}

type PutRequest struct {
	Key   string
	Value string

	ClientID  uint64
	RequestID uint64
}

type PutResponse struct {
	RespStatus ResponseStatus
	PrevValue  string
	KeyFound   bool
}

func (pr *PutResponse) Status() ResponseStatus {
	return pr.RespStatus
}

type GetRequest struct {
	Key string

	ClientID  uint64
	RequestID uint64
}

type GetResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	Value      string
}

func (gr *GetResponse) Status() ResponseStatus {
	return gr.RespStatus
}

type CASRequest struct {
	Key     string
	Compare string
	Value   string

	ClientID  uint64
	RequestID uint64
}

type CASResponse struct {
	RespStatus ResponseStatus
	PrevValue  string
	KeyFound   bool
}

func (cr *CASResponse) Status() ResponseStatus {
	return cr.RespStatus
}

type AppendRequest struct {
	Key   string
	Value string

	ClientID  uint64
	RequestID uint64
}

type AppendResponse struct {
	RespStatus ResponseStatus
	PrevValue  string
	KeyFound   bool
}

func (ap *AppendResponse) Status() ResponseStatus {
	return ap.RespStatus
}

type ResponseStatus int

const (
	StatusInvalid ResponseStatus = iota
	StatusOk
	StatusNotLeader
	StatusFailedCommit
	StatusDeplicateRequest
)

func (rs ResponseStatus) String() string {
	switch rs {
	case 0:
		return "invalid"
	case 1:
		return "OK"
	case 2:
		return "NotLeader"
	case 3:
		return "FailedCommit"
	case 4:
		return "DepulicateRequest"
	default:
		return "unkown"
	}
}
