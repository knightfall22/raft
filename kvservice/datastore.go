package kvservice

import (
	"log"
	"sync"
)

// DataStore is a simple, concurrency-safe key-value store used as a backend
// for kvservice.
type Datastore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewDatastore() *Datastore {
	return &Datastore{
		data: make(map[string]string),
	}
}

// Get fetches the value of key from the datastore and returns (v, true) if
// it was found or ("", false) otherwise.
func (ds *Datastore) Get(key string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	v, ok := ds.data[key]

	return v, ok
}

// Put assigns datastore[key]=value, and returns (v, true) if the key was
// previously in the store and its value was v, or ("", false) otherwise.
func (ds *Datastore) Put(key, value string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	v, ok := ds.data[key]
	ds.data[key] = value
	return v, ok
}

func (ds *Datastore) Append(key, value string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	log.Printf("I am a key: %s, and value: %s", key, value)
	v, ok := ds.data[key]
	ds.data[key] += value
	return v, ok
}

// CAS performs an atomic compare-and-swap:
// if key exists and its prev value == compare, write value, else nop
// The prev value and whether the key existed in the store is returned.
func (ds *Datastore) CAS(key, compare, value string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	prevVal, ok := ds.data[key]
	if ok && compare == prevVal {
		ds.data[key] = value
	}

	return prevVal, ok
}
