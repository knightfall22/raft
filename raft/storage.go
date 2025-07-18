package raft

import "sync"

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)

	//Returns true if any set was made on this storage
	HasData() bool
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu sync.Mutex

	m map[string][]byte
}

func NewMapStorage() *MapStorage {
	return &MapStorage{
		m: make(map[string][]byte),
	}
}

func (m *MapStorage) Set(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = value
}

func (m *MapStorage) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, found := m.m[key]

	return v, found
}

func (m *MapStorage) HasData() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.m) > 0
}
