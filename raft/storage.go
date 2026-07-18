package raft

import "sync"

// PersistentState is the crash-stable subset of Node state. Commit/apply and
// leader replication indexes are deliberately volatile and do not belong here.
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

type StableStore interface {
	Save(PersistentState)
	Load() (PersistentState, bool)
}

type MemoryStableStore struct {
	mu    sync.Mutex
	state PersistentState
	ok    bool
}

func NewMemoryStableStore() *MemoryStableStore { return &MemoryStableStore{} }

func (s *MemoryStableStore) Save(state PersistentState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.Log = append([]LogEntry(nil), state.Log...)
	s.state = state
	s.ok = true
}

func (s *MemoryStableStore) Load() (PersistentState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.state
	state.Log = append([]LogEntry(nil), state.Log...)
	return state, s.ok
}
