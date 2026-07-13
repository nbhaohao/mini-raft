package raft

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type Harness struct {
	t             *testing.T
	seed          int64
	net           *network
	servers       []*Server
	trace         *Trace
	commitMu      sync.Mutex
	commits       map[int][]CommitEntry
	collectorDone chan struct{}
	collectorWG   sync.WaitGroup
}

func NewHarness(t *testing.T, nodeCount int, seed int64) *Harness {
	t.Helper()
	net := newNetwork(seed)
	trace := NewTrace(seed)
	ready := make(chan struct{})
	h := &Harness{
		t: t, seed: seed, net: net, trace: trace,
		commits:       make(map[int][]CommitEntry),
		collectorDone: make(chan struct{}),
	}

	for id := 0; id < nodeCount; id++ {
		peers := make([]int, 0, nodeCount-1)
		for peer := 0; peer < nodeCount; peer++ {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		commitC := make(chan CommitEntry, 128)
		h.servers = append(h.servers, newServer(id, peers, net, ready, seed, trace, commitC))
		h.collectorWG.Add(1)
		go func(id int, c <-chan CommitEntry) {
			defer h.collectorWG.Done()
			for {
				select {
				case entry := <-c:
					h.commitMu.Lock()
					h.commits[id] = append(h.commits[id], entry)
					h.commitMu.Unlock()
				case <-h.collectorDone:
					return
				}
			}
		}(id, commitC)
	}
	close(ready)
	return h
}

func (h *Harness) SubmitToServer(id int, cmd any) bool {
	return h.servers[id].node.Submit(cmd)
}

func (h *Harness) CheckCommitted(cmd any) (int, int) {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	count, index := 0, -1
	var reference []CommitEntry
	for nodeID, entries := range h.commits {
		for position, entry := range entries {
			if reflect.DeepEqual(entry.Command, cmd) {
				if entry.Index != position {
					h.t.Fatalf("seed=%d: node=%d commit position=%d carries Index=%d\n%s", h.seed, nodeID, position, entry.Index, h.trace)
				}
				if index >= 0 && entry.Index != index {
					h.t.Fatalf("seed=%d: command=%v committed at indexes %d and %d\n%s", h.seed, cmd, index, entry.Index, h.trace)
				}
				if reference == nil {
					reference = append([]CommitEntry(nil), entries[:position+1]...)
				} else {
					for i := 0; i <= position; i++ {
						if i >= len(reference) || !reflect.DeepEqual(entries[i], reference[i]) {
							h.t.Fatalf("seed=%d: commit prefixes diverged at index=%d\nnode=%d commits=%v\nreference=%v\n%s", h.seed, i, nodeID, entries, reference, h.trace)
						}
					}
				}
				count++
				index = entry.Index
				break
			}
		}
	}
	return count, index
}

func (h *Harness) CheckCommittedN(cmd any, n int) (int, int) {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, index := h.CheckCommitted(cmd)
		if count == n {
			return count, index
		}
		if count > n {
			h.t.Fatalf("seed=%d: command %v committed on %d nodes, want exactly %d\n%s", h.seed, cmd, count, n, h.trace)
		}
		time.Sleep(20 * time.Millisecond)
	}
	count, index := h.CheckCommitted(cmd)
	h.t.Fatalf("seed=%d: command %v committed on %d nodes, want %d\n%s", h.seed, cmd, count, n, h.trace)
	return count, index
}

func (h *Harness) Shutdown() {
	for _, s := range h.servers {
		s.Stop()
	}
	close(h.collectorDone)
	h.collectorWG.Wait()
}

func (h *Harness) CheckLogN(cmd any, n int) (int, int) {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, index := h.countLogs(cmd)
		if count == n {
			return count, index
		}
		if count > n {
			h.t.Fatalf("seed=%d: command %v appears in %d logs, want exactly %d\n%s", h.seed, cmd, count, n, h.trace)
		}
		time.Sleep(20 * time.Millisecond)
	}
	count, index := h.countLogs(cmd)
	h.t.Fatalf("seed=%d: command %v appears in %d logs, want exactly %d\n%s", h.seed, cmd, count, n, h.trace)
	return count, index
}

func (h *Harness) countLogs(cmd any) (int, int) {
	count, index := 0, -1
	for _, server := range h.servers {
		server.node.mu.Lock()
		for i, entry := range server.node.log {
			if reflect.DeepEqual(entry.Command, cmd) {
				if index >= 0 && index != i {
					server.node.mu.Unlock()
					h.t.Fatalf("seed=%d: command %v appears at log indexes %d and %d\n%s", h.seed, cmd, index, i, h.trace)
				}
				count++
				index = i
				break
			}
		}
		server.node.mu.Unlock()
	}
	return count, index
}

func (h *Harness) Disconnect(id int) {
	h.trace.Add(id, 0, Follower, "Harness", "disconnect")
	h.net.disconnect(id)
}

func (h *Harness) Reconnect(id int) {
	h.trace.Add(id, 0, Follower, "Harness", "reconnect")
	h.net.reconnect(id)
}

func (h *Harness) Crash(id int) {
	h.Disconnect(id)
	h.servers[id].Stop()
}

func (h *Harness) Restart(id int) {
	h.Reconnect(id)
}

func (h *Harness) Events() []TraceEvent {
	return h.trace.Events()
}

func (h *Harness) CheckSingleLeader() (int, int) {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leader := -1
		term := -1
		for _, s := range h.servers {
			if !h.net.isConnected(s.id) {
				continue
			}
			st := s.Status()
			if st.Role == Leader {
				if leader >= 0 {
					h.t.Fatalf("seed=%d: leaders %d and %d both active\n%s", h.seed, leader, st.ID, h.trace)
				}
				leader = st.ID
				term = st.Term
			}
		}
		if leader >= 0 {
			return leader, term
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.t.Fatalf("seed=%d: leader not found\n%s", h.seed, h.trace)
	return -1, -1
}

func (h *Harness) CheckNoLeader() {
	h.t.Helper()
	for _, s := range h.servers {
		if !h.net.isConnected(s.id) {
			continue
		}
		st := s.Status()
		if st.Role == Leader {
			h.t.Fatalf("seed=%d: server %d is leader; want none\n%s", h.seed, st.ID, h.trace)
		}
	}
}

func requireEventually(t *testing.T, seed int64, trace fmt.Stringer, want string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("seed=%d: timed out waiting for %s\n%s", seed, want, trace)
}
