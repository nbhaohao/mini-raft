package raft

import (
	"fmt"
	"testing"
	"time"
)

type Harness struct {
	t       *testing.T
	seed    int64
	net     *network
	servers []*Server
	trace   *Trace
}

func NewHarness(t *testing.T, nodeCount int, seed int64) *Harness {
	t.Helper()
	net := newNetwork(seed)
	trace := NewTrace(seed)
	ready := make(chan struct{})
	h := &Harness{t: t, seed: seed, net: net, trace: trace}

	for id := 0; id < nodeCount; id++ {
		peers := make([]int, 0, nodeCount-1)
		for peer := 0; peer < nodeCount; peer++ {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		h.servers = append(h.servers, newServer(id, peers, net, ready, seed, trace))
	}
	close(ready)
	return h
}

func (h *Harness) Shutdown() {
	for _, s := range h.servers {
		s.Stop()
	}
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
