package raft

import (
	"testing"
	"time"
)

func TestM01P2RandomizedDeadlines(t *testing.T) {
	h := NewHarness(t, 5, 20260712)
	defer h.Shutdown()

	seen := make(map[time.Time]bool)
	for _, s := range h.servers {
		d := s.node.electionDeadlineForTest()
		if seen[d] {
			t.Fatalf("seed=%d: duplicate election deadline %v; want per-node randomized deadlines\n%s", h.seed, d, h.trace)
		}
		seen[d] = true
	}

	before := h.servers[0].node.electionDeadlineForTest()
	h.servers[0].node.mu.Lock()
	h.servers[0].node.resetElectionDeadlineLocked()
	h.servers[0].node.mu.Unlock()
	after := h.servers[0].node.electionDeadlineForTest()
	if !after.After(before) || after.Sub(before) == electionMin {
		t.Fatalf("seed=%d: resetElectionDeadlineLocked did not choose a fresh randomized deadline\nbefore=%v after=%v\n%s", h.seed, before, after, h.trace)
	}
}

func TestM01P3VoteRules(t *testing.T) {
	h := NewHarness(t, 3, 3103)
	defer h.Shutdown()

	var r1 RequestVoteReply
	if err := h.servers[0].node.RequestVote(RequestVoteArgs{Term: 1, CandidateID: 1}, &r1); err != nil {
		t.Fatal(err)
	}
	if !r1.VoteGranted || r1.Term != 1 {
		t.Fatalf("seed=%d: first vote got %+v; want grant in term 1\n%s", h.seed, r1, h.trace)
	}

	var retry RequestVoteReply
	_ = h.servers[0].node.RequestVote(RequestVoteArgs{Term: 1, CandidateID: 1}, &retry)
	if !retry.VoteGranted {
		t.Fatalf("seed=%d: retry from same candidate denied; want idempotent grant\n%s", h.seed, h.trace)
	}

	var r2 RequestVoteReply
	_ = h.servers[0].node.RequestVote(RequestVoteArgs{Term: 1, CandidateID: 2}, &r2)
	if r2.VoteGranted {
		t.Fatalf("seed=%d: granted two candidates in same term\n%s", h.seed, h.trace)
	}

	var old RequestVoteReply
	_ = h.servers[0].node.RequestVote(RequestVoteArgs{Term: 0, CandidateID: 2}, &old)
	if old.VoteGranted {
		t.Fatalf("seed=%d: granted stale term vote\n%s", h.seed, h.trace)
	}
}

func TestM01P4InitialElection(t *testing.T) {
	h := NewHarness(t, 3, 4101)
	defer h.Shutdown()

	leader, term := h.CheckSingleLeader()
	if leader < 0 || term <= 0 {
		t.Fatalf("seed=%d: invalid leader=%d term=%d\n%s", h.seed, leader, term, h.trace)
	}
}

func TestM01P4MinorityCannotLead(t *testing.T) {
	h := NewHarness(t, 3, 4201)
	defer h.Shutdown()

	h.Disconnect(0)
	h.Disconnect(1)
	time.Sleep(700 * time.Millisecond)
	h.CheckNoLeader()
}

func TestM01P5LeaderFailure(t *testing.T) {
	h := NewHarness(t, 3, 5101)
	defer h.Shutdown()

	leader, term := h.CheckSingleLeader()
	h.Disconnect(leader)
	newLeader, newTerm := h.CheckSingleLeader()
	if newLeader == leader {
		t.Fatalf("seed=%d: disconnected leader was re-elected\n%s", h.seed, h.trace)
	}
	if newTerm <= term {
		t.Fatalf("seed=%d: new term=%d, old term=%d; want a higher term\n%s", h.seed, newTerm, term, h.trace)
	}
}

func TestM01P5OldLeaderStepsDown(t *testing.T) {
	h := NewHarness(t, 3, 5201)
	defer h.Shutdown()

	oldLeader, _ := h.CheckSingleLeader()
	h.Disconnect(oldLeader)
	newLeader, _ := h.CheckSingleLeader()
	h.Reconnect(oldLeader)

	requireEventually(t, h.seed, h.trace, "old leader to step down", func() bool {
		old := h.servers[oldLeader].Status()
		cur := h.servers[newLeader].Status()
		return old.Role != Leader && cur.Role == Leader
	})
}

func TestM01P6ElectionStress(t *testing.T) {
	for seed := int64(6000); seed < 6010; seed++ {
		t.Run("seed", func(t *testing.T) {
			h := NewHarness(t, 5, seed)
			defer h.Shutdown()

			for i := 0; i < 3; i++ {
				leader, _ := h.CheckSingleLeader()
				h.Disconnect(leader)
				h.CheckSingleLeader()
				h.Reconnect(leader)
				h.CheckSingleLeader()
			}
		})
	}
}
