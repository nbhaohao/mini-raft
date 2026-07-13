package raft

import (
	"testing"
	"time"
)

func TestM02P1SubmitLeaderOnly(t *testing.T) {
	h := NewHarness(t, 3, 7201)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	for _, s := range h.servers {
		if s.id == leader {
			continue
		}
		if h.SubmitToServer(s.id, "follower-must-reject") {
			t.Fatalf("seed=%d: follower %d accepted Submit\n%s", h.seed, s.id, h.trace)
		}
	}
	n := h.servers[leader].node
	n.mu.Lock()
	for _, peer := range n.peerIDs {
		next, nextOK := n.nextIndex[peer]
		match, matchOK := n.matchIndex[peer]
		if !nextOK || !matchOK || next != 0 || match != -1 {
			t.Fatalf("seed=%d: peer=%d next=(%d,%v) match=(%d,%v); want 0/-1 initialized at election\n%s", h.seed, peer, next, nextOK, match, matchOK, h.trace)
		}
	}
	n.mu.Unlock()
	if !h.SubmitToServer(leader, "m02-p1") {
		t.Fatalf("seed=%d: leader rejected Submit\n%s", h.seed, h.trace)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 1 || n.log[0].Command != "m02-p1" || n.log[0].Term != n.currentTerm {
		t.Fatalf("seed=%d: leader log=%v; want one current-term entry\n%s", h.seed, n.log, h.trace)
	}
}

func TestM02P2ConsistencyCheck(t *testing.T) {
	h := NewHarness(t, 1, 7202)
	defer h.Shutdown()
	n := h.servers[0].node
	n.mu.Lock()
	n.currentTerm = 3
	n.log = []LogEntry{{Term: 1, Command: "a"}, {Term: 2, Command: "stale"}}
	n.mu.Unlock()

	var rejected AppendEntriesReply
	_ = n.AppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 9, PrevLogIndex: 1, PrevLogTerm: 1}, &rejected)
	if rejected.Success {
		t.Fatalf("seed=%d: mismatched prev log accepted\n%s", h.seed, h.trace)
	}
	var accepted AppendEntriesReply
	_ = n.AppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: 9, PrevLogIndex: 0, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Command: "b"}}, LeaderCommit: 1,
	}, &accepted)
	if !accepted.Success || len(n.log) != 2 || n.log[1].Command != "b" {
		t.Fatalf("seed=%d: conflict suffix was not replaced: reply=%+v log=%+v\n%s", h.seed, accepted, n.log, h.trace)
	}
	var repeated AppendEntriesReply
	_ = n.AppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: 9, PrevLogIndex: 0, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Command: "b"}}, LeaderCommit: 99,
	}, &repeated)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !repeated.Success || len(n.log) != 2 {
		t.Fatalf("seed=%d: replay duplicated entries: reply=%+v log=%+v\n%s", h.seed, repeated, n.log, h.trace)
	}
	if n.commitIndex != len(n.log)-1 {
		t.Fatalf("seed=%d: commitIndex=%d; want capped at local tail=%d\n%s", h.seed, n.commitIndex, len(n.log)-1, h.trace)
	}
}

func TestM02P3ReplicationConverges(t *testing.T) {
	h := NewHarness(t, 3, 7203)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	if !h.SubmitToServer(leader, "m02-p3") {
		t.Fatalf("seed=%d: leader rejected Submit\n%s", h.seed, h.trace)
	}
	h.CheckLogN("m02-p3", 3)
}

func TestM02P4CommitOneCommand(t *testing.T) {
	h := NewHarness(t, 3, 7204)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	if !h.SubmitToServer(leader, "one") {
		t.Fatalf("seed=%d: leader rejected Submit\n%s", h.seed, h.trace)
	}
	count, index := h.CheckCommittedN("one", 3)
	if count != 3 || index < 0 {
		t.Fatalf("seed=%d: got count=%d index=%d\n%s", h.seed, count, index, h.trace)
	}
}

func TestM02P4CommitMultipleCommands(t *testing.T) {
	h := NewHarness(t, 3, 7205)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	for _, cmd := range []string{"a", "b", "c"} {
		if !h.SubmitToServer(leader, cmd) {
			t.Fatalf("seed=%d: leader rejected Submit %q\n%s", h.seed, cmd, h.trace)
		}
	}
	for index, cmd := range []string{"a", "b", "c"} {
		count, gotIndex := h.CheckCommittedN(cmd, 3)
		if count != 3 || gotIndex != index {
			t.Fatalf("seed=%d: command=%q count=%d index=%d; want all nodes at index=%d\n%s", h.seed, cmd, count, gotIndex, index, h.trace)
		}
	}
}

func TestM02P4NoCommitWithoutQuorum(t *testing.T) {
	h := NewHarness(t, 3, 7206)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	h.Disconnect(leader)
	// Submit before the m01 lease expires: acceptance is local, commit needs quorum.
	if !h.SubmitToServer(leader, "isolated") {
		t.Fatalf("seed=%d: isolated leader should still accept before it notices quorum loss\n%s", h.seed, h.trace)
	}
	time.Sleep(350 * time.Millisecond)
	if count, _ := h.CheckCommitted("isolated"); count != 0 {
		t.Fatalf("seed=%d: isolated command committed on %d nodes\n%s", h.seed, count, h.trace)
	}
}

func TestM02P5ElectionRestriction(t *testing.T) {
	h := NewHarness(t, 2, 7207)
	defer h.Shutdown()
	n := h.servers[0].node
	n.mu.Lock()
	n.currentTerm = 4
	n.log = []LogEntry{{Term: 1, Command: "old"}, {Term: 4, Command: "new"}}
	n.votedFor = -1
	n.lastLeaderContact = time.Time{}
	n.mu.Unlock()
	var staleTerm RequestVoteReply
	_ = n.RequestVote(RequestVoteArgs{Term: 5, CandidateID: 1, LastLogIndex: 100, LastLogTerm: 3}, &staleTerm)
	if staleTerm.VoteGranted {
		t.Fatalf("seed=%d: stale candidate received a vote\n%s", h.seed, h.trace)
	}
	var higherTermShorter RequestVoteReply
	_ = n.RequestVote(RequestVoteArgs{Term: 5, CandidateID: 1, LastLogIndex: 0, LastLogTerm: 5}, &higherTermShorter)
	if !higherTermShorter.VoteGranted {
		t.Fatalf("seed=%d: higher-last-term candidate was rejected because its index was shorter\n%s", h.seed, h.trace)
	}
}

func TestM02P6FollowerCatchUp(t *testing.T) {
	h := NewHarness(t, 3, 7208)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	follower := (leader + 1) % 3
	h.Disconnect(follower)
	if !h.SubmitToServer(leader, "catch-up") {
		t.Fatalf("seed=%d: leader rejected Submit\n%s", h.seed, h.trace)
	}
	h.CheckCommittedN("catch-up", 2)
	if count, _ := h.countLogs("catch-up"); count != 2 {
		t.Fatalf("seed=%d: disconnected follower did not actually miss the entry; log count=%d\n%s", h.seed, count, h.trace)
	}
	h.Reconnect(follower)
	h.CheckLogN("catch-up", 3)
	h.CheckCommittedN("catch-up", 3)
}

func TestM02P6LeaderChangeConflictOverwrite(t *testing.T) {
	h := NewHarness(t, 3, 7209)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	h.Disconnect(leader)
	if !h.SubmitToServer(leader, "old-uncommitted") {
		t.Fatalf("seed=%d: isolated leader should accept its divergent local suffix\n%s", h.seed, h.trace)
	}
	newLeader, _ := h.CheckSingleLeader()
	if newLeader == leader {
		t.Fatalf("seed=%d: disconnected leader was re-elected\n%s", h.seed, h.trace)
	}
	if !h.SubmitToServer(newLeader, "new-term") {
		t.Fatalf("seed=%d: new leader rejected Submit\n%s", h.seed, h.trace)
	}
	h.CheckCommittedN("new-term", 2)
	h.Reconnect(leader)
	h.CheckCommittedN("new-term", 3)
	if count, _ := h.CheckCommitted("old-uncommitted"); count != 0 {
		t.Fatalf("seed=%d: divergent old-leader entry became committed on %d nodes\n%s", h.seed, count, h.trace)
	}
	for _, server := range h.servers {
		server.node.mu.Lock()
		for _, entry := range server.node.log {
			if entry.Command == "old-uncommitted" {
				server.node.mu.Unlock()
				t.Fatalf("seed=%d: node=%d retained conflicting old suffix log=%v\n%s", h.seed, server.id, server.node.log, h.trace)
			}
		}
		server.node.mu.Unlock()
	}
}

func TestM02P6ReplicationStress(t *testing.T) {
	for seed := int64(7210); seed < 7220; seed++ {
		t.Run("seed", func(t *testing.T) {
			h := NewHarness(t, 5, seed)
			defer h.Shutdown()
			leader, _ := h.CheckSingleLeader()
			if !h.SubmitToServer(leader, seed) {
				t.Fatalf("seed=%d: leader rejected Submit\n%s", seed, h.trace)
			}
			h.CheckCommittedN(seed, 5)
		})
	}
}
