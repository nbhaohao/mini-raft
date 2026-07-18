package raft

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func newPersistentTestNode(t *testing.T, id int, store StableStore) *Node {
	t.Helper()
	ready := make(chan struct{})
	close(ready)
	net := newNetwork(1)
	n := newNode(id, nil, &nodeCaller{fromID: id, net: net}, ready, 1, NewTrace(1), make(chan CommitEntry, 16), store)
	t.Cleanup(n.Stop)
	return n
}

// countNodeEvents returns only the trace events produced by node `id`. A crashed
// node must stop emitting; the surviving majority keeps writing StartElection
// events under their own ids, so we must not count the whole trace.
func countNodeEvents(h *Harness, id int) int {
	c := 0
	for _, ev := range h.Events() {
		if ev.Node == id {
			c++
		}
	}
	return c
}

// notCommitted asserts no connected server has ever committed cmd.
func notCommitted(t *testing.T, h *Harness, cmd any) {
	t.Helper()
	if c, _ := h.CheckCommitted(cmd); c != 0 {
		t.Fatalf("seed=%d: command %v committed on %d nodes, want none\n%s", h.seed, cmd, c, h.trace)
	}
}

// leaderAmong waits until exactly one node from ids reports Leader and returns it.
func leaderAmong(t *testing.T, h *Harness, ids ...int) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leader := -1
		for _, id := range ids {
			if h.servers[id].Status().Role == Leader {
				if leader >= 0 {
					leader = -2
					break
				}
				leader = id
			}
		}
		if leader >= 0 {
			return leader
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("seed=%d: no single leader among %v\n%s", h.seed, ids, h.trace)
	return -1
}

func TestM03P1RestorePersistentState(t *testing.T) {
	store := NewMemoryStableStore()
	want := PersistentState{CurrentTerm: 7, VotedFor: 2, Log: []LogEntry{{Term: 6, Command: "kept"}}}
	store.Save(want)
	n := newPersistentTestNode(t, 0, store)

	n.mu.Lock()
	got := PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: append([]LogEntry(nil), n.log...)}
	n.mu.Unlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restore got=%+v want=%+v", got, want)
	}

	// The restored log must be a private copy: mutating the node's log must not
	// reach back into the store's snapshot.
	n.mu.Lock()
	n.log[0].Term = 99
	n.mu.Unlock()
	loaded, _ := store.Load()
	if loaded.Log[0].Term != 6 {
		t.Fatalf("node log aliases store backing array: loaded=%+v", loaded.Log)
	}
}

func TestM03P2TermVoteSurviveRestart(t *testing.T) {
	store := NewMemoryStableStore()
	n := newPersistentTestNode(t, 0, store)

	n.mu.Lock()
	n.startElectionLocked()
	n.mu.Unlock()

	state, ok := store.Load()
	if !ok || state.CurrentTerm < 1 || state.VotedFor != 0 {
		t.Fatalf("new term/self vote not saved before unlock: ok=%v state=%+v", ok, state)
	}
}

func TestM03P3LogSurvivesRestart(t *testing.T) {
	store := NewMemoryStableStore()
	n := newPersistentTestNode(t, 0, store)

	n.mu.Lock()
	n.role, n.currentTerm = Leader, 3
	n.mu.Unlock()
	if !n.Submit("persist-me") {
		t.Fatal("leader rejected Submit")
	}

	state, ok := store.Load()
	if !ok || len(state.Log) != 1 || state.Log[0].Command != "persist-me" {
		t.Fatalf("leader log not saved on Submit: ok=%v state=%+v", ok, state)
	}
}

// P4 · a crashed follower must catch up after Restart. The command submitted
// while it was down has to reach it once it comes back from its store.
func TestM03P4FollowerRestartCatchUp(t *testing.T) {
	h := NewHarness(t, 3, 7310)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader()
	follower := (leader + 1) % 3
	h.SubmitToServer(leader, 10)
	h.SubmitToServer(leader, 11)
	h.CheckCommittedN(10, 3)
	h.CheckCommittedN(11, 3)

	h.Crash(follower)
	// Leader plus one live follower is still a majority, so new commands commit.
	h.SubmitToServer(leader, 12)
	h.CheckCommittedN(12, 2)

	h.Restart(follower)
	// Everything, including the entry it never saw, is delivered after replay.
	h.CheckCommittedN(11, 3)
	h.CheckCommittedN(12, 3)
}

// P4 · crashing the leader must not lose already-committed entries; a new leader
// emerges and the restored old leader rejoins as a follower.
func TestM03P4LeaderRestartSafety(t *testing.T) {
	h := NewHarness(t, 3, 7311)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader()
	h.SubmitToServer(leader, 20)
	h.SubmitToServer(leader, 21)
	h.CheckCommittedN(20, 3)
	h.CheckCommittedN(21, 3)

	h.Crash(leader)
	newLeader, _ := h.CheckSingleLeader()
	if newLeader == leader {
		t.Fatalf("seed=7311: crashed leader still reported as leader")
	}
	// A current-term entry keeps the new term making progress on the majority.
	h.SubmitToServer(newLeader, 22)
	h.CheckCommittedN(22, 2)

	h.Restart(leader)
	// The restored node replays its persisted log; committed history is intact.
	h.SubmitToServer(newLeader, 23)
	h.CheckCommittedN(23, 3)
	h.CheckCommittedN(20, 3)
}

// P4 · a full-cluster restart loses all volatile state; nodes must re-elect and
// re-commit their persisted logs, then keep accepting new commands.
func TestM03P4AllRestartContinue(t *testing.T) {
	h := NewHarness(t, 3, 7312)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader()
	h.SubmitToServer(leader, 30)
	h.SubmitToServer(leader, 31)
	h.CheckCommittedN(30, 3)
	h.CheckCommittedN(31, 3)

	for id := 0; id < 3; id++ {
		h.Crash(id)
	}
	for id := 0; id < 3; id++ {
		h.Restart(id)
	}

	newLeader, _ := h.CheckSingleLeader()
	// A current-term entry is required before the previous-term prefix commits.
	h.SubmitToServer(newLeader, 32)
	h.CheckCommittedN(32, 3)
	h.CheckCommittedN(30, 3)
	h.CheckCommittedN(31, 3)
}

// P4 · after Crash, the old Node must be fully dead: no goroutine may keep
// writing trace, RPCs, or the store.
func TestM03P4CrashedNodeStopsWriting(t *testing.T) {
	h := NewHarness(t, 3, 7313)
	defer h.Shutdown()

	id, _ := h.CheckSingleLeader()
	h.Crash(id)
	before := countNodeEvents(h, id)
	time.Sleep(3 * heartbeatInterval)
	if after := countNodeEvents(h, id); after != before {
		t.Fatalf("seed=7313: crashed node %d still produced trace events: before=%d after=%d\n%s", id, before, after, h.trace)
	}
}

// P5 · simplified Figure 8 boss. An entry appended by a since-isolated leader is
// an old-term entry that must never be committed on its own; only a current-term
// entry drives commit, and crash/restart of the new leader must not break that.
func TestM03P5Figure8CurrentTermCommit(t *testing.T) {
	h := NewHarness(t, 3, 7314)
	defer h.Shutdown()

	leader, _ := h.CheckSingleLeader()
	f1, f2 := (leader+1)%3, (leader+2)%3

	// Isolate the leader, then append: 300 stays on the lone old leader only.
	h.Disconnect(leader)
	h.SubmitToServer(leader, 300)

	newLeader := leaderAmong(t, h, f1, f2)
	// A new leader must not directly commit the inherited/old-term entry.
	notCommitted(t, h, 300)

	// A current-term entry reaches the {f1,f2} majority and commits.
	h.SubmitToServer(newLeader, 301)
	h.CheckCommittedN(301, 2)

	// Churn the new leader through a real crash/restart; safety must hold.
	h.Crash(newLeader)
	h.Restart(newLeader)

	// Bring the stale old leader back; 300 gets overwritten, never committed.
	h.Reconnect(leader)
	finalLeader := leaderAmong(t, h, 0, 1, 2)
	h.SubmitToServer(finalLeader, 302)
	h.CheckCommittedN(302, 3)
	notCommitted(t, h, 300)
}

// P6 · faults compose orthogonally: Partition only cuts cross-group links, and
// Heal must restore those links without silently clearing an explicit drop rule.
func TestM03P6PartitionDelayDrop(t *testing.T) {
	h := NewHarness(t, 3, 7315)
	defer h.Shutdown()
	h.CheckSingleLeader()

	h.SetDrop(2, true)
	h.Partition([]int{0, 1}, []int{2})
	if h.net.canCommunicate(0, 2) || h.net.canCommunicate(1, 2) {
		t.Fatalf("seed=7315: cross-partition link still open")
	}
	// The majority side keeps a leader; the isolated minority cannot lead.
	leaderAmong(t, h, 0, 1)

	h.Heal()
	if !h.net.canCommunicate(0, 2) || !h.net.canCommunicate(1, 2) {
		t.Fatalf("seed=7315: Heal did not restore cross-partition links")
	}
	h.net.mu.Lock()
	dropStill := h.net.rules[2].drop
	h.net.mu.Unlock()
	if !dropStill {
		t.Fatalf("seed=7315: Heal wrongly cleared the explicit drop rule on node 2")
	}
}

// P6 · persistence stress: replay the same crash/restart cycle across the fixed
// seed set so a failure can be reproduced by seed.
func TestM03P6PersistenceStress(t *testing.T) {
	for seed := int64(7310); seed <= 7319; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			h := NewHarness(t, 3, seed)
			defer h.Shutdown()

			leader, _ := h.CheckSingleLeader()
			follower := (leader + 1) % 3
			base := int(seed) * 10
			h.SubmitToServer(leader, base)
			h.CheckCommittedN(base, 3)

			// Crash a follower: the leader keeps a majority, so we exercise the
			// persist/restart/catch-up path without a fragile 2-node election.
			h.Crash(follower)
			h.SubmitToServer(leader, base+1)
			h.CheckCommittedN(base+1, 2)

			h.Restart(follower)
			h.SubmitToServer(leader, base+2)
			h.CheckCommittedN(base+2, 3)
			h.CheckCommittedN(base, 3)
		})
	}
}
