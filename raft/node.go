package raft

import (
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Status struct {
	ID   int
	Term int
	Role Role
}

type peerCaller interface {
	Call(peerID int, method string, args any, reply any) error
}

const (
	heartbeatInterval = 120 * time.Millisecond
	electionMin       = 300 * time.Millisecond
	electionJitter    = 300 * time.Millisecond
	// leaderLease：失联 leader 在一个 lease 内确认不到多数派联系就自行让位。
	// 必须 > heartbeatInterval（健康 leader 每轮续租不被误判），且 < electionMin
	// （配合 leader-stickiness 把新选举窗口拉到 ≥ electionMin，让位先于新主产生）。
	leaderLease = 2 * heartbeatInterval
)

type Node struct {
	mu sync.Mutex

	id      int
	peerIDs []int
	caller  peerCaller
	trace   *Trace
	rng     *rand.Rand

	currentTerm       int
	votedFor          int
	role              Role
	deadline          time.Time
	nextHeartbeat     time.Time
	lastLeaderContact time.Time
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	commitC           chan<- CommitEntry
	commitNotify      chan struct{}
	nextIndex         map[int]int
	matchIndex        map[int]int

	done chan struct{}
}

func newNode(id int, peerIDs []int, caller peerCaller, ready <-chan struct{}, seed int64, trace *Trace, commitC chan<- CommitEntry) *Node {
	n := &Node{
		id:           id,
		peerIDs:      append([]int(nil), peerIDs...),
		caller:       caller,
		trace:        trace,
		rng:          rand.New(rand.NewSource(seed + int64(id)*1009)),
		currentTerm:  0,
		votedFor:     -1,
		role:         Follower,
		commitIndex:  -1,
		lastApplied:  -1,
		commitC:      commitC,
		commitNotify: make(chan struct{}, 1),
		nextIndex:    make(map[int]int),
		matchIndex:   make(map[int]int),
		done:         make(chan struct{}),
	}
	n.resetElectionDeadlineLocked()

	go func() {
		<-ready
		n.run()
	}()
	return n
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Term: n.currentTerm, Role: n.role}
}

func (n *Node) Stop() {
	select {
	case <-n.done:
	default:
		close(n.done)
	}
}

func (n *Node) run() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.tick()
		case <-n.done:
			return
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		if time.Now().After(n.deadline) {
			n.role = Follower
			n.resetElectionDeadlineLocked()
			n.recordLocked("StepDown", "no majority contact within lease; back to follower")
			return
		}
		if time.Now().After(n.nextHeartbeat) {
			n.broadcastHeartbeatLocked()
		}
		return
	}
	if time.Now().After(n.deadline) {
		n.startElectionLocked()
	}
}

func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.resetElectionDeadlineLocked()
	electionTerm := n.currentTerm
	n.recordLocked("StartElection", "term=%d self-vote", electionTerm)

	votes := 1
	lastIndex, lastTerm := n.lastLogInfoLocked()
	for _, peer := range n.peerIDs {
		go n.collectVote(peer, electionTerm, lastIndex, lastTerm, &votes)
	}
}

func (n *Node) collectVote(peer int, electionTerm, lastIndex, lastTerm int, votes *int) {
	args := RequestVoteArgs{Term: electionTerm, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	var reply RequestVoteReply
	if err := n.caller.Call(peer, "Node.RequestVote", args, &reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.votedFor = -1
		n.role = Follower
		n.resetElectionDeadlineLocked()
		n.recordLocked("StepDown", "reply term=%d > currentTerm; back to follower", reply.Term)
		return
	}

	if n.currentTerm != electionTerm || n.role != Candidate {
		return
	}
	if !reply.VoteGranted {
		return
	}

	*votes++
	if *votes > (len(n.peerIDs)+1)/2 {
		n.role = Leader
		n.initReplicationStateLocked()
		n.refreshLeaderDeadlineLocked()
		n.recordLocked("BecomeLeader", "term=%d votes=%d", n.currentTerm, *votes)
		n.broadcastHeartbeatLocked()
	}
}

func (n *Node) broadcastHeartbeatLocked() {
	// P3 起心跳与真实 entries 走同一条复制路径：空心跳 = entries 恰好为空的 AppendEntries。
	n.broadcastAppendEntriesLocked()
}

func (n *Node) Submit(command any) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return false
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: command})
	index := len(n.log) - 1
	n.recordLocked("Submit", "index=%d term=%d accepted (leader-local only)", index, n.currentTerm)
	return true
}

func (n *Node) initReplicationStateLocked() {
	next := len(n.log)
	for _, peer := range n.peerIDs {
		n.nextIndex[peer] = next
		n.matchIndex[peer] = -1
	}
}

func (n *Node) sendHeartbeat(peer int, term int, acks *int) {
	args := AppendEntriesArgs{Term: term, LeaderID: n.id, PrevLogIndex: -1, PrevLogTerm: -1, LeaderCommit: -1}
	var reply AppendEntriesReply
	if err := n.caller.Call(peer, "Node.AppendEntries", args, &reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.votedFor = -1
		n.role = Follower
		n.resetElectionDeadlineLocked()
		n.recordLocked("StepDown", "heartbeat reply term=%d > currentTerm; back to follower", reply.Term)
		return
	}

	if n.role != Leader || n.currentTerm != term {
		return
	}
	if !reply.Success {
		return
	}
	*acks++
	if *acks > (len(n.peerIDs)+1)/2 {
		n.refreshLeaderDeadlineLocked()
	}
}

func (n *Node) refreshLeaderDeadlineLocked() {
	n.deadline = time.Now().Add(leaderLease)
}

func (n *Node) resetElectionDeadlineLocked() {
	jitter := time.Duration(n.rng.Int63n(int64(electionJitter)))
	base := time.Now()
	if n.deadline.After(base) {
		base = n.deadline
	}
	n.deadline = base.Add(electionMin + jitter)
}

func (n *Node) electionDeadlineForTest() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.deadline
}
