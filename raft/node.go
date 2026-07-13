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
)

type Node struct {
	mu sync.Mutex

	id      int
	peerIDs []int
	caller  peerCaller
	trace   *Trace
	rng     *rand.Rand

	currentTerm   int
	votedFor      int
	role          Role
	deadline      time.Time
	nextHeartbeat time.Time

	done chan struct{}
}

func newNode(id int, peerIDs []int, caller peerCaller, ready <-chan struct{}, seed int64, trace *Trace) *Node {
	n := &Node{
		id:          id,
		peerIDs:     append([]int(nil), peerIDs...),
		caller:      caller,
		trace:       trace,
		rng:         rand.New(rand.NewSource(seed + int64(id)*1009)),
		currentTerm: 0,
		votedFor:    -1,
		role:        Follower,
		done:        make(chan struct{}),
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
	for _, peer := range n.peerIDs {
		go n.collectVote(peer, electionTerm, &votes)
	}
}

func (n *Node) collectVote(peer int, electionTerm int, votes *int) {
	args := RequestVoteArgs{Term: electionTerm, CandidateID: n.id}
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
		n.recordLocked("BecomeLeader", "term=%d votes=%d", n.currentTerm, *votes)
		n.broadcastHeartbeatLocked()
	}
}

func (n *Node) broadcastHeartbeatLocked() {
	n.nextHeartbeat = time.Now().Add(heartbeatInterval)
	term := n.currentTerm
	for _, peer := range n.peerIDs {
		go n.sendHeartbeat(peer, term)
	}
}

func (n *Node) sendHeartbeat(peer int, term int) {
	args := AppendEntriesArgs{Term: term, LeaderID: n.id}
	var reply AppendEntriesReply
	if err := n.caller.Call(peer, "Node.AppendEntries", args, &reply); err != nil {
		return
	}
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
