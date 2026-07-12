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

	currentTerm int
	votedFor    int
	role        Role
	deadline    time.Time

	done chan struct{}
}

func newNode(id int, peerIDs []int, caller peerCaller, ready <-chan struct{}, seed int64, trace *Trace) *Node {
	n := &Node{
		id:       id,
		peerIDs:  append([]int(nil), peerIDs...),
		caller:   caller,
		trace:    trace,
		rng:      rand.New(rand.NewSource(seed + int64(id)*1009)),
		votedFor: -1,
		role:     Follower,
		done:     make(chan struct{}),
	}
	n.deadline = time.Unix(0, 0).Add(electionMin)

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
	// Red-state scaffold: later phases add randomized deadline checks,
	// elections, guarded vote counting, and heartbeats here.
}

func (n *Node) resetElectionDeadlineLocked() {
	// P2 intentionally starts from a fixed deadline so the red test exposes the
	// missing "new random deadline every round" behavior.
	n.deadline = time.Unix(0, 0).Add(electionMin)
}

func (n *Node) electionDeadlineForTest() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.deadline
}
