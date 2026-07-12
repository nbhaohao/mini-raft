package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var errDisconnected = errors.New("peer disconnected")

type faultRule struct {
	drop  bool
	delay time.Duration
}

type network struct {
	mu        sync.Mutex
	nodes     map[int]*Node
	connected map[int]bool
	rng       *rand.Rand
	rules     map[int]faultRule
}

func newNetwork(seed int64) *network {
	return &network{
		nodes:     make(map[int]*Node),
		connected: make(map[int]bool),
		rng:       rand.New(rand.NewSource(seed)),
		rules:     make(map[int]faultRule),
	}
}

func (n *network) addNode(node *Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[node.id] = node
	n.connected[node.id] = true
}

func (n *network) disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = false
}

func (n *network) reconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = true
}

func (n *network) setDelay(id int, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r := n.rules[id]
	r.delay = delay
	n.rules[id] = r
}

func (n *network) setDrop(id int, drop bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r := n.rules[id]
	r.drop = drop
	n.rules[id] = r
}

type nodeCaller struct {
	fromID int
	net    *network
}

func (c *nodeCaller) Call(peerID int, method string, args any, reply any) error {
	return c.net.call(c.fromID, peerID, method, args, reply)
}

func (n *network) call(fromID int, peerID int, method string, args any, reply any) error {
	n.mu.Lock()
	node := n.nodes[peerID]
	connected := n.connected[fromID] && n.connected[peerID]
	rule := n.rules[peerID]
	n.mu.Unlock()

	if node == nil || !connected || rule.drop {
		return errDisconnected
	}
	if rule.delay > 0 {
		time.Sleep(rule.delay)
	}

	switch method {
	case "Node.RequestVote":
		return node.RequestVote(args.(RequestVoteArgs), reply.(*RequestVoteReply))
	case "Node.AppendEntries":
		return node.AppendEntries(args.(AppendEntriesArgs), reply.(*AppendEntriesReply))
	default:
		return errors.New("unknown method: " + method)
	}
}
