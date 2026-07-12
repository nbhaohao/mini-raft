package raft

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type TraceEvent struct {
	At     time.Time
	Node   int
	Term   int
	Role   Role
	Kind   string
	Detail string
}

type Trace struct {
	mu     sync.Mutex
	seed   int64
	events []TraceEvent
}

func NewTrace(seed int64) *Trace {
	return &Trace{seed: seed}
}

func (t *Trace) Add(node int, term int, role Role, kind string, format string, args ...any) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, TraceEvent{
		At:     time.Now(),
		Node:   node,
		Term:   term,
		Role:   role,
		Kind:   kind,
		Detail: fmt.Sprintf(format, args...),
	})
}

func (t *Trace) Events() []TraceEvent {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]TraceEvent(nil), t.events...)
}

func (t *Trace) String() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d events=%d\n", t.seed, len(t.events))
	start := 0
	if len(t.events) > 20 {
		start = len(t.events) - 20
	}
	for _, ev := range t.events[start:] {
		fmt.Fprintf(&b, "node=%d term=%d role=%s kind=%s %s\n", ev.Node, ev.Term, ev.Role, ev.Kind, ev.Detail)
	}
	return b.String()
}
