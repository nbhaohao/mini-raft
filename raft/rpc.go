package raft

type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		n.recordLocked("RequestVote", "term=%d candidate=%d grant=%v (stale term)", args.Term, args.CandidateID, reply.VoteGranted)
		return nil
	}

	n.recordLocked("RequestVote", "term=%d candidate=%d grant=%v", args.Term, args.CandidateID, reply.VoteGranted)
	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.Success = false
	n.recordLocked("AppendEntries", "term=%d leader=%d success=%v", args.Term, args.LeaderID, reply.Success)
	return nil
}

func (n *Node) recordLocked(kind, format string, args ...any) {
	if n.trace == nil {
		return
	}
	n.trace.Add(n.id, n.currentTerm, n.role, kind, format, args...)
}
