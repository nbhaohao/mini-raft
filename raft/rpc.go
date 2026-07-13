package raft

import "time"

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

	// leader-stickiness：若在最小选举超时内刚从合法 leader 收到过心跳，
	// 拒绝任何 RequestVote 且不采纳其 term——挡住断连后凭抬高的 term 回来
	// 捣乱的节点，把新选举窗口强制拉到 >= electionMin。
	if time.Since(n.lastLeaderContact) < electionMin {
		n.recordLocked("RequestVote", "term=%d candidate=%d grant=%v (leader sticky)", args.Term, args.CandidateID, reply.VoteGranted)
		return nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = -1
		n.role = Follower
		reply.Term = n.currentTerm
	}

	if n.votedFor == -1 || n.votedFor == args.CandidateID {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionDeadlineLocked()
	}

	n.recordLocked("RequestVote", "term=%d candidate=%d grant=%v", args.Term, args.CandidateID, reply.VoteGranted)
	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.Success = false

	if args.Term < n.currentTerm {
		n.recordLocked("AppendEntries", "term=%d leader=%d success=%v (stale term)", args.Term, args.LeaderID, reply.Success)
		return nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = -1
	}
	n.role = Follower
	reply.Term = n.currentTerm
	reply.Success = true
	n.lastLeaderContact = time.Now()
	n.resetElectionDeadlineLocked()

	n.recordLocked("AppendEntries", "term=%d leader=%d success=%v", args.Term, args.LeaderID, reply.Success)
	return nil
}

func (n *Node) recordLocked(kind, format string, args ...any) {
	if n.trace == nil {
		return
	}
	n.trace.Add(n.id, n.currentTerm, n.role, kind, format, args...)
}
