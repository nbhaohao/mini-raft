package raft

import "time"

type LogEntry struct {
	Term    int
	Command any
}

type CommitEntry struct {
	Command any
	Index   int
	Term    int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
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
	n.lastLeaderContact = time.Now()
	n.resetElectionDeadlineLocked()
	// 你来实现（P2 follower 只在共同前缀匹配后改日志）：
	// 1. 校验 prev index/term；2. 从首个冲突处截断并追加；3. 幂等重放不增长；
	// 4. LeaderCommit 只采纳到本地日志尾端。P2 不负责 leader 的多数提交。
	reply.Success = true

	n.recordLocked("AppendEntries", "term=%d leader=%d success=%v", args.Term, args.LeaderID, reply.Success)
	return nil
}

func (n *Node) lastLogInfoLocked() (int, int) {
	// 你来实现（P5 把最后一条日志的 index/term 放进 RequestVote）：
	// 空日志返回 -1/-1；非空返回最后一格的位置与 term。
	return -1, -1
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) recordLocked(kind, format string, args ...any) {
	if n.trace == nil {
		return
	}
	n.trace.Add(n.id, n.currentTerm, n.role, kind, format, args...)
}
