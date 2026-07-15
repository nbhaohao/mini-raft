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

	// prev 一致性检查：PrevLogIndex=-1 表示空前缀（entries 从下标 0 起，无需匹配任何已有格）。
	// 否则本地必须已有该格且 term 相同，才证明共同前缀对齐；越界或 term 不符则拒绝，等 leader 回退重试。
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			n.recordLocked("AppendEntries", "term=%d leader=%d success=false (prev mismatch: prevIdx=%d)", args.Term, args.LeaderID, args.PrevLogIndex)
			return nil
		}
	}
	// 对齐共同前缀：逐格比对 entries，遇到第一个 term 冲突处才截断并追加剩余。
	// 已有且一致的格幂等跳过——不整条清空，才不会误删共同前缀里已提交的日志。
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(n.log) {
			if n.log[idx].Term == entry.Term {
				continue // 已有且 term 一致：幂等重放，不增长
			}
			n.log = n.log[:idx] // 冲突：从此格起截断，保留 [0,idx) 共同前缀
		}
		n.log = append(n.log, args.Entries[i:]...) // 追加冲突点起的全部剩余
		break
	}
	// 采纳 leader 的提交进度，但至多到本地日志尾端：follower 不能提交一个自己还没收到的 index。
	// 真正把 committed 日志投递给状态机（写 commitC）留给 P4。
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = minInt(args.LeaderCommit, len(n.log)-1)
	}
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
