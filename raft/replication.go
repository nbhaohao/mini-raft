package raft

// replication.go is the learner-owned protocol surface for m02. Transport,
// fault injection, and trace collection remain in the files supplied in m01.

func (n *Node) appendArgsForPeerLocked(peer int) AppendEntriesArgs {
	// 你来实现（P3 从 nextIndex 推出共同前缀与待发送后缀）：
	// PrevLogIndex 指向 nextIndex 前一格；Entries 从 nextIndex 开始复制。
	return AppendEntriesArgs{Term: n.currentTerm, LeaderID: n.id, PrevLogIndex: -1, PrevLogTerm: -1, LeaderCommit: n.commitIndex}
}

func (n *Node) replicateToPeer(peer int, term int, acks *int) {
	// 你来实现（P3 RPC 等待不持锁，回包后再复查 term 与 role）：
	// success 推进 nextIndex/matchIndex；日志不匹配才回退并重试。
}

func (n *Node) broadcastAppendEntriesLocked() {
	// 你来实现（P3 用同一条 AppendEntries 路径承载空心跳和真实 entries）：
	// 每个 peer 启动一次复制，并沿用 m01 的多数 ack leader lease。
}

func (n *Node) advanceCommitIndexLocked() {
	// 你来实现（P4 只让 currentTerm entry 直接推进 commitIndex）：
	// 从日志尾部向前找多数 matchIndex；旧 term 前缀只能被间接提交。
}

func (n *Node) notifyCommitLocked() {
	// 你来实现（P4 只发轻量通知，不能持 Node.mu 阻塞写 commitC）。
}

func (n *Node) runCommitSender() {
	// 你来实现（P4 使用单一长期 goroutine 串行投递）：
	// 锁内快照 commitIndex/lastApplied，锁外按 index 写 CommitEntry。
	<-n.done
}
