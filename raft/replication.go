package raft

// replication.go is the learner-owned protocol surface for m02. Transport,
// fault injection, and trace collection remain in the files supplied in m01.

func (n *Node) appendArgsForPeerLocked(peer int) AppendEntriesArgs {
	next := n.nextIndex[peer]
	prevIndex := next - 1 // 共同前缀的最后一格：nextIndex 前一格
	prevTerm := -1        // prevIndex=-1（从头发）时无前一格，用哨兵 -1
	if prevIndex >= 0 {
		prevTerm = n.log[prevIndex].Term
	}
	// 拷贝一份 entries，避免与本地 n.log 共享底层数组（RPC 时已释放锁，log 可能被并发改）。
	entries := append([]LogEntry(nil), n.log[next:]...)
	return AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

func (n *Node) replicateToPeer(peer int, term int, acks *int) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	args := n.appendArgsForPeerLocked(peer)
	n.mu.Unlock() // RPC 等待期间不持锁

	var reply AppendEntriesReply
	if err := n.caller.Call(peer, "Node.AppendEntries", args, &reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 回包见更高 term：立即清票降级，放弃 leader 身份。
	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.votedFor = -1
		n.role = Follower
		n.resetElectionDeadlineLocked()
		n.recordLocked("StepDown", "append reply term=%d > currentTerm; back to follower", reply.Term)
		return
	}
	// 复查：RPC 往返期间可能已被降级或换届，陈旧回包不得改状态。
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if !reply.Success {
		// 共同前缀未对齐：nextIndex 回退一格，下一轮心跳用更早的 prev 重试。
		if n.nextIndex[peer] > 0 {
			n.nextIndex[peer]--
		}
		n.recordLocked("Replicate", "peer=%d rejected; nextIndex-- -> %d", peer, n.nextIndex[peer])
		return
	}
	// 你来实现（P3 S3）：成功路径推进 matchIndex/nextIndex + 多数 ack 续 lease。
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
