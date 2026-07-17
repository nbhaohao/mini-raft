package raft

import "time"

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
	// 成功：follower 已接纳「这次发出的」entries。matchIndex 只能推到本 RPC 真正送达的位置，
	// 即 prev + 本次 entries 条数——不是 leader 日志尾端（leader 可能更领先，follower 没收到那么多）。
	newMatch := args.PrevLogIndex + len(args.Entries)
	if newMatch > n.matchIndex[peer] { // 单调前进：陈旧回包不得让 matchIndex 回退
		n.matchIndex[peer] = newMatch
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1
	n.advanceCommitIndexLocked()
	*acks++
	if *acks > (len(n.peerIDs)+1)/2 {
		n.refreshLeaderDeadlineLocked() // 多数派联系确认，续 leader lease
	}
}

func (n *Node) broadcastAppendEntriesLocked() {
	n.nextHeartbeat = time.Now().Add(heartbeatInterval)
	term := n.currentTerm
	acks := 1 // 自己先算一票
	for _, peer := range n.peerIDs {
		go n.replicateToPeer(peer, term, &acks)
	}
}

func (n *Node) advanceCommitIndexLocked() {
	for index := len(n.log) - 1; index > n.commitIndex; index-- {
		if n.log[index].Term != n.currentTerm {
			continue
		}

		count := 1 // leader 本地已有该 entry
		for _, peer := range n.peerIDs {
			if n.matchIndex[peer] >= index {
				count++
			}
		}
		if count > (len(n.peerIDs)+1)/2 {
			n.commitIndex = index
			return
		}
	}
}

func (n *Node) notifyCommitLocked() {
	// 你来实现（P4 只发轻量通知，不能持 Node.mu 阻塞写 commitC）。
}

func (n *Node) runCommitSender() {
	// 你来实现（P4 使用单一长期 goroutine 串行投递）：
	// 锁内快照 commitIndex/lastApplied，锁外按 index 写 CommitEntry。
	<-n.done
}
