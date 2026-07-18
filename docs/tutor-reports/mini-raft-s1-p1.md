# mini-raft m03 S1 P1 学习报告

## 本 phase 规则与不变量

- “稳定状态深拷贝；恢复先于 goroutine。”
- 持久状态是 `currentTerm`、`votedFor`、`log`。
- `role`、`deadline`、`commitIndex`、`lastApplied`、`nextIndex`、`matchIndex` 是易失状态，重启后重新建立。
- `Save` 与 `Load` 都复制 `log`，Node 与 store 不能共享底层数组。
- `commitIndex` 不是单靠日志内容就能推出；日志只能证明有哪些历史，提交还需要新 leader 依据多数派复制状态确认。

## 核心反例一句话

若只复制 slice header，节点修改自己的日志就会改写 store 的快照，恢复读到的内容不再是 crash 前状态。

## 我答错或卡壳的点与纠正

- 一开始不确定 `newNode` 所在文件；已定位到 `raft/node.go`。
- 一开始说 `commitIndex` 可由日志重放推导；纠正为：日志可持久化，但提交进度还需新 leader 根据多数派复制状态重新确认，不能仅凭日志推出。

## 3 个一句话自检

1. 为什么 `Load` 必须发生在 `go n.run()` 和 `go n.runCommitSender()` 之前？
2. 如果 `Save` 只保存 slice header，节点后续改日志会破坏什么不变量？
3. leader 断电重启后，哪些字段必须保留，哪些字段必须重新计算？
