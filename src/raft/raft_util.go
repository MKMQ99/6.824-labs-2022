package raft

import (
	"math/rand"
	"time"
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	TO_FOLLOWER  = 0
	TO_CANDIDATE = 1
	TO_LEADER    = 2

	ELECTION_TIMEOUT_MAX = 400
	ELECTION_TIMEOUT_MIN = 250

	HeartbeatSleep = 100
	AppliedSleep   = 10
)

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludeIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs)-1 == 0 {
		return rf.lastIncludeTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	// return rf.nextIndex[server] - 1, rf.logs[rf.nextIndex[server]-1].Term
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	// 前面 leader 发送的 PrevLogIndex 太老会返回 UpNextIndex = PrevLogIndex + 1, 同时设置 nextIndex[server] 也为 PrevLogIndex + 1, 可能超出了 lastIndex
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logs[curIndex-rf.lastIncludeIndex]
}

// 通过快照偏移还原真实日志任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex-rf.lastIncludeIndex == 0 {
		return rf.lastIncludeTerm
	}
	return rf.logs[curIndex-rf.lastIncludeIndex].Term
}

// lab3B 使用
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetMe() int {
	return rf.me
}
