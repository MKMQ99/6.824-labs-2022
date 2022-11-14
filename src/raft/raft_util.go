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
	ELECTION_TIMEOUT_MIN = 200

	HeartbeatSleep = 120
	AppliedSleep   = 10
)

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	// log.Printf("查看preinfo: %v\n", rf.nextIndex[server]-1)
	// if rf.nextIndex[server] == 0 {
	// 	return -1, -1
	// }
	return rf.nextIndex[server] - 1, rf.logs[rf.nextIndex[server]-1].Term
	// return rf.getLastIndex(), rf.logs[rf.getLastIndex()].Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
