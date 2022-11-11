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

	ELECTION_TIMEOUT_MAX = 175
	ELECTION_TIMEOUT_MIN = 75

	HeartbeatSleep = 35
)

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return 0
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}
