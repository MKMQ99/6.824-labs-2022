package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config        // indexed by config num
	seqMap    map[int64]int   //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan Op //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
}

type Op struct {
	// Your data here.
	OpType      string
	ClientId    int64
	SeqId       int
	QueryNum    int
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: "Join", SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func(idx int) {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}(lastIndex)

	select {
	case <-ch:
		reply.Err = OK
		return
	case <-time.After(REPLY_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: "Leave", SeqId: args.SeqId, ClientId: args.ClientId, LeaveGids: args.GIDs}
	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func(idx int) {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}(lastIndex)

	select {
	case <-ch:
		reply.Err = OK
		return
	case <-time.After(REPLY_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: "Move", SeqId: args.SeqId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGid: args.GID}
	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func(idx int) {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}(lastIndex)

	select {
	case <-ch:
		reply.Err = OK
		return
	case <-time.After(REPLY_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: "Query", SeqId: args.SeqId, ClientId: args.ClientId, QueryNum: args.Num}
	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func(idx int) {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}(lastIndex)

	select {
	case <-ch:
		reply.Err = OK
		sc.mu.Lock()
		if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[op.QueryNum]
		}
		sc.mu.Unlock()
		return
	case <-time.After(REPLY_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)
	go sc.applyMsgHandlerLoop()
	return sc
}
