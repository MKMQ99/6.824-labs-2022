package kvraft

import (
	"bytes"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func init() {
	// 获取日志文件句柄
	// 以 只写入文件|没有时创建|文件尾部追加 的形式打开这个文件
	os.Remove(`./log.log`)
	// logFile, err := os.OpenFile(`./log.log`, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	logFile, err := os.OpenFile(`/dev/null`, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	// 设置存储位置
	log.SetOutput(logFile)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	OpType   string
}

type ClientRecord struct {
	RequestID    int
	LastResponse *CommandReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sessions  map[int64]ClientRecord     //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan *CommandReply //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	kvPersist map[string]string          // 存储持久化的KV键值对	K / V

	lastIncludeIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// _, ifLeader := kv.rf.GetState()
	// if !ifLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	// 通过raft.Start() 提交Op给raft Leader, 然后开启特殊的Wait Channel 等待Raft 返回给自己这个Req结果。
	op := Op{OpType: "Get", Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getWaitCh(lastIndex)
	kv.mu.Unlock()

	select {
	case agreement := <-ch:
		reply.Err = agreement.Err
		reply.Value = agreement.Value
	case <-time.After(REPLY_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	go func() {
		kv.mu.Lock()
		kv.killWaitCh(lastIndex)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	// _, ifLeader := kv.rf.GetState()
	// if !ifLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	kv.mu.Lock()
	if kv.ifDuplicate(args.ClientId, args.SeqId) {
		reply.Err = kv.sessions[args.ClientId].LastResponse.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 封装Op传到下层start
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getWaitCh(lastIndex)
	kv.mu.Unlock()

	select {
	case agreement := <-ch:
		reply.Err = agreement.Err
	case <-time.After(REPLY_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	go func() {
		kv.mu.Lock()
		kv.killWaitCh(lastIndex)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.sessions = make(map[int64]ClientRecord)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan *CommandReply)

	kv.lastIncludeIndex = -1

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	// go kv.applyMsgHandlerLoop()
	go kv.applier()

	return kv
}

func (kv *KVServer) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	log.Printf("[log] raftId: %v\nkvPersist: %v\nseqMap: %v\n", kv.rf.GetMe(), kv.kvPersist, kv.sessions)
	e.Encode(kv.kvPersist)
	e.Encode(kv.sessions)
	e.Encode(kv.lastIncludeIndex)
	data := w.Bytes()
	return data
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvPersist map[string]string
	var sessions map[int64]ClientRecord
	var lastIncludeIndex int
	if d.Decode(&kvPersist) == nil && d.Decode(&sessions) == nil && d.Decode(&lastIncludeIndex) == nil {
		kv.kvPersist = kvPersist
		kv.sessions = sessions
		kv.lastIncludeIndex = lastIncludeIndex
	} else {
		log.Printf("[Server(%v)] Failed to decode snapshot!!!", kv.me)
	}
}
