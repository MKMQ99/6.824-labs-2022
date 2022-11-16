package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int        // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int        // 在当前获得选票的候选人的 Id
	logs        []LogEntry // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	// Volatile state on all servers
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	// Volatile state on leaders
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值
	// 以上成员来源于论文

	getVoteNum            int           // 记录此次投票中获取的票数 2A
	state                 State         // 记录当前是三个状态里的哪一个 2A
	lastResetElectionTime time.Time     // 最后一次更改时间 2A
	electionTimeout       time.Duration // 200-400ms 选举的间隔时间不同 可以有效的防止选举失败 2A

	applyCh chan ApplyMsg

	// 2D中用于传入快照点
	lastIncludeIndex int
	lastIncludeTerm  int
}

type State int

type LogEntry struct {
	Term    int
	Command interface{}
}

func init() {
	// 获取日志文件句柄
	// 以 只写入文件|没有时创建|文件尾部追加 的形式打开这个文件
	// logFile, err := os.OpenFile(`./log.log`, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	logFile, err := os.OpenFile(`/dev/null`, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	// 设置存储位置
	log.SetOutput(logFile)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		DPrintf("%v encode currentTerm error: %v", rf, err)
	}

	err = e.Encode(rf.votedFor)
	if err != nil {
		DPrintf("%v encode votedFor error: %v", rf, err)
	}

	err = e.Encode(rf.logs)
	if err != nil {
		DPrintf("%v encode log error: %v", rf, err)
	}

	err = e.Encode(rf.lastIncludeIndex)
	if err != nil {
		DPrintf("%v encode lastIncludeIndex error: %v", rf, err)
	}

	err = e.Encode(rf.lastIncludeTerm)
	if err != nil {
		DPrintf("%v encode lastIncludeTerm error: %v", rf, err)
	}

	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludeIndex, lastIncludeTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		log.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}

	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	// 更新快照下标/任期
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs
	// if index > rf.commitIndex {
	// 	fmt.Printf("out of expect index > rf.commitIndex\n")
	// }
	// if index > rf.lastApplied {
	// 	fmt.Printf("out of expect index > rf.lastApplied\n")
	// }

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引(2D包含快照
	LastLogTerm  int // 竞选人最后日志条目的任期号(2D包含快照
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool // 是否投票给了该竞选人
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 由于网络分区或者是节点crash，导致的任期比接收者还小，直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		log.Printf("[投票否决] : args.Term[%v] < rf.currentTerm[%v], rf[%v] reject the candidate: %v\n",
			args.Term, rf.currentTerm,
			rf.me, args.CandidateId,
		)
		return
	}

	// 预期的结果:任期大于当前节点，进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || rf.votedFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		log.Printf("[投票否决] : rf[%v] LastLogIndex[%v] LastLogTerm[%v] votefor[%v], reject the candidate: %v LastLogIndex[%v] LastLogTerm[%v] \n",
			rf.me, rf.getLastIndex(), rf.getLastTerm(), rf.votedFor,
			args.CandidateId, args.LastLogIndex, args.LastLogTerm,
		)
		return
	} else {
		reply.VoteGranted = true
		log.Printf("[投票预成功] : rf[%v] vote candidate: %v, prev_voteFor: %v\n", rf.me, args.CandidateId, rf.votedFor)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.getVoteNum = 0
		rf.lastResetElectionTime = time.Now()
		rf.persist()
		log.Printf("[投票成功] : rf[%v] vote candidate: %v\n", rf.me, args.CandidateId)
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return index, term, isLeader
	}
	if rf.state != LEADER {
		return index, term, isLeader
	} else {
		isLeader = true
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		log.Printf("[客户端命令输入]: %v, term: %v\n", command, term)
		return index, term, isLeader
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		nowTime := time.Now()
		time.Sleep(time.Duration(getRand(int64(rf.me))) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastResetElectionTime.Before(nowTime) && rf.state != LEADER {
			if rf.state == FOLLOWER {
				rf.state = CANDIDATE
			}
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.getVoteNum = 1
			rf.persist()

			// 发起投票
			log.Printf("[发起投票] :Rf[%v] 发起投票\n", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(server int) {
					rf.mu.Lock()
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.getLastIndex(),
						LastLogTerm:  rf.getLastTerm(),
					}
					reply := RequestVoteReply{}
					rf.mu.Unlock()

					res := rf.sendRequestVote(server, &args, &reply)

					if res {
						rf.mu.Lock()
						// 判断自身是否还是竞选者，且任期不冲突
						if rf.state != CANDIDATE || args.Term < rf.currentTerm {
							rf.mu.Unlock()
							return
						}
						// 返回者的任期大于args（网络分区原因)进行返回
						if reply.Term > args.Term {
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
							}
							rf.state = FOLLOWER
							rf.votedFor = -1
							rf.getVoteNum = 0
							rf.persist()
							rf.mu.Unlock()
							return
						}
						// 返回结果正确判断是否大于一半节点同意
						if reply.VoteGranted && rf.currentTerm == args.Term {
							rf.getVoteNum++
							if rf.getVoteNum >= len(rf.peers)/2+1 {
								rf.state = LEADER
								rf.votedFor = -1
								rf.getVoteNum = 0
								rf.persist()
								// 选举成功立马添加一条空的log
								// 发送空command的方法称为 no-op 日志，解决figure8问题,帮助leader将以前Term的日志附带发送，但是这里仅仅发送了心跳
								rf.leaderAppendEntries()

								rf.nextIndex = make([]int, len(rf.peers))
								for i := 0; i < len(rf.peers); i++ {
									rf.nextIndex[i] = rf.getLastIndex() + 1
								}
								rf.matchIndex = make([]int, len(rf.peers))
								rf.matchIndex[rf.me] = rf.getLastIndex()
								rf.lastResetElectionTime = time.Now()
								rf.mu.Unlock()
								log.Printf("[投票结果] : 现在的leader为[%v]\n", rf.me)
								return
							}
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						return
					}
				}(i)
			}
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	// rf.logs = make([]LogEntry, 0)
	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = FOLLOWER
	rf.getVoteNum = 0
	rf.electionTimeout = time.Millisecond * time.Duration(ELECTION_TIMEOUT_MIN+rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN))
	rf.lastResetElectionTime = time.Now()
	rf.applyCh = applyCh

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// AppendEntriesArgs Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int        // 用于匹配日志的任期是否是合适的，是否有冲突
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	UpNextIndex int  // 如果发生conflict时reply传过来的正确的下标用于更新nextIndex[i]
}

func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			// log.Printf("[getPrevLogInfo], [%v], Term: %v, log: %v, nextIndex: %v, matchIndex: %v\n",
			// 	rf.me, rf.currentTerm, rf.logs, rf.nextIndex, rf.matchIndex,
			// )
			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			// 判断有无新的log
			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}

			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, &args, &reply)
			// for !ok {
			// 	// time.Sleep(2 * time.Millisecond)
			// 	if rf.killed() {
			// 		return
			// 	}
			// 	rf.mu.Lock()
			// 	if rf.state != LEADER {
			// 		rf.mu.Unlock()
			// 		return
			// 	}
			// 	rf.mu.Unlock()
			// 	ok = rf.sendAppendEntries(server, &args, &reply)
			// }
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER {
					return
				}
				// 可能突然加入了一个Term很高的节点，要重新选举
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.getVoteNum = 0
					rf.lastResetElectionTime = time.Now()
					rf.persist()
					return
				}

				if reply.Success {

					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 判断commit log
					for index := rf.getLastIndex(); index > rf.commitIndex && index > rf.lastIncludeIndex; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum++
								continue
							}
							if rf.matchIndex[i] >= index {
								sum++
							}
						}

						// 大于一半，且因为是从后往前，一定会大于原本commitIndex
						// 后面那个条件很重要, 因为论文figure8给出了一种情况，表明leader只能提交当前Term的log, 以前Term的log是附带提交
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}
					}
				} else {
					// 返回为冲突
					// 如果冲突不为-1，则进行更新
					if reply.UpNextIndex != -1 {
						if reply.UpNextIndex == args.PrevLogIndex+1 {
							// 这里是因为PreLogIndex太老了
							rf.nextIndex[server] = reply.UpNextIndex
						} else {
							// 每次跳过一个Term
							prevIndex := args.PrevLogIndex
							//为什么 > 1, 因为原始插入了一条空log, 所以nextIndex至少为 1
							for prevIndex > rf.lastIncludeIndex+1 && rf.restoreLogTerm(prevIndex) == args.PrevLogTerm {
								prevIndex--
							}
							rf.nextIndex[server] = prevIndex
						}
					}
				}
			}
		}(index)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[接收日志消息], [%v]从[%v]收到, Term: %v, PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v, LeaderCommit: %v\n",
		rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit,
	)
	log.Printf("[返回日志消息[%v]修改前属性], Term: %v, commitIndex: %v, logs: %v\n",
		rf.me, rf.currentTerm, rf.commitIndex, rf.logs,
	)
	defer func(re *AppendEntriesReply) {
		log.Printf("[返回日志消息], [%v]发送给[%v], Term: %v, Success: %v, UpNextIndex: %v\n",
			rf.me, args.LeaderId, re.Term, re.Success, re.UpNextIndex,
		)
	}(reply)
	defer func(r *Raft) {
		log.Printf("[返回日志消息[%v]修改后属性], Term: %v, commitIndex: %v, logs: %v\n",
			r.me, r.currentTerm, r.commitIndex, r.logs,
		)
	}(rf)

	// 根据论文AppendEntries RPC中的规则进行实现

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.state = FOLLOWER
	//这里voteFor不能设置成-1，否则会重复投票
	rf.lastResetElectionTime = time.Now()
	rf.currentTerm = args.Term
	rf.getVoteNum = 0
	rf.persist()

	// Leader给的log已经被快照了，太老了
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	// rule 2,3
	// 缺失日志或者日志Term对不上
	// if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	if args.PrevLogIndex < len(rf.logs) {
	// 		// Term对不上退一个
	// 		rf.logs = rf.logs[0:args.PrevLogIndex]
	// 	}
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	reply.UpNextIndex = args.PrevLogIndex - 1
	// 	log.Printf("[break rule2/3, [%v] reply to [%v]]\n",
	// 		rf.me, args.LeaderId,
	// 	)
	// 	return
	// }
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() // 其实并没有作用，不是-1都一样
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			// tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			// for index := args.PrevLogIndex; index > rf.lastIncludeIndex; index-- {
			// 	if rf.restoreLogTerm(index) != tempTerm {
			// 		reply.UpNextIndex = index + 1
			// 		break
			// 	}
			// }
			// lastIncludeIndex + 1 到 args.PrevLogIndex 都是同一个Term的
			// if reply.UpNextIndex == -1 {
			// 	reply.UpNextIndex = rf.lastIncludeIndex + 1
			// }
			reply.UpNextIndex = args.PrevLogIndex - 1 // 其实并没有作用，不是-1都一样
			return
		}
	}

	// rule 4
	rf.logs = append(rf.logs[0:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()

	// rule  5
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getLastIndex() {
			rf.commitIndex = rf.getLastIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
}

func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for !rf.killed() {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied++
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
		}
	}
}

type InstallSnapshotArgs struct {
	Term             int // 发送请求方的任期
	LeaderId         int // 请求方的LeaderId
	LastIncludeIndex int // 快照最后applied的日志下标
	LastIncludeTerm  int // 快照最后applied时的当前任期
	// Offset           int    // 快照中块的字节偏移量
	Data []byte // 快照区块的原始字节流数据
	// Done             bool   // 是否为最后一个块
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		// 在RPC期间, Term变化了
		if rf.state != LEADER || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.getVoteNum = 0
			rf.persist()
			rf.lastResetElectionTime = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.state = FOLLOWER
	rf.getVoteNum = 0
	rf.persist()
	rf.lastResetElectionTime = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg

}
