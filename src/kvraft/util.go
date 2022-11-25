package kvraft

const (
	REPLY_TIMEOUT       = 400
	CHANGELEADERPERIODS = 20
)

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			// commited command
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)

				kv.mu.Lock()
				// if outdated, ignore
				if applyMsg.CommandIndex <= kv.lastIncludeIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastIncludeIndex = applyMsg.CommandIndex

				var reply *CommandReply
				// check for duplicates before apply to state machine
				if op.OpType != "Get" && kv.ifDuplicate(op.ClientId, op.SeqId) {
					reply = kv.sessions[op.ClientId].LastResponse
				} else {
					reply = kv.applyCommand(op)
					// DPrintf("Server %d applied command %+v\n", kv.me, command)
					if op.OpType != "Get" {
						kv.sessions[op.ClientId] = ClientRecord{op.SeqId, reply}
					}
				}

				// after applying command, compare if raft is oversized
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					DPrintf("Server %d takes a snapshot till index %d\n", kv.me, applyMsg.CommandIndex)
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
				}

				// check the same term and leadership before reply
				if currentTerm, isLeader := kv.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
					ch := kv.getWaitCh(applyMsg.CommandIndex)
					ch <- reply
				}
				kv.mu.Unlock()
			} else { // committed snapshot
				kv.mu.Lock()
				if kv.lastIncludeIndex < applyMsg.SnapshotIndex {
					DPrintf("Server %d receives a snapshot till index %d\n", kv.me, applyMsg.SnapshotIndex)
					kv.DecodeSnapShot(applyMsg.Snapshot)
					// server receiving snapshot must be a follower/crashed leader so no need to reply
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyMsgHandlerLoop() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {

				kv.mu.Lock()

				if kv.lastIncludeIndex >= msg.CommandIndex {
					kv.mu.Unlock()
					continue
				}

				index := msg.CommandIndex
				op := msg.Command.(Op)

				var reply *CommandReply

				if op.OpType != "Get" && kv.ifDuplicate(op.ClientId, op.SeqId) {
					reply = kv.sessions[op.ClientId].LastResponse
				} else {
					reply = kv.applyCommand(op)
					if op.OpType != "Get" {
						kv.sessions[op.ClientId] = ClientRecord{op.SeqId, reply}
					}
				}

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				if currentTerm, isLeader := kv.rf.GetState(); currentTerm == msg.CommandTerm && isLeader {
					// 将返回的ch返回waitCh
					kv.getWaitCh(index) <- reply
				}
				kv.mu.Unlock()
			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.lastIncludeIndex < msg.SnapshotIndex {
					// 读取快照的数据
					kv.DecodeSnapShot(msg.Snapshot)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	lastSeq, exist := kv.sessions[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeq.RequestID
}

func (kv *KVServer) applyCommand(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}
	if op.OpType == "Get" {
		val, ok := kv.kvPersist[op.Key]
		if ok {
			reply.Value = val
		} // else can reply empty string for no-key
	} else if op.OpType == "Put" {
		kv.kvPersist[op.Key] = op.Value
	} else {
		kv.kvPersist[op.Key] += op.Value
	}
	return reply
}

func (kv *KVServer) killWaitCh(index int) {
	ch, ok := kv.waitChMap[index]
	if ok {
		close(ch)
		delete(kv.waitChMap, index)
	}
}

func (kv *KVServer) getWaitCh(index int) chan *CommandReply {
	ch, ok := kv.waitChMap[index]
	if !ok {
		ch := make(chan *CommandReply, 1)
		kv.waitChMap[index] = ch
		return ch
	}
	return ch
}
