package kvraft

import "log"

const (
	REPLY_TIMEOUT       = 400
	CHANGELEADERPERIODS = 20
)

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		msg := <-kv.applyCh
		if msg.CommandValid {

			if kv.lastIncludeIndex >= msg.CommandIndex {
				return
			}

			index := msg.CommandIndex
			op := msg.Command.(Op)
			if !kv.ifDuplicate(op.ClientId, op.SeqId) {
				kv.mu.Lock()
				log.Printf("[log], raftID : %v\nkvPersist: %v\n", kv.rf.GetMe(), kv.kvPersist)
				switch op.OpType {
				case "Put":
					kv.kvPersist[op.Key] = op.Value
				case "Append":
					kv.kvPersist[op.Key] += op.Value
				}
				kv.seqMap[op.ClientId] = op.SeqId
				kv.mu.Unlock()
			}

			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				snapshot := kv.PersistSnapShot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}

			// 将返回的ch返回waitCh
			kv.getWaitCh(index) <- op
		}
		if msg.SnapshotValid {
			kv.mu.Lock()
			// 判断此时有没有竞争
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				// 读取快照的数据
				kv.DecodeSnapShot(msg.Snapshot)
				kv.lastIncludeIndex = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}
