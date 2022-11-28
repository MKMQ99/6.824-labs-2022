package shardkv

import (
	"sync/atomic"
	"time"
)

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 传入op的SeqId此次op
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {

	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {

		// 判断切片是否都收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			// One way to do this is for the server to detect that it has lost leadership,
			// by noticing that a different request has appeared at the index returned by Start()
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-timer.C:
		return ErrOverTime
	}
}

// 判断当前节点是否处于迁移期
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {

	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range KvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}
