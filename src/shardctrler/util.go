package shardctrler

import "sort"

func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	sc.mu.Unlock()
	return ch
}

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			if !sc.ifDuplicate(op.ClientId, op.SeqId) {
				sc.mu.Lock()
				sc.seqMap[op.ClientId] = op.SeqId
				switch op.OpType {
				case "Join":
					sc.JoinFunc(op.JoinServers)
				case "Leave":
					sc.LeaveFunc(op.LeaveGids)
				case "Move":
					sc.MoveFunc(op.MoveGid, op.MoveShard)
				default:

				}
				sc.mu.Unlock()
			}
			// 将返回的ch返回waitCh
			sc.getWaitCh(index) <- op
		}
	}
}

func (sc *ShardCtrler) JoinFunc(servers map[int][]string) Err {

	// 取出最后一个config将分组加进去
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}
	newConfig := Config{len(sc.configs), lastConfig.Shards, newGroups}
	s2g := groupByGid(&newConfig)
	// fmt.Printf("s2g0: %v\n", s2g)
	for {
		source, target := getGIDWithMaximumShards(s2g), getGIDWithMinimumShards(s2g)
		// fmt.Printf("getGIDWithMinimumShards: %v\t, getGIDWithMaximumShards: %v\n", source, target)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	// fmt.Printf("s2g1: %v\n", s2g)
	// fmt.Printf("group: %v\n", newConfig.Groups)
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) LeaveFunc(gids []int) Err {

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	newConfig := Config{len(sc.configs), lastConfig.Shards, newGroups}
	s2g := groupByGid(&newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}
	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := getGIDWithMinimumShards(s2g)
			s2g[target] = append(s2g[target], shard)
		}
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) MoveFunc(gid int, shard int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{}}

	// 填充并赋值
	for shards, gids := range lastConfig.Shards {
		newConfig.Shards[shards] = gids
	}
	newConfig.Shards[shard] = gid

	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}

	sc.configs = append(sc.configs, newConfig)
	return OK
}

func groupByGid(config *Config) map[int][]int {
	shardsCount := map[int][]int{}
	for k, _ := range config.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range config.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}

func getGIDWithMinimumShards(shardsCount map[int][]int) int {
	var keys []int
	for k, _ := range shardsCount {
		keys = append(keys, k)
	}
	// 为了保证每个raft是一致的，map的遍历不是一致的！！！
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(shardsCount[gid]) < min {
			index, min = gid, len(shardsCount[gid])
		}
	}
	return index
}

func getGIDWithMaximumShards(shardsCount map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := shardsCount[0]; ok && len(shards) > 0 {
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range shardsCount {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(shardsCount[gid]) > max {
			index, max = gid, len(shardsCount[gid])
		}
	}
	return index
}
