package shardctrler

import "sort"

func (sc *ShardCtrler) JoinOperation(servers map[int][]string) {
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	prevConfig := sc.configs[len(sc.configs)-1]
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = make([]string, 0)
		newConfig.Groups[k] = append(newConfig.Groups[k], v...)
	}
	for k, v := range servers {
		newConfig.Groups[k] = make([]string, 0)
		newConfig.Groups[k] = append(newConfig.Groups[k], v...)
	}
	sc.Balance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) LeaveOperation(gids []int) {
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	prevConfig := sc.configs[len(sc.configs)-1]
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = make([]string, 0)
		newConfig.Groups[k] = append(newConfig.Groups[k], v...)
	}
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
	}
	sc.Balance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) MoveOperation(shard, gid int) {
	prevConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: prevConfig.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = make([]string, 0)
		newConfig.Groups[k] = append(newConfig.Groups[k], v...)
	}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Queryperation(num int) Config {
	if num == -1 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) Balance(c *Config) {
	// c中已经准备好 Groups
	prevConfig := sc.configs[len(sc.configs)-1]
	prevShards := prevConfig.Shards
	left := make([]int, 0)       //等待分配的 shards
	count := make(map[int][]int) //gid -> 分配的shards
	groupSize := len(c.Groups)
	if groupSize == 0 {
		groupSize = 1
	}
	avg := NShards / groupSize
	overCnt := NShards % groupSize
	for gid := range c.Groups {
		if _, ok := count[gid]; !ok {
			count[gid] = make([]int, 0)
		}
	}
	for shard, gid := range prevShards {
		if _, ok := c.Groups[gid]; !ok || gid == 0 {
			left = append(left, shard)
			continue
		}
		if len(count[gid]) < avg {
			count[gid] = append(count[gid], shard)
		} else if len(count[gid]) == avg && overCnt > 0 {
			overCnt--
			count[gid] = append(count[gid], shard)
		} else {
			left = append(left, shard)
		}
	}

	keys := make([]int, 0)
	for key := range count {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		if len(count[gid]) < avg {
			length := avg - len(count[gid])
			count[gid] = append(count[gid], left[:length]...)
			left = left[length:]
		}
	}

	for gid, shards := range count {
		for _, shard := range shards {
			c.Shards[shard] = gid
		}
	}

}
