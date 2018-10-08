package def

import (
"sync"
)

const (
	defaultShardCount uint8 = 32
)

type shardMap struct {
	shardCount uint8
	shards     []*sync.Map
}

// Create a new SyncMap with given shard count.
// NOTE: shard count must be power of 2, default shard count will be used otherwise.
func NewWithShard(shardCount uint8) *shardMap {
	if !isPowerOfTwo(shardCount) {
		shardCount = defaultShardCount
	}
	m := new(shardMap)
	m.shardCount = shardCount
	m.shards = make([]*sync.Map, m.shardCount)
	for i, _ := range m.shards {
		m.shards[i] = &sync.Map{}
	}
	return m
}

// Find the specific shard with the given key
func (m *shardMap) locate(key string) *sync.Map {
	return m.shards[bkdrHash(key)&uint32((m.shardCount-1))]
}

// Retrieves a value
func (m *shardMap) Get(key string) (value interface{}, ok bool) {
	shard := m.locate(key)
	value, ok = shard.Load(key)
	return
}

// Sets value with the given key
func (m *shardMap) Set(key string, value interface{}) {
	shard := m.locate(key)
	shard.Store(key, value)
}

// Removes an item
func (m *shardMap) Delete(key string) {
	shard := m.locate(key)
	shard.Delete(key)
}

const seed uint32 = 131 // 31 131 1313 13131 131313 etc..

func bkdrHash(str string) uint32 {
	var h uint32

	for _, c := range str {
		h = h*seed + uint32(c)
	}

	return h
}

func isPowerOfTwo(x uint8) bool {
	return x != 0 && (x&(x-1) == 0)
}
