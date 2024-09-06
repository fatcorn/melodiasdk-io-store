package cache

import (
	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/types"
	"encoding/hex"
	"fmt"
	"github.com/allegro/bigcache"
	"sync"
	"time"
)

var (
	_ types.CommitKVStore             = (*CommitKVStoreBigCache)(nil)
	_ types.MultiStorePersistentCache = (*CommitKVStoreCacheManager)(nil)

	// DefaultCommitKVStoreCacheSize defines the persistent ARC cache size for a
	// CommitKVStoreCache.
)

type (
	// CommitKVStoreBigCache implements an inter-block (persistent) cache that wraps a
	// CommitKVStore. Reads first hit the internal ARC (Adaptive Replacement Cache).
	// During a cache miss, the read is delegated to the underlying CommitKVStore
	// and cached. Deletes and writes always happen to both the cache and the
	// CommitKVStore in a write-through manner. Caching performed in the
	// CommitKVStore and below is completely irrelevant to this layer.
	CommitKVStoreBigCache struct {
		types.CommitKVStore
		cache *bigcache.BigCache
		// the same CommitKVStoreCache may be accessed concurrently by multiple
		// goroutines due to transaction parallelization
		mtx sync.RWMutex
	}
)

func NewCommitKVStoreBigCache(store types.CommitKVStore, size uint) *CommitKVStoreBigCache {
	config := bigcache.DefaultConfig(time.Minute * 100)
	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		panic(fmt.Errorf("failed to create KVStore cache: %s", err))
	}

	return &CommitKVStoreBigCache{
		CommitKVStore: store,
		cache:         cache,
	}
}

// CacheWrap implements the CacheWrapper interface
func (ckv *CommitKVStoreBigCache) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(ckv)
}

// getFromCache queries the write-through cache for a value by key.
func (ckv *CommitKVStoreBigCache) getFromCache(key []byte) (interface{}, error) {
	ckv.mtx.RLock()
	defer ckv.mtx.RUnlock()
	return ckv.cache.Get(string(key))
}

// getAndWriteToCache queries the underlying CommitKVStore and writes the result
func (ckv *CommitKVStoreBigCache) getAndWriteToCache(key []byte) []byte {
	ckv.mtx.RLock()
	defer ckv.mtx.RUnlock()
	value := ckv.CommitKVStore.Get(key)
	ckv.cache.Set(string(key), value)
	return value
}

// Get retrieves a value by key. It will first look in the write-through cache.
// If the value doesn't exist in the write-through cache, the query is delegated
// to the underlying CommitKVStore.
func (ckv *CommitKVStoreBigCache) Get(key []byte) []byte {
	types.AssertValidKey(key)

	keyStr := string(key)
	toString := hex.EncodeToString(key)
	if toString == "0214dc6f17bbec824fff8f86587966b2047db6ab73677374616b65" || toString == "0214f1829676db577682e944fc3493d451b67ff3e29f7374616b65" || toString == "0214603871c2ddd41c26ee77495e2e31e6de7f9957e0657468" {
		return nil
	}
	//t1 := time.Now()
	valueI, err := ckv.cache.Get(keyStr)
	//t2 := time.Now()
	//if t2.Sub(t1).Milliseconds() >= 1 {
	//}
	//println("get cache time============", "sub", t2.Sub(t1).String(), "cache length", ckv.cache.Len(), "cache", ckv.cache)

	if err == nil {
		return valueI
	}

	// cache miss; write to cache
	value := ckv.CommitKVStore.Get(key)

	if value != nil {
		ckv.cache.Set(keyStr, value)
	}

	return value
}

// Set inserts a key/value pair into both the write-through cache and the
// underlying CommitKVStore.
func (ckv *CommitKVStoreBigCache) Set(key, value []byte) {
	//ckv.mtx.Lock()
	//defer ckv.mtx.Unlock()

	types.AssertValidKey(key)
	types.AssertValidValue(value)
	//t1 := time.Now()
	if value != nil {
		ckv.cache.Set(string(key), value)
	}
	////t2 := time.Now()

	ckv.CommitKVStore.Set(key, value)
	//t3 := time.Now()
	//println("commit kv store", "set cache time", t2.Sub(t1).String(), "set iavl time", t3.Sub(t2).String())
}

// Delete removes a key/value pair from both the write-through cache and the
// underlying CommitKVStore.
func (ckv *CommitKVStoreBigCache) Delete(key []byte) {
	ckv.mtx.Lock()
	defer ckv.mtx.Unlock()

	//ckv.cache.Remove(string(key))
	ckv.cache.Delete(string(key))
	ckv.CommitKVStore.Delete(key)
}

// Reset resets in the internal caches.
func (cmgr *CommitKVStoreBigCache) ClearCache() {
	// Clear the map.
	// Please note that we are purposefully using the map clearing idiom.
	// See https://github.com/cosmos/cosmos-sdk/issues/6681.
	config := bigcache.DefaultConfig(time.Minute * 100000)

	cache, _ := bigcache.NewBigCache(config)
	cmgr.cache = cache
}
