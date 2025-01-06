package multiversion

import (
	"bytes"
	"cosmossdk.io/store/types"
	"cosmossdk.io/store/types/occ"
	occtypes "cosmossdk.io/store/types/occ"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"time"

	db "github.com/cosmos/cosmos-db"
	"os"
	"sort"
	"sync"
)

type VersionStoreIndexKeys struct {
	multiVersionValue MultiVersionValue
	txWritesetKeys    []string
	readset           ReadSet
	iterateset        Iterateset
}

var _ MultiVersionStore = (*ListStore)(nil)

type ListStore struct {
	// map that stores the key string -> MultiVersionValue mapping for accessing from a given key
	multiVersionMap map[string]any
	writeKeyList    []MultiVersionValue
	//// TODO: do we need to support iterators as well similar to how cachekv does it - yes
	//
	//txWritesetKeys *sync.Map // map of tx index -> writeset keys []string
	//txReadSets     *sync.Map // map of tx index -> readset ReadSet
	//txIterateSets  *sync.Map // map of tx index -> iterateset Iterateset
	testMap      ConcurrentMap
	testShardMap ShardedMap

	keysList []VersionStoreIndexKeys

	parentStore types.KVStore
	versionList bool
	mu          sync.Mutex
}

func NewMultiVersionListStore(parentStore types.KVStore, totalTask int) *ListStore {
	//config := bigcache.DefaultConfig(time.Minute * 100000)
	//cache, err := bigcache.NewBigCache(config)
	//if err != nil {
	//	panic(fmt.Errorf("failed to create KVStore cache: %s", err))
	//}
	versionListEnv := os.Getenv("versionList")
	versionList := true
	if versionListEnv != "" {
		versionList = false
	}

	keysList := make([]VersionStoreIndexKeys, totalTask)
	versionMap := make(map[string]any)
	return &ListStore{
		multiVersionMap: versionMap,
		parentStore:     parentStore,
		keysList:        keysList,
		versionList:     versionList,
		testMap:         *NewConcurrentMap(),
		testShardMap:    *NewShardedMap(128),
	}
}

// VersionedIndexedStore creates a new versioned index store for a given incarnation and transaction index
func (s *ListStore) VersionedIndexedStore(index int, incarnation int, abortChannel chan occ.Abort, totalTask int) *VersionIndexedStore {

	return NewVersionIndexedStore(s.parentStore, s, index, incarnation, abortChannel, totalTask)
}

// GetLatest implements MultiVersionStore.
func (s *ListStore) GetLatest(key []byte) (value MultiVersionValueItem) {
	keyString := string(key)
	mvVal, found := s.testShardMap.Get(keyString)
	// if the key doesn't exist in the overall map, return nil
	if !found {
		return nil
	}
	latestVal, found := mvVal.(MultiVersionValue).GetLatest()
	if !found {
		return nil // this is possible IF there is are writeset that are then removed for that key
	}
	return latestVal
}

func (s *ListStore) NewKey(key string, totalTask int) {
	s.testShardMap.Update(key, totalTask)
	//get, b := s.testShardMap.Get(key)
	//if !b {
	//
	//	s.testShardMap.Set(key, NewMultiVersionListItem(1))
	//}else{
	//	if len(get.(*multiVersionListItem).valueList) == 1 {
	//		s.testShardMap.Set(key,NewMultiVersionListItem(totalTask))
	//	}
	//}

	//
	////get, b := s.testMap.Get(key)
	////if !b {
	////	s.testMap.Set(key,NewMultiVersionListItem(1))
	////}else{
	////	if len(get.(*multiVersionListItem).valueList) == 1 {
	////		s.testMap.Set(key,NewMultiVersionListItem(totalTask))
	////	}
	////}
	//s.mu.Lock()
	//defer s.mu.Unlock()
	//mvVal, found := s.multiVersionMap[key]
	//if !found {
	//	mvVal := NewMultiVersionListItem(1)
	//	if value == nil {
	//		mvVal.Delete(index, incarnation)
	//	} else {
	//		mvVal.Set(index, incarnation, value)
	//	}
	//	s.multiVersionMap[key] =  mvVal
	//} else {
	//	//println("get new key find")
	//	if len(mvVal.(*multiVersionListItem).valueList) == 1 {
	//		s.multiVersionMap[key] = NewMultiVersionListItem(totalTask)
	//	}
	//}
}

//func (s *ListStore) LoadMultiVersion(key string) (value MultiVersionValue,f bool) {
//	mvVal, found := s.multiVersionMap.Get(key)
//	// if the key doesn't exist in the overall map, return nil
//	if found != nil {
//		return nil,false
//	}
//	keyIndex := binary.LittleEndian.Uint32(mvVal)
//	versionValue := s.writeKeyList[keyIndex]
//	if versionValue == nil {
//		return nil,false
//	}
//	return versionValue,true
//}

// GetLatestBeforeIndex implements MultiVersionStore.
func (s *ListStore) GetLatestBeforeIndex(index int, key []byte) (value MultiVersionValueItem) {
	keyString := string(key)
	mvVal, found := s.testShardMap.Get(keyString)
	// if the key doesn't exist in the overall map, return nil
	if !found {
		return nil
	}
	val, found := mvVal.(MultiVersionValue).GetLatestBeforeIndex(index)
	// otherwise, we may have found a value for that key, but its not written before the index passed in
	if !found {
		return nil
	}
	// found a value prior to the passed in index, return that value (could be estimate OR deleted, but it is a definitive value)
	return val
}

// GetLatestBeforeIndex implements MultiVersionStore.
func (s *ListStore) GetLatestBeforeIndexExpansion(index int, key []byte) (value MultiVersionValueItem) {
	keyString := string(key)
	mvVal, found := s.testShardMap.Get(keyString)
	// if the key doesn't exist in the overall map, return nil
	if !found {
		return nil
	}
	val, found := mvVal.(MultiVersionValue).GetLatestBeforeIndexExpansion(index)

	// otherwise, we may have found a value for that key, but its not written before the index passed in
	if !found {
		return nil
	}
	// found a value prior to the passed in index, return that value (could be estimate OR deleted, but it is a definitive value)
	return val
}

// Has implements MultiVersionStore. It checks if the key exists in the multiversion store at or before the specified index.
func (s *ListStore) Has(index int, key []byte) bool {

	keyString := string(key)
	mvVal, found := s.testShardMap.Get(keyString)
	// if the key doesn't exist in the overall map, return nil
	if !found {
		return false // this is okay because the caller of this will THEN need to access the parent store to verify that the key doesnt exist there
	}
	_, foundVal := mvVal.(MultiVersionValue).GetLatestBeforeIndex(index)
	return foundVal
}

func (s *ListStore) removeOldWriteset(index int, newWriteSet WriteSet) {
	writeset := make(map[string][]byte)
	if newWriteSet != nil {
		// if non-nil writeset passed in, we can use that to optimize removals
		writeset = newWriteSet
	}
	// if there is already a writeset existing, we should remove that fully
	keys := s.keysList[index]
	oldKeys := keys.txWritesetKeys
	if oldKeys == nil || len(oldKeys) == 0 {
		return
	}
	// we need to delete all of the keys in the writeset from the multiversion store
	for _, key := range oldKeys {
		// small optimization to check if the new writeset is going to write this key, if so, we can leave it behind
		if _, ok := writeset[key]; ok {
			// we don't need to remove this key because it will be overwritten anyways - saves the operation of removing + rebalancing underlying btree
			continue
		}
		// remove from the appropriate item if present in multiVersionMap
		mvVal, found := s.testShardMap.Get(key)
		// if the key doesn't exist in the overall map, return nil
		if !found {
			continue
		}
		mvVal.(MultiVersionValue).Remove(index)
	}
}

// SetWriteset sets a writeset for a transaction index, and also writes all of the multiversion items in the writeset to the multiversion store.
// TODO: returns a list of NEW keys added
func (s *ListStore) SetWriteset(index int, incarnation int, writeset WriteSet, totalTask int) {
	// TODO: add telemetry spans
	// remove old writeset if it exists
	s.removeOldWriteset(index, writeset)

	writeSetKeys := make([]string, 0, len(writeset))
	for key, value := range writeset {
		writeSetKeys = append(writeSetKeys, key)
		loadVal, ok := s.testShardMap.Get(key)
		if !ok {
			panic("map get nil")
		}
		mvVal := loadVal.(MultiVersionValue)
		if value == nil {
			mvVal.Delete(index, incarnation)
		} else {
			mvVal.Set(index, incarnation, value)
		}
	}
	sort.Strings(writeSetKeys) // TODO: if we're sorting here anyways, maybe we just put it into a btree instead of a slice
	s.keysList[index].txWritesetKeys = writeSetKeys
}

// InvalidateWriteset iterates over the keys for the given index and incarnation writeset and replaces with ESTIMATEs
func (s *ListStore) InvalidateWriteset(index int, incarnation int, totalTask int) {
	keys := s.keysList[index].txWritesetKeys
	if keys == nil || len(keys) == 0 {
		return
	}
	for _, key := range keys {
		// invalidate all of the writeset items - is this suboptimal? - we could potentially do concurrently if slow because locking is on an item specific level
		val, _ := s.testShardMap.Get(key)
		val.(MultiVersionValue).SetEstimate(index, incarnation)
	}
	// we leave the writeset in place because we'll need it for key removal later if/when we replace with a new writeset
}

// SetEstimatedWriteset is used to directly write estimates instead of writing a writeset and later invalidating
func (s *ListStore) SetEstimatedWriteset(index int, incarnation int, writeset WriteSet) {
	// remove old writeset if it exists
	s.removeOldWriteset(index, writeset)

	writeSetKeys := make([]string, 0, len(writeset))
	// still need to save the writeset so we can remove the elements later:
	for key := range writeset {
		writeSetKeys = append(writeSetKeys, key)

		mvVal, _ := s.testShardMap.Get(key) // init if necessary
		mvVal.(MultiVersionValue).SetEstimate(index, incarnation)
	}
	sort.Strings(writeSetKeys)
	s.keysList[index].txWritesetKeys = writeSetKeys
}

// GetAllWritesetKeys implements MultiVersionStore.
func (s *ListStore) GetAllWritesetKeys() map[int][]string {
	writesetKeys := make(map[int][]string)
	// TODO: is this safe?
	for index, keys := range s.keysList {
		writesetKeys[index] = keys.txWritesetKeys
	}
	return writesetKeys
}

func (s *ListStore) SetReadset(index int, readset ReadSet) {
	s.keysList[index].readset = readset
}

func (s *ListStore) GetReadset(index int) ReadSet {
	readsetAny := s.keysList[index].readset
	return readsetAny
}

func (s *ListStore) SetIterateset(index int, iterateset Iterateset) {
	s.keysList[index].iterateset = iterateset
}

func (s *ListStore) GetIterateset(index int) Iterateset {
	iteratesetAny := s.keysList[index].iterateset
	return iteratesetAny
}

func (s *ListStore) ClearReadset(index int) {
	s.keysList[index].readset = nil
}

func (s *ListStore) ClearIterateset(index int) {
	s.keysList[index].iterateset = nil
}

// CollectIteratorItems implements MultiVersionStore. It will return a memDB containing all of the keys present in the multiversion store within the iteration range prior to (exclusive of) the index.
func (s *ListStore) CollectIteratorItems(index int) *db.MemDB {
	sortedItems := db.NewMemDB()

	// get all writeset keys prior to index
	for i := 0; i < index; i++ {
		writesetAny := s.keysList[index].txWritesetKeys
		if writesetAny == nil || len(writesetAny) == 0 {
			continue
		}
		indexedWriteset := writesetAny
		// TODO: do we want to exclude keys out of the range or just let the iterator handle it?
		for _, key := range indexedWriteset {
			// TODO: inefficient because (logn) for each key + rebalancing? maybe theres a better way to add to a tree to reduce rebalancing overhead
			sortedItems.Set([]byte(key), []byte{})
		}
	}
	return sortedItems
}

func (s *ListStore) validateIterator(index int, tracker iterationTracker) bool {
	// collect items from multiversion store
	sortedItems := s.CollectIteratorItems(index)
	// add the iterationtracker writeset keys to the sorted items
	for key := range tracker.writeset {
		sortedItems.Set([]byte(key), []byte{})
	}
	validChannel := make(chan bool, 1)
	abortChannel := make(chan occtypes.Abort, 1)

	// listen for abort while iterating
	go func(iterationTracker iterationTracker, items *db.MemDB, returnChan chan bool, abortChan chan occtypes.Abort) {
		var parentIter types.Iterator
		expectedKeys := iterationTracker.iteratedKeys
		foundKeys := 0
		iter := s.newMVSValidationIterator(index, iterationTracker.startKey, iterationTracker.endKey, items, iterationTracker.ascending, iterationTracker.writeset, abortChan)
		if iterationTracker.ascending {
			parentIter = s.parentStore.Iterator(iterationTracker.startKey, iterationTracker.endKey)
		} else {
			parentIter = s.parentStore.ReverseIterator(iterationTracker.startKey, iterationTracker.endKey)
		}
		// create a new MVSMergeiterator
		mergeIterator := NewMVSMergeIterator(parentIter, iter, iterationTracker.ascending, NoOpHandler{})
		defer mergeIterator.Close()
		for ; mergeIterator.Valid(); mergeIterator.Next() {
			if (len(expectedKeys) - foundKeys) == 0 {
				// if we have no more expected keys, then the iterator is invalid
				returnChan <- false
				return
			}
			key := mergeIterator.Key()
			// TODO: is this ok to not delete the key since we shouldnt have duplicate keys?
			if _, ok := expectedKeys[string(key)]; !ok {
				// if key isn't found
				returnChan <- false
				return
			}
			// remove from expected keys
			foundKeys += 1
			// delete(expectedKeys, string(key))

			// if our iterator key was the early stop, then we can break
			if bytes.Equal(key, iterationTracker.earlyStopKey) {
				break
			}
		}
		// return whether we found the exact number of expected keys
		returnChan <- !((len(expectedKeys) - foundKeys) > 0)
	}(tracker, sortedItems, validChannel, abortChannel)
	select {
	case <-abortChannel:
		// if we get an abort, then we know that the iterator is invalid
		return false
	case valid := <-validChannel:
		return valid
	}
}

func (s *ListStore) checkIteratorAtIndex(index int) bool {
	valid := true
	iterateSetAny := s.keysList[index].iterateset
	if iterateSetAny == nil || len(iterateSetAny) == 0 {
		return true
	}
	iterateset := iterateSetAny
	for _, iterationTracker := range iterateset {
		// TODO: if the value of the key is nil maybe we need to exclude it? - actually it should
		iteratorValid := s.validateIterator(index, *iterationTracker)
		valid = valid && iteratorValid
	}
	return valid
}

func (s *ListStore) checkReadsetAtIndex(index int) (bool, []int) {
	conflictSet := make(map[int]struct{})
	valid := true

	readSetAny := s.keysList[index].readset
	if readSetAny == nil || len(readSetAny) == 0 {
		return true, []int{}
	}
	readset := readSetAny
	//iterate over readset and check if the value is the same as the latest value relateive to txIndex in the multiversion store
	for key, valueArr := range readset {
		if len(valueArr) != 1 {
			valid = false
			continue
		}
		value := valueArr[0]
		// get the latest value from the multiversion store
		hexKey := hex.EncodeToString([]byte(key))
		latestValue := s.GetLatestBeforeIndex(index, []byte(key))
		if latestValue == nil {
			// this is possible if we previously read a value from a transaction write that was later reverted, so this time we read from parent store
			parentVal := s.parentStore.Get([]byte(key))
			if !bytes.Equal(parentVal, value) {
				fmt.Println("1key:", hexKey, "parentVal:", hex.EncodeToString(parentVal), "value:", hex.EncodeToString(value))
				valid = false
			}
		} else {
			// if estimate, mark as conflict index - but don't invalidate
			if latestValue.IsEstimate() {
				conflictSet[latestValue.Index()] = struct{}{}
			} else if latestValue.IsDeleted() {
				if value != nil {
					// conflict
					// TODO: would we want to return early?
					conflictSet[latestValue.Index()] = struct{}{}
					fmt.Println("2key:", hexKey, "latestValue:", latestValue, "value:", value)
					valid = false
				}
			} else if !bytes.Equal(latestValue.Value(), value) {
				conflictSet[latestValue.Index()] = struct{}{}
				fmt.Println("3key:", hexKey, "latestValue:", hex.EncodeToString(latestValue.Value()), "index", latestValue.Index(), "value:", hex.EncodeToString(value))
				valid = false
			}
		}
	}

	conflictIndices := make([]int, 0, len(conflictSet))
	for index := range conflictSet {
		conflictIndices = append(conflictIndices, index)
	}

	sort.Ints(conflictIndices)

	return valid, conflictIndices
}

// TODO: do we want to return bool + []int where bool indicates whether it was valid and then []int indicates only ones for which we need to wait due to estimates? - yes i think so?
func (s *ListStore) ValidateTransactionState(index int) (bool, []int) {
	// defer telemetry.MeasureSince(time.Now(), "store", "mvs", "validate")

	// TODO: can we parallelize for all iterators?
	iteratorValid := s.checkIteratorAtIndex(index)

	readsetValid, conflictIndices := s.checkReadsetAtIndex(index)

	return iteratorValid && readsetValid, conflictIndices
}

func (s *ListStore) WriteLatestToStore() {

	now := time.Now()
	total := 0
	for _, shard := range s.testShardMap.shards {
		for key, val := range shard {
			if nil == val {
				continue
			}
			mvValue, found := val.(MultiVersionValue).GetLatestNonEstimate()

			if !found {
				// this means that at some point, there was an estimate, but we have since removed it so there isn't anything writeable at the key, so we can skip
				continue
			}
			// we shouldn't have any ESTIMATE values when performing the write, because we read the latest non-estimate values only
			if mvValue.IsEstimate() {
				panic("should not have any estimate values when writing to parent store")
			}
			// if the value is deleted, then delete it from the parent store
			var store types.KVStore
			if hex.EncodeToString([]byte(key)) == "02" {
				store = s.parentStore
			} else {
				store = s.parentStore.GetParent()
			}
			total++
			if mvValue.IsDeleted() {
				// We use []byte(key) instead of conv.UnsafeStrToBytes because we cannot
				// be sure if the underlying store might do a save with the byteslice or
				// not. Once we get confirmation that .Delete is guaranteed not to
				// save the byteslice, then we can assume only a read-only copy is sufficient.
				store.Delete([]byte(key))
				continue
			}
			if mvValue.Value() != nil {
				store.Set([]byte(key), mvValue.Value())
				//count++
			}
		}
	}
	println("list store =========", "total", total, "t", time.Since(now).String())

}

type ConcurrentMap struct {
	data   map[string]interface{}
	locks  map[string]*sync.Mutex
	global sync.Mutex
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		data:  make(map[string]interface{}),
		locks: make(map[string]*sync.Mutex),
	}
}

func (cm *ConcurrentMap) getKeyLock(key string) *sync.Mutex {
	cm.global.Lock()
	defer cm.global.Unlock()
	if lock, exists := cm.locks[key]; exists {
		return lock
	}
	lock := &sync.Mutex{}
	cm.locks[key] = lock
	return lock
}

func (cm *ConcurrentMap) Set(key string, value interface{}) {
	keyLock := cm.getKeyLock(key)
	keyLock.Lock()
	defer keyLock.Unlock()
	cm.data[key] = value
}

func (cm *ConcurrentMap) Get(key string) (interface{}, bool) {
	keyLock := cm.getKeyLock(key)
	keyLock.Lock()
	defer keyLock.Unlock()
	value, exists := cm.data[key]
	return value, exists
}

type ShardedMap struct {
	shards     []map[string]interface{}
	locks      []sync.RWMutex
	shardCount int
}

func NewShardedMap(shardCount int) *ShardedMap {
	shards := make([]map[string]interface{}, shardCount)
	locks := make([]sync.RWMutex, shardCount)
	for i := range shards {
		shards[i] = make(map[string]interface{})
	}
	return &ShardedMap{shards: shards, locks: locks, shardCount: shardCount}
}

func (sm *ShardedMap) getShard(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)) % uint32(sm.shardCount))
}

func (sm *ShardedMap) Set(key string, value interface{}) {
	shardIndex := sm.getShard(key)
	sm.locks[shardIndex].Lock()
	defer sm.locks[shardIndex].Unlock()
	sm.shards[shardIndex][key] = value
}

func (sm *ShardedMap) Update(key string, totalTask int) {
	shardIndex := sm.getShard(key)
	sm.locks[shardIndex].Lock()
	defer sm.locks[shardIndex].Unlock()
	value, exists := sm.shards[shardIndex][key]
	if !exists {
		sm.shards[shardIndex][key] = NewMultiVersionListItem(1)
	} else {
		if len(value.(*multiVersionListItem).valueList) == 1 {
			sm.shards[shardIndex][key] = NewMultiVersionListItem(totalTask)
		}
	}
}

func (sm *ShardedMap) Get(key string) (interface{}, bool) {
	shardIndex := sm.getShard(key)
	sm.locks[shardIndex].RLock()
	defer sm.locks[shardIndex].RUnlock()
	value, exists := sm.shards[shardIndex][key]
	return value, exists
}

func (sm *ShardedMap) SetIf(key string, value interface{}, ifFun func(interface{}, bool) bool) interface{} {
	shardIndex := sm.getShard(key)
	sm.locks[shardIndex].Lock()
	defer sm.locks[shardIndex].Unlock()
	old, exists := sm.shards[shardIndex][key]
	if ifFun(old, exists) {
		sm.shards[shardIndex][key] = value
		return value
	}
	return old
}

func (sm *ShardedMap) Delete(key string) bool {
	shardIndex := sm.getShard(key)
	sm.locks[shardIndex].Lock()
	defer sm.locks[shardIndex].Unlock()
	_, exists := sm.shards[shardIndex][key]
	if exists {
		delete(sm.shards[shardIndex], key)
	}
	return exists
}
