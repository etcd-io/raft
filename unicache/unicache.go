package unicache

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

const cachedFieldNumber = 1

// Precomputed protobuf tags for fast checking: tag = (field_number << 3) | wire_type
const (
	cachedFieldBytesTag  = byte((cachedFieldNumber << 3) | int(protowire.BytesType))  // 0x0A for field 1
	cachedFieldVarintTag = byte((cachedFieldNumber << 3) | int(protowire.VarintType)) // 0x08 for field 1
)

const maxCacheSize = 1000

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache(maxCommit *uint64, minCacheVersion func() uint64, capacity int) UniCache
	EncodeData(data []byte, currCacheIdx uint64) ([]byte, uint32)
	DecodeEntry(entry pb.Entry) (pb.Entry, bool)
	SafeEncode(data []byte, appendIdx uint64, encodedID uint32) ([]byte, []byte)
	BatchSafeEncode(entries []pb.Entry) (fullData [][]byte, logData [][]byte)
	GetNextId() uint32
	PrintCache()
	UpdateCache(entry pb.Entry) (pb.Entry, bool)
	BatchUpdateCache(entries []pb.Entry) ([]pb.Entry, bool)
	PurgeEvicted(appendCommitGap uint64)
	CacheHits() uint64
	ResetCacheHits() uint64
	GetMinCacheIdx(currMinIdx uint64) uint64
}

type cacheEntry struct {
	id       uint32
	key      []byte
	lastIdx  uint64
	addedIdx uint64
}

type uniCache struct {
	mu sync.RWMutex

	cache        map[uint32]*cacheEntry
	reverseCache map[string]uint32
	nextID       uint32
	capacity     int

	lruList *list.List
	lruMap  map[uint32]*list.Element

	evicted    map[uint32]*list.Element
	evictOrder *list.List

	maxCommit       *uint64
	minCacheVersion func() uint64

	cachehits    uint64
	lastInFlight uint64
}

// NewUniCache constructs a UniCache with simple LRU caching.
func NewUniCache(maxCommit *uint64, minCacheVersion func() uint64, capacity int) UniCache {
	return &uniCache{
		cache:        make(map[uint32]*cacheEntry),
		reverseCache: make(map[string]uint32),

		lruList: list.New(),
		lruMap:  make(map[uint32]*list.Element),

		nextID:   1,
		capacity: capacity,

		evicted:    make(map[uint32]*list.Element),
		evictOrder: list.New(),

		maxCommit:       maxCommit,
		minCacheVersion: minCacheVersion,

		cachehits:    uint64(0),
		lastInFlight: math.MaxUint64,
	}
}

func (uc *uniCache) CacheHits() uint64 {
	return atomic.LoadUint64(&uc.cachehits)
}

func (uc *uniCache) ResetCacheHits() uint64 {
	atomic.StoreUint64(&uc.cachehits, 0)
	return atomic.LoadUint64(&uc.cachehits)
}

// NewUniCache implements the UniCache interface.
func (uc *uniCache) NewUniCache(maxCommit *uint64, minCacheVersion func() uint64, capacity int) UniCache {
	return NewUniCache(maxCommit, minCacheVersion, capacity)
}

func (uc *uniCache) GetMinCacheIdx(currMinIdx uint64) uint64 {
	if uc.lastInFlight == math.MaxUint64 {
		return currMinIdx
	}
	return uc.lastInFlight
}

func (uc *uniCache) updateLRU(id uint32) {
	if elem, ok := uc.lruMap[id]; ok {
		uc.lruList.MoveToFront(elem)
	}

}

func (uc *uniCache) addToLRU(cacheEntry *cacheEntry) {
	elem := uc.lruList.PushFront(cacheEntry)
	uc.lruMap[cacheEntry.id] = elem

	uc.evictLRU(cacheEntry.lastIdx)
}

func (uc *uniCache) evictLRU(currIdx uint64) {
	if len(uc.cache) <= uc.capacity {
		return
	}

	elem := uc.lruList.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(*cacheEntry)

	if currIdx-entry.lastIdx <= uint64(uc.capacity) {
		return
	}

	//fmt.Printf("[evictLRU] index=%d evicting ID=%d lenCache:%d, lastIdx=%d capacity=%d len evicted=%d\n", currIdx, entry.id, len(uc.cache), entry.lastIdx, uc.capacity, len(uc.evicted))

	minCommit := int(uc.minCacheVersion())

	if minCommit == 0 {
		delete(uc.cache, entry.id)
		delete(uc.reverseCache, string(entry.key))
		delete(uc.lruMap, entry.id)
		uc.lruList.Remove(elem)
		return
	}

	evictedElem := uc.evictOrder.PushBack(entry)
	uc.evicted[entry.id] = evictedElem

	delete(uc.cache, entry.id)
	delete(uc.reverseCache, string(entry.key))
	delete(uc.lruMap, entry.id)
	uc.lruList.Remove(elem)
}

func (uc *uniCache) PurgeEvicted(currIdx uint64) {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	// Keep evicted cache large: 50*capacity (~100,000 entries)
	maxEvicted := uc.capacity * 150
	for len(uc.evicted) > maxEvicted {
		front := uc.evictOrder.Front()
		if front == nil {
			break
		}
		e := front.Value.(*cacheEntry)
		uc.evictOrder.Remove(front)
		delete(uc.evicted, e.id)
	}
}

func (uc *uniCache) GetNextId() uint32 {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	return uc.nextID
}

func (uc *uniCache) PrintCache() {
	uc.mu.RLock()
	fmt.Println("len cache: ", len(uc.cache), "len evicted", len(uc.evicted), "nextId:", uc.nextID)
	uc.mu.RUnlock()
}

func (uc *uniCache) SafeEncode(data []byte, appendIdx uint64, encodedID uint32) ([]byte, []byte) {
	if len(data) == 0 || encodedID == 0 {
		return data, nil
	}

	uc.mu.RLock()
	defer uc.mu.RUnlock()

	elem, ok := uc.cache[encodedID]

	if ok {
		if appendIdx-elem.lastIdx <= uint64(uc.capacity) && uc.minCacheVersion() >= elem.addedIdx {
			atomic.AddUint64(&uc.cachehits, 1)
			//fmt.Printf("[SafeEncode] index=%d cachehits=%d appendIdx=%d lastIdx=%d minCachedIdx=%d\n", appendIdx, uc.cachehits, appendIdx, elem.lastIdx, uc.minCacheVersion())

			fullData, err := ReplaceProtoField(data, cachedFieldNumber, elem.key, protowire.BytesType)
			if err == nil {
				return data, fullData
			}
		}
		//fmt.Printf("[SafeEncode] index=%d eviction risk, restoring full for ID=%d\n", appendIdx, encodedID)
		newData, err := ReplaceProtoField(data, cachedFieldNumber, elem.key, protowire.BytesType)
		if err == nil {
			//fmt.Printf("[SafeEncode] index=%d successfully restored ID=%d\n", appendIdx, encodedID)
			return newData, newData
		}
	}
	// check evicted cache
	if evElem, ok := uc.evicted[encodedID]; ok {
		ev := evElem.Value.(*cacheEntry)
		newData, err := ReplaceProtoField(data, cachedFieldNumber, ev.key, protowire.BytesType)
		if err == nil {
			//fmt.Printf("[SafeEncode] index=%d restored from evicted ID=%d keyHash=%x\n", appendIdx, encodedID, sha256.Sum256(ev.key))
			return newData, newData
		}
		//fmt.Println("[SafeEncode] evicted restore failed:", err)
	}
	fmt.Printf("[SafeEncode] index=%d didnt find data for ID=%d, capacity=%d, cache size=%d, evicted size=%d, nextId=%d\n",
		appendIdx, encodedID, uc.capacity, len(uc.cache), len(uc.evicted), uc.nextID)
	return data, nil
}

func (uc *uniCache) BatchSafeEncode(entries []pb.Entry) (fullData [][]byte, logData [][]byte) {
	fullData = make([][]byte, len(entries))
	logData = make([][]byte, len(entries))

	uc.mu.RLock()
	minVer := uc.minCacheVersion()
	capLimit := uint64(uc.capacity)

	keys := make([][]byte, len(entries))
	isSafeHit := make([]bool, len(entries))
	var hits uint64

	for i := range entries {
		id := entries[i].EncodedID
		if len(entries[i].Data) == 0 || id == 0 {
			continue
		}

		var entry *cacheEntry
		if e, ok := uc.cache[id]; ok {
			entry = e
			// Logic: Is it safe to send the tiny ID to followers?
			if entries[i].Index-e.lastIdx <= capLimit && minVer >= e.addedIdx {
				isSafeHit[i] = true
				e.lastIdx = entries[i].Index
				hits++
			}
		} else if ev, ok := uc.evicted[id]; ok {
			entry = ev.Value.(*cacheEntry)
			// Evicted entries are NEVER safe hits; they MUST be restored for followers
		}

		if entry != nil {
			keys[i] = entry.key
		}
	}
	uc.mu.RUnlock()

	if hits > 0 {
		atomic.AddUint64(&uc.cachehits, hits)
	}

	// 2. Transform OUTSIDE the lock
	for i := range keys {
		if keys[i] == nil {
			continue
		}

		newData, err := ReplaceProtoField(entries[i].Data, cachedFieldNumber, keys[i], protowire.BytesType)
		if err == nil {
			fullData[i] = newData
			// If it WASN'T a safe hit, the log needs the full data too
			if !isSafeHit[i] {
				logData[i] = newData
			}
		}
	}

	return fullData, logData
}

func (uc *uniCache) EncodeData(data []byte, currCacheIdx uint64) ([]byte, uint32) {
	if len(data) == 0 {
		return data, 0
	}

	// Parse protobuf OUTSIDE the lock
	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	if err != nil {
		return data, 0
	}
	keyStr := string(keyBytes)

	// Only lock for the map lookups (fast)
	uc.mu.RLock()
	id, ok := uc.reverseCache[keyStr]
	if !ok {
		uc.mu.RUnlock()
		return data, 0
	}
	if _, exists := uc.cache[id]; !exists {
		uc.mu.RUnlock()
		return data, 0
	}

	// Safety check: ID must be in active cache range
	// Only encode if id >= nextID - capacity (entry is in active cache)
	// Leader will have entry in cache or evicted (safety net)
	if uc.nextID > uint32(uc.capacity) {
		minActiveID := uc.nextID - uint32(uc.capacity/2)
		if id < minActiveID {
			uc.mu.RUnlock()
			return data, 0
		}
	}
	uc.mu.RUnlock()

	// Encoding outside the lock
	encodedID := protowire.AppendVarint(nil, uint64(id))
	newData, err := ReplaceProtoField(data, cachedFieldNumber, encodedID, protowire.VarintType)
	if err == nil {
		atomic.StoreUint64(&uc.lastInFlight, currCacheIdx)
		return newData, id
	}
	return data, 0
}

// DecodeEntry restores original key bytes or caches first-seen keys.
func (uc *uniCache) DecodeEntry(entry pb.Entry) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	// Super-fast check: if first byte matches BytesType tag, data is NOT encoded
	if entry.Data[0] == cachedFieldBytesTag {
		return entry, true
	}

	// Only parse protobuf if we might need to decode (first byte is 0x08 = VarintType)
	keyField, wireType, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		return entry, false
	}

	// Should not reach here at 0% hit rate, but keep as safety
	if wireType == protowire.BytesType {
		return entry, true
	}

	if wireType == protowire.VarintType {
		id, n := protowire.ConsumeVarint(keyField)
		if n <= 0 {
			return entry, false
		}

		// Only lock for cache lookup
		uc.mu.RLock()
		elem, ok := uc.cache[uint32(id)]
		uc.mu.RUnlock()

		if !ok {
			fmt.Println("[decode cache] not in cache: ", id, "index", entry.Index, "type ", entry.Type)
			return entry, false
		}

		// ReplaceProtoField outside the lock
		newData, err := ReplaceProtoField(entry.Data, cachedFieldNumber, elem.key, protowire.BytesType)
		if err != nil {
			return entry, false
		}
		entry.Data = newData
		return entry, true
	}
	return entry, true
}

func (uc *uniCache) UpdateCache(entry pb.Entry) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	// If entry is encoded (VarintType), decode it first
	// This happens on the leader which doesn't go through DecodeEntry
	if entry.Data[0] == cachedFieldVarintTag {
		decoded, ok := uc.DecodeEntry(entry)
		if !ok {
			return entry, false
		}
		entry = decoded
	}

	// Fast path: if first byte is BytesType tag, we know the structure
	if entry.Data[0] != cachedFieldBytesTag {
		return entry, false
	}

	// Parse protobuf OUTSIDE the lock
	keyField, wireType, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		return entry, false
	}

	if wireType != protowire.BytesType {
		return entry, false
	}

	keyStr := string(keyField)

	// First, try read lock to check if key exists (common case for updates)
	uc.mu.RLock()
	id, exists := uc.reverseCache[keyStr]
	if exists {
		ent := uc.cache[id]
		uc.mu.RUnlock()

		// Key exists - only need write lock for brief update
		uc.mu.Lock()
		if ent.lastIdx < entry.Index {
			ent.lastIdx = entry.Index
			uc.updateLRU(id)
			if uc.lastInFlight <= entry.Index {
				atomic.StoreUint64(&uc.lastInFlight, math.MaxUint64)
			}
		}
		uc.mu.Unlock()
		return entry, true
	}
	uc.mu.RUnlock()

	// Key doesn't exist - need write lock to add
	// Pre-allocate struct outside lock
	newElem := &cacheEntry{
		key:      keyField,
		lastIdx:  entry.Index,
		addedIdx: entry.Index,
	}

	uc.mu.Lock()
	// Double-check in case another goroutine added it
	if id, exists := uc.reverseCache[keyStr]; exists {
		ent := uc.cache[id]
		if ent.lastIdx < entry.Index {
			ent.lastIdx = entry.Index
			uc.updateLRU(id)
		}
		uc.mu.Unlock()
		return entry, true
	}

	// Add new entry
	newID := uc.nextID
	uc.nextID++
	newElem.id = newID
	uc.cache[newID] = newElem
	uc.reverseCache[keyStr] = newID
	uc.addToLRU(newElem)
	uc.mu.Unlock()

	return entry, true
}

func (uc *uniCache) BatchUpdateCache(entries []pb.Entry) ([]pb.Entry, bool) {
	// 1. PRE-PROCESSING (Outside the lock)
	// We store the keys we extracted so we don't re-parse them inside the lock.
	type preparedData struct {
		entry    pb.Entry
		keyStr   string
		keyField []byte
	}
	prepared := make([]preparedData, 0, len(entries))

	for _, entry := range entries {
		// Empty data is a no-op, but valid
		if len(entry.Data) == 0 || entry.Type != pb.EntryNormal {
			prepared = append(prepared, preparedData{entry: entry})
			continue
		}

		currentEntry := entry
		// Handle Varint encoding
		if entry.Data[0] == cachedFieldVarintTag {
			decoded, ok := uc.DecodeEntry(entry)
			if !ok {
				return nil, false
			}
			currentEntry = decoded
		}

		// Validation checks
		if currentEntry.Data[0] != cachedFieldBytesTag {
			return nil, false
		}

		keyField, wireType, err := GetProtoFieldAndWireType(currentEntry.Data, cachedFieldNumber)
		if err != nil || wireType != protowire.BytesType {
			return nil, false
		}

		prepared = append(prepared, preparedData{
			entry:    currentEntry,
			keyStr:   string(keyField),
			keyField: keyField,
		})
	}

	// 2. BATCH UPDATE (Inside the lock)
	uc.mu.Lock()
	defer uc.mu.Unlock()

	for _, p := range prepared {
		// Skip if there's no key (empty data case)
		if p.keyStr == "" {
			continue
		}

		if id, exists := uc.reverseCache[p.keyStr]; exists {
			// Update existing entry
			ent := uc.cache[id]
			if ent.lastIdx < p.entry.Index {
				ent.lastIdx = p.entry.Index
				uc.updateLRU(id)
				if uc.lastInFlight <= p.entry.Index {
					atomic.StoreUint64(&uc.lastInFlight, math.MaxUint64)
				}
			}
		} else {
			// Add new entry to cache
			newID := uc.nextID
			uc.nextID++

			newElem := &cacheEntry{
				id:       newID,
				key:      p.keyField,
				lastIdx:  p.entry.Index,
				addedIdx: p.entry.Index,
			}

			uc.cache[newID] = newElem
			uc.reverseCache[p.keyStr] = newID
			uc.addToLRU(newElem)
		}
	}

	return entries, true
}

func ReplaceProtoField(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	var out []byte
	for len(data) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("failed to consume tag")
		}
		originalTag := protowire.AppendTag(nil, fieldNum, wireType)
		data = data[n:]
		var fieldBytes []byte
		var skip int
		switch wireType {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(data)
			if m < 0 {
				return nil, errors.New("failed to consume varint")
			}
			fieldBytes = protowire.AppendVarint(nil, v)
			skip = m
		case protowire.Fixed32Type:
			v, m := protowire.ConsumeFixed32(data)
			if m < 0 {
				return nil, errors.New("failed to consume fixed32")
			}
			fieldBytes = protowire.AppendFixed32(nil, v)
			skip = m
		case protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(data)
			if m < 0 {
				return nil, errors.New("failed to consume fixed64")
			}
			fieldBytes = protowire.AppendFixed64(nil, v)
			skip = m
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return nil, errors.New("failed to consume bytes")
			}
			// For non-replaced fields, we want to keep the full encoding (tag + length + value)
			fieldBytes = protowire.AppendBytes(nil, v)
			skip = m
		case protowire.StartGroupType:
			v, m := protowire.ConsumeGroup(fieldNum, data)
			if m < 0 {
				return nil, errors.New("failed to consume group")
			}
			fieldBytes = v
			skip = m
		default:
			return nil, fmt.Errorf("unknown wire type: %v", wireType)
		}

		if int(fieldNum) == targetField {
			// Build the new field.
			var encodedNewValue []byte
			if newWireType == protowire.BytesType {
				encodedNewValue = protowire.AppendBytes(nil, newValue)
			} else {
				encodedNewValue = newValue
			}
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			out = append(out, newTag...)
			out = append(out, encodedNewValue...)
		} else {
			// Keep the field unchanged.
			out = append(out, originalTag...)
			out = append(out, fieldBytes...)
		}
		data = data[skip:]
	}
	return out, nil
}

func GetProtoFieldAndWireType(data []byte, targetField int) ([]byte, protowire.Type, error) {
	for len(data) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, 0, errors.New("failed to consume tag")
		}
		data = data[n:]
		if int(fieldNum) == targetField {
			switch wireType {
			case protowire.VarintType:
				v, nn := protowire.ConsumeVarint(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume varint")
				}
				return protowire.AppendVarint(nil, v), wireType, nil
			case protowire.BytesType:
				v, nn := protowire.ConsumeBytes(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume bytes")
				}
				return v, wireType, nil
			case protowire.Fixed32Type:
				v, nn := protowire.ConsumeFixed32(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume fixed32")
				}
				return protowire.AppendFixed32(nil, v), wireType, nil
			case protowire.Fixed64Type:
				v, nn := protowire.ConsumeFixed64(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume fixed64")
				}
				return protowire.AppendFixed64(nil, v), wireType, nil
			case protowire.StartGroupType:
				v, nn := protowire.ConsumeGroup(fieldNum, data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume group")
				}
				return v, wireType, nil
			default:
				return nil, 0, fmt.Errorf("unknown wire type: %v", wireType)
			}
		} else {
			var skip int
			switch wireType {
			case protowire.VarintType:
				_, skip = protowire.ConsumeVarint(data)
			case protowire.Fixed32Type:
				_, skip = protowire.ConsumeFixed32(data)
			case protowire.Fixed64Type:
				_, skip = protowire.ConsumeFixed64(data)
			case protowire.BytesType:
				_, skip = protowire.ConsumeBytes(data)
			case protowire.StartGroupType:
				_, skip = protowire.ConsumeGroup(fieldNum, data)
			default:
				return nil, 0, fmt.Errorf("unknown wire type: %v", wireType)
			}
			if skip < 0 {
				return nil, 0, errors.New("failed to skip field")
			}
			data = data[skip:]
		}
	}
	return nil, 0, fmt.Errorf("field number %d not found", targetField)
}
