package unicache

import (
	"container/list"
	"errors"
	"fmt"
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

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache(minCacheVersion func() uint64, capacity int) UniCache
	EncodeData(data []byte, currCacheIdx uint64) ([]byte, uint32)
	DecodeEntry(entry pb.Entry) (pb.Entry, bool)
	SafeEncode(data []byte, appendIdx uint64, encodedID uint32) ([]byte, []byte)
	GetNextId() uint32
	UpdateCache(entry pb.Entry) (pb.Entry, bool)
	BatchUpdateCache(entries []pb.Entry) ([]pb.Entry, bool)
	PurgeEvicted()
	CacheHits() uint64
	ResetCacheHits() uint64
}

type cacheEntry struct {
	id       uint32
	key      []byte
	lastIdx  uint64
	addedIdx uint64
}

type uniCache struct {
	cache        map[uint32]*cacheEntry
	reverseCache map[string]uint32
	nextID       uint32
	capacity     int

	lruList *list.List
	lruMap  map[uint32]*list.Element

	evicted         map[uint32]*list.Element
	evictOrder      *list.List
	evictedCapacity int

	maxCommit       *uint64
	minCacheVersion func() uint64

	cachehits uint64
}

// NewUniCache constructs a UniCache with simple LRU caching.
func NewUniCache(minCacheVersion func() uint64, capacity int) UniCache {
	return &uniCache{
		cache:        make(map[uint32]*cacheEntry),
		reverseCache: make(map[string]uint32),

		lruList: list.New(),
		lruMap:  make(map[uint32]*list.Element),

		nextID:   1,
		capacity: capacity,

		evicted:         make(map[uint32]*list.Element),
		evictOrder:      list.New(),
		evictedCapacity: 2 * capacity,

		minCacheVersion: minCacheVersion,

		cachehits: uint64(0),
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
func (uc *uniCache) NewUniCache(minCacheVersion func() uint64, capacity int) UniCache {
	return NewUniCache(minCacheVersion, capacity)
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

	evictedElem := uc.evictOrder.PushBack(entry)
	uc.evicted[entry.id] = evictedElem

	delete(uc.cache, entry.id)
	delete(uc.reverseCache, string(entry.key))
	delete(uc.lruMap, entry.id)
	uc.lruList.Remove(elem)

	// Cap the evicted buffer to prevent unbounded growth.
	for len(uc.evicted) > uc.evictedCapacity {
		front := uc.evictOrder.Front()
		if front == nil {
			break
		}
		oldest := front.Value.(*cacheEntry)
		uc.evictOrder.Remove(front)
		delete(uc.evicted, oldest.id)
	}
}

// PurgeEvicted removes entries from the front of evictOrder whose lastIdx is
// below the safety threshold T = max(0, minCacheVersion - capacity).
// When all followers have committed at least capacity steps past lastIdx,
// no follower can encode with that evicted ID anymore, so it is safe to drop.
func (uc *uniCache) PurgeEvicted() {
	minC := uc.minCacheVersion()
	var T uint64
	if minC > uint64(uc.capacity) {
		T = minC - uint64(uc.capacity)
	}
	for uc.evictOrder.Len() > 0 {
		front := uc.evictOrder.Front()
		e := front.Value.(*cacheEntry)
		if e.lastIdx >= T {
			break
		}
		uc.evictOrder.Remove(front)
		delete(uc.evicted, e.id)
	}
	// Belt-and-suspenders cap enforcement.
	for len(uc.evicted) > uc.evictedCapacity {
		front := uc.evictOrder.Front()
		if front == nil {
			break
		}
		oldest := front.Value.(*cacheEntry)
		uc.evictOrder.Remove(front)
		delete(uc.evicted, oldest.id)
	}
}

// IsEncodedData reports whether data contains a RepliCache-encoded entry,
// i.e. protobuf field 1 carries a varint cache ID instead of raw bytes.
func IsEncodedData(data []byte) bool {
	return len(data) > 0 && data[0] == cachedFieldVarintTag
}

func (uc *uniCache) GetNextId() uint32 {
	return uc.nextID
}

func (uc *uniCache) SafeEncode(data []byte, appendIdx uint64, encodedID uint32) ([]byte, []byte) {
	if len(data) == 0 || encodedID == 0 {
		return data, nil
	}

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
	return nil, nil
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
	id, ok := uc.reverseCache[keyStr]
	if !ok {
		return data, 0
	}
	if _, exists := uc.cache[id]; !exists {
		return data, 0
	}

	// Safety check: ID must be in active cache range
	// Only encode if id >= nextID - capacity (entry is in active cache)
	// Leader will have entry in cache or evicted (safety net)
	if uc.nextID > uint32(uc.capacity) {
		minActiveID := uc.nextID - uint32(uc.capacity/2)
		if id < minActiveID {
			return data, 0
		}
	}

	// Encoding outside the lock
	encodedID := protowire.AppendVarint(nil, uint64(id))
	newData, err := ReplaceProtoField(data, cachedFieldNumber, encodedID, protowire.VarintType)
	if err == nil {
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

		elem, ok := uc.cache[uint32(id)]
		if !ok {
			fmt.Println("[decode cache] not in cache: ", id, "index", entry.Index, "type ", entry.Type)
			return entry, false
		}

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
	id, exists := uc.reverseCache[keyStr]
	if exists {
		ent := uc.cache[id]

		// Key exists - only need write lock for brief update
		if ent.lastIdx < entry.Index {
			ent.lastIdx = entry.Index
			uc.updateLRU(id)
		}
		return entry, true
	}

	// Key doesn't exist - need write lock to add
	// Pre-allocate struct outside lock
	newElem := &cacheEntry{
		key:      keyField,
		lastIdx:  entry.Index,
		addedIdx: entry.Index,
	}

	// Double-check in case another goroutine added it
	if id, exists := uc.reverseCache[keyStr]; exists {
		ent := uc.cache[id]
		if ent.lastIdx < entry.Index {
			ent.lastIdx = entry.Index
			uc.updateLRU(id)
		}
		return entry, true
	}

	// Add new entry
	newID := uc.nextID
	uc.nextID++
	newElem.id = newID
	uc.cache[newID] = newElem
	uc.reverseCache[keyStr] = newID
	uc.addToLRU(newElem)

	return entry, true
}

// updateExistingCacheEntry updates lastIdx and LRU for an entry already in the
// active cache. Returns false if the id is not in the active cache.
func (uc *uniCache) updateExistingCacheEntry(id uint32, entryIndex uint64) bool {
	ent, ok := uc.cache[id]
	if !ok {
		return false
	}
	if ent.lastIdx < entryIndex {
		ent.lastIdx = entryIndex
		uc.updateLRU(id)
	}
	return true
}

// updateCacheFromEvicted restores an evicted key as a new cache entry.
// Returns false if the id is not in the evicted buffer.
func (uc *uniCache) updateCacheFromEvicted(evictedID uint32, entryIndex uint64) bool {
	evElem, ok := uc.evicted[evictedID]
	if !ok {
		return false
	}
	ev := evElem.Value.(*cacheEntry)

	// Remove from evicted
	uc.evictOrder.Remove(evElem)
	delete(uc.evicted, evictedID)

	// Re-add as a new cache entry
	newID := uc.nextID
	uc.nextID++
	newElem := &cacheEntry{
		id:       newID,
		key:      ev.key,
		lastIdx:  entryIndex,
		addedIdx: entryIndex,
	}
	uc.cache[newID] = newElem
	uc.reverseCache[string(ev.key)] = newID
	uc.addToLRU(newElem)
	return true
}

func (uc *uniCache) BatchUpdateCache(entries []pb.Entry) ([]pb.Entry, bool) {
	for i := range entries {
		entry := entries[i]

		// Skip empty data and non-normal entries
		if len(entry.Data) == 0 || entry.Type != pb.EntryNormal {
			continue
		}

		// Fast path: EncodedID != 0 means the leader already assigned a cache ID.
		// Look up directly by ID — no proto parsing, no string conversion.
		if entry.EncodedID != 0 {
			// Try active cache first (common case)
			if uc.updateExistingCacheEntry(entry.EncodedID, entry.Index) {
				continue
			}
			// Try evicted cache (rare: key was evicted between encode and commit)
			if uc.updateCacheFromEvicted(entry.EncodedID, entry.Index) {
				continue
			}
			// ID not found in either cache — fall through to slow path
		}

		// Slow path: parse proto to extract key (new key or follower path)
		currentData := entry.Data

		// Handle varint-encoded entries (leader seeing its own encoded entries)
		if currentData[0] == cachedFieldVarintTag {
			decoded, ok := uc.DecodeEntry(entry)
			if !ok {
				return nil, false
			}
			currentData = decoded.Data
		}

		if currentData[0] != cachedFieldBytesTag {
			return nil, false
		}

		keyField, wireType, err := GetProtoFieldAndWireType(currentData, cachedFieldNumber)
		if err != nil || wireType != protowire.BytesType {
			return nil, false
		}

		keyStr := string(keyField)

		if id, exists := uc.reverseCache[keyStr]; exists {
			// Key already in cache — update lastIdx
			uc.updateExistingCacheEntry(id, entry.Index)
		} else {
			// New key — add to cache
			newID := uc.nextID
			uc.nextID++
			newElem := &cacheEntry{
				id:       newID,
				key:      keyField,
				lastIdx:  entry.Index,
				addedIdx: entry.Index,
			}
			uc.cache[newID] = newElem
			uc.reverseCache[keyStr] = newID
			uc.addToLRU(newElem)
		}
	}

	return entries, true
}

func ReplaceProtoField(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	// Fast path: when the target field is first in the serialized data
	// (true for all cachedFieldNumber=1 call sites), we can skip the
	// per-field walk entirely and splice in a single pre-sized allocation.
	if len(data) > 0 && int(data[0]>>3) == targetField {
		oldWireType := protowire.Type(data[0] & 0x07)
		var fieldEnd int
		switch oldWireType {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data[1:])
			if n < 0 {
				return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
			}
			fieldEnd = 1 + n
		case protowire.BytesType:
			_, n := protowire.ConsumeBytes(data[1:])
			if n < 0 {
				return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
			}
			fieldEnd = 1 + n
		default:
			return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
		}
		rest := data[fieldEnd:]

		// New tag: single byte for field numbers 1-15.
		newTag := byte(targetField<<3) | byte(newWireType)

		// Pre-compute exact output size for a single allocation.
		var newFieldLen int
		if newWireType == protowire.BytesType {
			newFieldLen = protowire.SizeVarint(uint64(len(newValue))) + len(newValue)
		} else {
			newFieldLen = len(newValue)
		}
		out := make([]byte, 0, 1+newFieldLen+len(rest))
		out = append(out, newTag)
		if newWireType == protowire.BytesType {
			out = protowire.AppendVarint(out, uint64(len(newValue)))
			out = append(out, newValue...)
		} else {
			out = append(out, newValue...)
		}
		out = append(out, rest...)
		return out, nil
	}
	return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
}

func replaceProtoFieldGeneral(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
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
	// Fast path: target field is first in the serialized data.
	// Returns sub-slices (0 alloc) for both varint and bytes wire types.
	if len(data) > 0 && int(data[0]>>3) == targetField {
		wireType := protowire.Type(data[0] & 0x07)
		switch wireType {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data[1:])
			if n > 0 {
				return data[1 : 1+n], wireType, nil
			}
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(data[1:])
			if n > 0 {
				return v, wireType, nil
			}
		}
		// Fall through to general path on parse error or uncommon wire type.
	}
	return getProtoFieldGeneral(data, targetField)
}

func getProtoFieldGeneral(data []byte, targetField int) ([]byte, protowire.Type, error) {
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
