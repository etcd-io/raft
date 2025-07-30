package unicache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

const cachedFieldNumber = 1

const maxCacheSize = 1000

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache(maxCommit *uint64, minCacheVersion func() uint64, capacity int) UniCache
	EncodeData(data []byte) ([]byte, uint32)
	DecodeEntry(entry pb.Entry) (pb.Entry, bool)
	SafeEncode(data []byte, appendIdx uint64, encodedID uint32) ([]byte, []byte)
	GetNextId() uint32
	PrintCache()
	UpdateCache(entry pb.Entry) (pb.Entry, bool)
	PurgeEvicted(idx uint64)
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

	cachehits uint64
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
func (uc *uniCache) NewUniCache(maxCommit *uint64, minCacheVersion func() uint64, capacity int) UniCache {
	return NewUniCache(maxCommit, minCacheVersion, capacity)
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
	elem := uc.lruList.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(*cacheEntry)

	if len(uc.cache) < uc.capacity && currIdx-entry.lastIdx <= uint64(uc.capacity) {
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
	// how many entries could still be needed?
	// (i.e. how far behind the slowest follower might lag)
	// guard against wrap/negative
	minC := uc.minCacheVersion()
	var window uint64
	if minC > uint64(uc.capacity) {
		window = minC - uint64(uc.capacity)
	} else {
		window = 0
	}

	// drop from the front until we’re down to 'window' elements
	for uc.evictOrder.Len() > int(window) {
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
	/*keys := make([]uint32, 0, len(uc.cache))
	for k := range uc.cache {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	fmt.Println("keys", keys)*/
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
			//fmt.Printf("[SafeEncode] index=%d cachehits=%d appendIdx=%d lastIdx=%d minCachedIdx=%d\n", appendIdx, cachehits, appendIdx, elem.lastIdx, uc.minCacheVersion())

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
	fmt.Printf("[SafeEncode] index=%d didnt find have data for for ID=%d \n", appendIdx, encodedID)
	return data, nil
}

func (uc *uniCache) EncodeData(data []byte) ([]byte, uint32) {
	if len(data) == 0 {
		return data, 0
	}

	uc.mu.RLock()
	defer uc.mu.RUnlock()

	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)

	if err != nil {
		return data, 0
	}

	keyStr := string(keyBytes)
	id, ok := uc.reverseCache[keyStr]

	if !ok {
		//fmt.Println("[EncodeData] not in reversecache")
		return data, 0
	}

	_, ok = uc.cache[id]
	if !ok {
		//fmt.Println("[EncodeData] not in cache")
		return data, 0
	}

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

	uc.mu.RLock()
	defer uc.mu.RUnlock()

	keyField, wireType, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		return entry, false
	}

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
			fmt.Println("[decode cache] not in cache: ", id, "index", entry.Index)
			return entry, false
		}
		newData, err := ReplaceProtoField(entry.Data, cachedFieldNumber, elem.key, protowire.BytesType)
		if err != nil {
			return entry, false
		}
		if err == nil {
			entry.Data = newData
		}
		return entry, true
	}
	return entry, true
}

func (uc *uniCache) UpdateCache(entry pb.Entry) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	uc.mu.Lock()
	defer uc.mu.Unlock()

	keyField, wireType, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		return entry, false
	}

	if wireType == protowire.VarintType {
		fmt.Println("[UpdateCache] Got unexpected VARINT")
		/*id, n := protowire.ConsumeVarint(keyField)
		if n <= 0 {
			return entry, false
		}
		elem, ok := uc.cache[uint32(id)]

		if !ok {
			fmt.Printf("[UpdateCache] index=%d ERROR key ID %d not in cache\n", entry.Index, id)
			return entry, false
		}

		newData, err := ReplaceProtoField(entry.Data, cachedFieldNumber, elem.key, protowire.BytesType)
		if err != nil {
			fmt.Printf("[ReplaceProtoField error] id=%d key=%x wireType=BytesType err=%v\n",
				id, elem.id, elem.key, err)
			debug.PrintStack()
			return entry, false
		}
		entry.Data = newData
		uc.updateLRU(uint32(id), entry.Index)

		if elem.lastIdx < entry.Index {
			elem.lastIdx = entry.Index
		}

		return entry, true*/

	} else if wireType == protowire.BytesType {
		keyStr := string(keyField)

		if id, ok := uc.reverseCache[keyStr]; !ok {
			newID := uc.nextID
			uc.nextID++
			elem := &cacheEntry{
				id:       newID,
				key:      keyField,
				lastIdx:  entry.Index,
				addedIdx: entry.Index,
			}
			uc.cache[newID] = elem
			uc.reverseCache[keyStr] = newID
			uc.addToLRU(elem)

			//fmt.Printf("[UpdateCache] index=%d LEARNED new key ID %d keyHash=%x\n",
			//entry.Index, newID, string(keyField))
		} else {
			ent := uc.cache[id]
			if ent.lastIdx < entry.Index {
				//fmt.Printf("[UpdateCache] index=%d updating key ID %d prevIndex=%d newIndex=%d keyHash=%x\n",
				//entry.Index, id, ent.lastIdx, entry.Index, string(keyField))
				ent.lastIdx = entry.Index
				uc.updateLRU(id)
			}
		}
		return entry, true
	}

	return entry, false
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
