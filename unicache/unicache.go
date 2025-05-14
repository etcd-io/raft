package unicache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

const cachedFieldNumber = 1

const maxCacheSize = 2

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache() UniCache
	EncodeData(data []byte) []byte
	DecodeEntry(entry pb.Entry, touchLRU bool) (pb.Entry, bool)
	GetNextId() uint32
}

type cacheEntry struct {
	id  uint32
	key []byte
}

type uniCache struct {
	mu           sync.RWMutex
	cache        map[uint32][]byte
	reverseCache map[string]uint32
	lruList      *list.List
	lruMap       map[uint32]*list.Element
	nextID       uint32
	capacity     int
}

// NewUniCache constructs a UniCache with simple LRU caching.
func NewUniCache() UniCache {
	return &uniCache{
		cache:        make(map[uint32][]byte),
		reverseCache: make(map[string]uint32),
		lruList:      list.New(),
		lruMap:       make(map[uint32]*list.Element),
		nextID:       1,
		capacity:     maxCacheSize,
	}
}

// NewUniCache implements the UniCache interface.
func (uc *uniCache) NewUniCache() UniCache {
	return NewUniCache()
}

func (uc *uniCache) updateLRU(id uint32) {
	if elem, ok := uc.lruMap[id]; ok {
		uc.lruList.MoveToFront(elem)
	}
}

func (uc *uniCache) addToLRU(id uint32, key []byte) {
	entry := cacheEntry{id: id, key: key}
	elem := uc.lruList.PushFront(entry)
	uc.lruMap[id] = elem

	if uc.lruList.Len() > uc.capacity {
		uc.evictLRU()
	}
}

func (uc *uniCache) evictLRU() {
	elem := uc.lruList.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(cacheEntry)

	delete(uc.cache, entry.id)
	delete(uc.reverseCache, string(entry.key))
	delete(uc.lruMap, entry.id)
	uc.lruList.Remove(elem)
}

func (uc *uniCache) GetNextId() uint32 {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	return uc.nextID
}

// EncodeData replaces a cached key with a varint ID by first copying
// the original slice and then doing an in-place compress on the copy.
func (uc *uniCache) EncodeData(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	if err != nil {
		return data
	}

	uc.mu.Lock()
	defer uc.mu.Unlock()

	keyStr := string(keyBytes)
	id, ok := uc.reverseCache[keyStr]

	if ok && id < uc.nextID {

		encodedID := protowire.AppendVarint(nil, uint64(id))
		newData, err := ReplaceProtoField(data, cachedFieldNumber, encodedID, protowire.VarintType)
		if err == nil {
			return newData
		}
	}

	return data
}

// DecodeEntry restores original key bytes or caches first-seen keys.
func (uc *uniCache) DecodeEntry(entry pb.Entry, touchLRU bool) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	keyField, wireType, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		return entry, true
	}

	uc.mu.Lock()
	defer uc.mu.Unlock()

	if wireType == protowire.BytesType {
		keyStr := string(keyField)
		if id, ok := uc.reverseCache[keyStr]; !ok {
			newID := uc.nextID
			uc.nextID++
			uc.cache[newID] = keyField
			uc.reverseCache[keyStr] = newID
			uc.addToLRU(newID, keyField)
		} else {
			if touchLRU {
				uc.updateLRU(uint32(id))
			}
		}
		return entry, true
	} else if wireType == protowire.VarintType {
		id, n := protowire.ConsumeVarint(keyField)
		if n <= 0 {
			return entry, false
		}
		origKey, ok := uc.cache[uint32(id)]
		if !ok {
			return entry, false
		}
		if touchLRU {
			uc.updateLRU(uint32(id))
		}
		newData, err := ReplaceProtoField(entry.Data, cachedFieldNumber, origKey, protowire.BytesType)
		if err == nil {
			entry.Data = newData
		}
		return entry, true
	} else {
		return entry, true
	}
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

// ReplaceProtoFieldInPlaceCompress replaces a field in-place when the new encoding is shorter.
func ReplaceProtoFieldInPlaceCompress(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	type fieldInfo struct {
		start    int
		fieldLen int
		isTarget bool
		newLen   int
	}

	// First pass: discover field offsets and lengths
	var fields []fieldInfo
	i := 0
	for i < len(data) {
		// 1) Consume tag
		fieldNum, wireType, tagLen := protowire.ConsumeTag(data[i:])
		if tagLen < 0 {
			return nil, fmt.Errorf("bad tag at offset %d", i)
		}
		start := i
		i += tagLen

		// 2) Consume the rest of the field (length-prefix + value) to get its total length
		var valLen int
		switch wireType {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data[i:])
			if n < 0 {
				return nil, fmt.Errorf("bad varint at offset %d", i)
			}
			valLen = n

		case protowire.BytesType:
			_, n := protowire.ConsumeBytes(data[i:])
			if n < 0 {
				return nil, fmt.Errorf("bad bytes at offset %d", i)
			}
			valLen = n

		default:
			// skip unsupported types entirely
			skip, err := skipField(wireType, data[i:])
			if err != nil {
				return nil, err
			}
			i += skip
			continue
		}

		fieldLen := tagLen + valLen
		isTarget := int(fieldNum) == targetField

		// 3) Compute replacement length if this is our target
		newLen := fieldLen
		if isTarget {
			tagBytes := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			var valBytes []byte
			if newWireType == protowire.BytesType {
				valBytes = protowire.AppendBytes(nil, newValue)
			} else {
				valBytes = newValue
			}
			newLen = len(tagBytes) + len(valBytes)
			if newLen > fieldLen {
				return nil, fmt.Errorf("replacement longer than original field")
			}
		}

		fields = append(fields, fieldInfo{
			start:    start,
			fieldLen: fieldLen,
			isTarget: isTarget,
			newLen:   newLen,
		})
		i = start + fieldLen
	}

	// Second pass: rewrite into the same buffer, backwards
	totalNew := 0
	for _, f := range fields {
		totalNew += f.newLen
	}
	writePos := totalNew

	for idx := len(fields) - 1; idx >= 0; idx-- {
		f := fields[idx]
		writePos -= f.newLen

		if f.isTarget {
			// build new tag+value
			tagBytes := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			var valBytes []byte
			if newWireType == protowire.BytesType {
				valBytes = protowire.AppendBytes(nil, newValue)
			} else {
				valBytes = newValue
			}
			copy(data[writePos:], tagBytes)
			copy(data[writePos+len(tagBytes):], valBytes)

		} else {
			// copy original field bytes
			copy(data[writePos:], data[f.start:f.start+f.fieldLen])
		}
	}

	return data[:totalNew], nil
}

func skipField(wt protowire.Type, data []byte) (int, error) {
	switch wt {
	case protowire.Fixed32Type:
		return 4, nil
	case protowire.Fixed64Type:
		return 8, nil
	case protowire.StartGroupType:
		_, n := protowire.ConsumeGroup(0, data)
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported wire type %v", wt)
	}
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
