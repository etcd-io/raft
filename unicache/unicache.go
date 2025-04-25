package unicache

import (
	"container/list"
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

const cachedFieldNumber = 1

const maxCacheSize = 10000

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache() UniCache
	EncodeData(data []byte, nextId *uint32) []byte
	DecodeEntry(entry pb.Entry) (pb.Entry, bool)
	GetNextId() uint32
}

type cacheEntry struct {
	id  uint32
	key []byte
}

type uniCache struct {
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
	return uc.nextID
}

// EncodeData replaces a cached key with a varint ID by first copying
// the original slice and then doing an in-place compress on the copy.
func (uc *uniCache) EncodeData(data []byte, nextId *uint32) []byte {
	if len(data) == 0 {
		return data
	}
	// 1) Extract the keyBytes
	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	if err != nil {
		return data
	}

	keyStr := string(keyBytes)
	id, ok := uc.reverseCache[keyStr]

	if ok && id < *nextId {
		uc.updateLRU(id)

		// 2) Copy original data into a new buffer
		buf := make([]byte, len(data))
		copy(buf, data)

		// 3) Compress in-place on the copy
		encodedID := protowire.AppendVarint(nil, uint64(id))
		newData, err := ReplaceProtoFieldInPlaceCompress(buf, cachedFieldNumber, encodedID, protowire.VarintType)
		if err == nil {
			return newData
		}
		// on error, fall through and return original
	}

	// MISS path: record key and update LRU, leave original data untouched
	newID := *nextId
	uc.reverseCache[keyStr] = newID
	uc.cache[newID] = keyBytes
	uc.addToLRU(newID, keyBytes)
	*nextId++

	return data
}

// DecodeEntry restores original key bytes or caches first-seen keys.
func (uc *uniCache) DecodeEntry(entry pb.Entry) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	keyField, wireType, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		return entry, true
	}
	if wireType == protowire.BytesType {
		keyStr := string(keyField)
		if id, ok := uc.reverseCache[keyStr]; !ok {
			newID := uc.nextID
			uc.nextID++
			uc.cache[newID] = keyField
			uc.reverseCache[keyStr] = newID
			uc.addToLRU(newID, keyField)
		} else {
			uc.updateLRU(id)
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
		uc.updateLRU(uint32(id))
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
		end      int
		isTarget bool
		newLen   int
	}
	var fields []fieldInfo
	i := 0
	for i < len(data) {
		start := i
		fieldNum, wireType, n := protowire.ConsumeTag(data[i:])
		if n < 0 {
			return nil, errors.New("bad tag")
		}
		i += n

		var oldValLen int
		switch wireType {
		case protowire.VarintType:
			_, vLen := protowire.ConsumeVarint(data[i:])
			oldValLen = vLen
		case protowire.BytesType:
			_, vLen := protowire.ConsumeBytes(data[i:])
			oldValLen = vLen
		default:
			skip, _ := skipField(wireType, data[i:])
			i += skip
			continue
		}
		end := i + oldValLen

		isTarget := int(fieldNum) == targetField
		newLen := end - start
		if isTarget {
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			var newField []byte
			if newWireType == protowire.BytesType {
				newField = protowire.AppendBytes(nil, newValue)
			} else {
				newField = newValue
			}
			newLen = len(newTag) + len(newField)
			if newLen > end-start {
				return nil, fmt.Errorf("new field encoding is larger than original")
			}
		}
		fields = append(fields, fieldInfo{start, end, isTarget, newLen})
		i = end
	}

	// compute total
	newTotal := 0
	for _, f := range fields {
		newTotal += f.newLen
	}
	writePos := newTotal
	for j := len(fields) - 1; j >= 0; j-- {
		f := fields[j]
		writePos -= f.newLen
		if f.isTarget {
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			var newField []byte
			if newWireType == protowire.BytesType {
				newField = protowire.AppendBytes(nil, newValue)
			} else {
				newField = newValue
			}
			copy(data[writePos:], newTag)
			copy(data[writePos+len(newTag):], newField)
		} else {
			copy(data[writePos:], data[f.start:f.end])
		}
	}

	return data[:newTotal], nil
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
