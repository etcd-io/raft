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

func (uc *uniCache) EncodeData(data []byte, nextId *uint32) []byte {
	if len(data) == 0 {
		return data
	}

	fmt.Println("data: ", data)
	// Extract the keyBytes: bytes representing the field to be encoded.
	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	fmt.Println("keybytes:", keyBytes)
	if err != nil {
		return data
	}

	keyStr := string(keyBytes)
	id, ok := uc.reverseCache[keyStr]

	if ok && id < *nextId {
		uc.updateLRU(id)

		encodedID := protowire.AppendVarint(nil, uint64(id))
		newData, err := ReplaceProtoFieldInPlaceCompress(data, cachedFieldNumber, encodedID, protowire.VarintType)
		fmt.Println("newdata: ", newData)
		if err != nil {
			return data
		}
		return newData
	}

	newID := *nextId
	if _, exists := uc.lruMap[newID]; exists {
		uc.updateLRU(newID)
	} else {
		uc.addToLRU(newID, keyBytes)
	}

	uc.cache[newID] = keyBytes
	uc.reverseCache[keyStr] = newID
	fmt.Println("add to cache: ", keyStr)
	fmt.Println("cache: ", uc.cache)
	(*nextId)++

	return data
}

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
			fmt.Println("DecodeEntry - id not found in cache:", id)
			fmt.Println("cache size", uc.lruList.Len())
			return entry, false
		}
		uc.updateLRU(uint32(id))
		newData, err := ReplaceProtoField(entry.Data, cachedFieldNumber, origKey, protowire.BytesType)
		if err != nil {
			fmt.Println("DecodeEntry - error replacing key field:", err)
			return entry, false
		}
		entry.Data = newData
		return entry, true
	} else {
		return entry, true
	}
}

// ReplaceProtoField is a helper that scans a protobuf-encoded message in data,
// and whenever it finds a field with number targetField it replaces that field’s value
// with newValue and uses newWireType. (It leaves all other fields unchanged.)
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

// ReplaceProtoFieldInPlaceCompress replaces occurrences of the target field in the
// protobuf message contained in data, handling only the compressing case (new encoding is shorter).
// For BytesType fields, it correctly inserts the length prefix.
func ReplaceProtoFieldInPlaceCompress(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	type fieldInfo struct {
		start    int  // start index of the field in the original slice
		end      int  // end index (exclusive)
		isTarget bool // whether this field is the one to replace
		newLen   int  // the length of the field after replacement
	}
	var fields []fieldInfo
	i := 0
	// First pass: record each field's boundaries and compute new lengths.
	for i < len(data) {
		start := i
		// Consume the tag.
		fieldNum, wireType, n := protowire.ConsumeTag(data[i:])
		if n < 0 {
			return nil, errors.New("failed to consume tag")
		}
		i += n

		var skip int
		switch wireType {
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume varint")
			}
			skip = m
		case protowire.Fixed32Type:
			_, m := protowire.ConsumeFixed32(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume fixed32")
			}
			skip = m
		case protowire.Fixed64Type:
			_, m := protowire.ConsumeFixed64(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume fixed64")
			}
			skip = m
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume bytes")
			}
			skip = m
		case protowire.StartGroupType:
			_, m := protowire.ConsumeGroup(fieldNum, data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume group")
			}
			skip = m
		default:
			return nil, fmt.Errorf("unknown wire type: %v", wireType)
		}
		i += skip

		origFieldLen := i - start
		isTarget := int(fieldNum) == targetField
		newFieldLen := origFieldLen
		if isTarget {
			// Build the new tag.
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			// For BytesType fields, the proper encoding uses protowire.AppendBytes,
			// which adds a length prefix. For other types, we use newValue directly.
			var newFieldBytes []byte
			if newWireType == protowire.BytesType {
				newFieldBytes = protowire.AppendBytes(nil, newValue)
			} else {
				newFieldBytes = newValue
			}
			newFieldLen = len(newTag) + len(newFieldBytes)
			// We expect newFieldLen to be <= origFieldLen.
			if newFieldLen > origFieldLen {
				return nil, fmt.Errorf("new field encoding is larger than original; expected compressing")
			}
		}
		fields = append(fields, fieldInfo{start: start, end: i, isTarget: isTarget, newLen: newFieldLen})
	}

	// Calculate the total new length.
	newTotalLen := 0
	for _, f := range fields {
		newTotalLen += f.newLen
	}

	// Second pass: Copy fields backwards to avoid overwriting data that hasn't been moved.
	writePos := newTotalLen
	for j := len(fields) - 1; j >= 0; j-- {
		f := fields[j]
		writePos -= f.newLen
		if f.isTarget {
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			var newFieldBytes []byte
			if newWireType == protowire.BytesType {
				newFieldBytes = protowire.AppendBytes(nil, newValue)
			} else {
				newFieldBytes = newValue
			}
			copy(data[writePos:], newTag)
			copy(data[writePos+len(newTag):], newFieldBytes)
		} else {
			copy(data[writePos:], data[f.start:f.end])
		}
	}

	// Return the slice re-sliced to the new length.
	return data[:newTotalLen], nil
}

// GetProtoFieldAndWireType scans the provided protobuf-encoded data looking for the first
// occurrence of the field with number targetField. It returns the raw value bytes, the field’s wire type,
// or an error if the field isn’t found.
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
				v, n := protowire.ConsumeVarint(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume varint")
				}
				return protowire.AppendVarint(nil, v), wireType, nil
			case protowire.BytesType:
				v, n := protowire.ConsumeBytes(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume bytes")
				}
				return v, wireType, nil
			case protowire.Fixed32Type:
				v, n := protowire.ConsumeFixed32(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume fixed32")
				}
				return protowire.AppendFixed32(nil, v), wireType, nil
			case protowire.Fixed64Type:
				v, n := protowire.ConsumeFixed64(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume fixed64")
				}
				return protowire.AppendFixed64(nil, v), wireType, nil
			case protowire.StartGroupType:
				v, n := protowire.ConsumeGroup(fieldNum, data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume group")
				}
				return v, wireType, nil
			default:
				return nil, 0, fmt.Errorf("unknown wire type: %v", wireType)
			}
		} else {
			// Skip this field.
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
