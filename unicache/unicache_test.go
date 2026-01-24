package unicache

import (
	"fmt"
	"sync"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestLRUEviction ensures that the LRU cache never grows beyond its capacity.
func TestLRUEviction(t *testing.T) {
	minC := func() uint64 { return 0 }
	uc, ok := NewUniCache(minC, 1000).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}

	cap := uc.capacity
	// Insert cap + extra entries
	for i := 0; i < cap+50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		// Simulate encoding to populate cache
		data, _ := uc.EncodeData(encodeProtoField(1, key), 0)
		// Force an entry with unique ID into the cache
		uc.DecodeEntry(makeEntry(data, uint64(i+1)))
	}

	if len(uc.cache) != cap {
		t.Fatalf("expected cache size %d, got %d", cap, len(uc.cache))
	}
}

// TestLRUConcurrency runs EncodeData and DecodeEntry in parallel to detect races.
func TestLRUConcurrency(t *testing.T) {
	minC := func() uint64 { return 0 }
	uc, ok := NewUniCache(minC, 1000).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}
	var wg sync.WaitGroup
	goroutines := 200
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := []byte(fmt.Sprintf("goroutine-%d-key-%d", id, j%1000))
				// Simulate client data with a cache field
				data, _ := uc.EncodeData(encodeProtoField(cachedFieldNumber, key), 0)
				// Wrap into an Entry for DecodeEntry
				entry := makeEntry(data, uint64(j+1))
				uc.DecodeEntry(entry)
			}
		}(i)
	}
	wg.Wait()
}

func TestLRUHeavyConcurrency(t *testing.T) {
	minC := func() uint64 { return 0 }
	uc, ok := NewUniCache(minC, 1000).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}
	var wg sync.WaitGroup
	const G = 200
	const I = 10000
	for i := 0; i < G; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < I; j++ {
				key := []byte(fmt.Sprintf("g%d-key-%d", id, j%500))
				data, _ := uc.EncodeData(encodeProtoField(cachedFieldNumber, key), 0)
				entry := makeEntry(data, uint64(j+1))
				uc.DecodeEntry(entry)
			}
		}(i)
	}
	wg.Wait()
}

// Helper to construct a pb.Entry for testing
func makeEntry(data []byte, index uint64) pb.Entry {
	return pb.Entry{Index: index, Term: 1, Type: pb.EntryNormal, Data: data}
}

// encodeProtoField wraps raw key as a simple protobuf field for testing
func encodeProtoField(fieldNum int, value []byte) []byte {
	// Use protowire to build a length-delimited field
	encoded := protowire.AppendTag(nil, protowire.Number(fieldNum), protowire.BytesType)
	encoded = protowire.AppendVarint(encoded, uint64(len(value)))
	encoded = append(encoded, value...)
	return encoded
}
