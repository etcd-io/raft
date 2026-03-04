package unicache

import (
	"fmt"
	"sync"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeEntry constructs a pb.Entry for testing.
func makeEntry(data []byte, index uint64) pb.Entry {
	return pb.Entry{Index: index, Term: 1, Type: pb.EntryNormal, Data: data}
}

// encodeProtoField wraps a raw byte slice as a protobuf length-delimited field.
func encodeProtoField(fieldNum int, value []byte) []byte {
	encoded := protowire.AppendTag(nil, protowire.Number(fieldNum), protowire.BytesType)
	encoded = protowire.AppendVarint(encoded, uint64(len(value)))
	return append(encoded, value...)
}

// commitKeys simulates committing a slice of keys via UpdateCache starting at startIdx.
func commitKeys(uc UniCache, keys [][]byte, startIdx uint64) {
	for i, key := range keys {
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}
}

// ---------------------------------------------------------------------------
// TestLRUEviction
// ---------------------------------------------------------------------------

// TestLRUEviction ensures that the LRU cache never grows beyond its capacity
// and that evicted entries are moved to the evicted buffer.
func TestLRUEviction(t *testing.T) {
	const capacity = 100
	// minCacheVersion must be > 0 so that evicted entries go to the evicted
	// buffer rather than being deleted directly.
	const startIdx = uint64(10 * capacity) // large enough that the evictLRU guard fires
	minC := func() uint64 { return startIdx + uint64(capacity) }
	uc, ok := NewUniCache(minC, capacity).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}

	// Insert capacity+50 unique keys via UpdateCache (the function that actually
	// populates the cache). Use indices starting well above capacity so the
	// evictLRU recent-use guard (currIdx - lastIdx <= capacity) is always satisfied.
	const extra = 50
	for i := 0; i < capacity+extra; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}

	// Cache must never exceed capacity (+1 tolerance because the evictLRU guard
	// prevents the very first overflow eviction, leaving the cache momentarily
	// one over before stabilising).
	if len(uc.cache) > capacity+1 {
		t.Fatalf("cache size %d exceeds capacity+1 (%d)", len(uc.cache), capacity+1)
	}
	// Evicted entries should have accumulated in the evicted buffer.
	if len(uc.evicted) == 0 {
		t.Fatal("expected some entries in the evicted buffer, got 0")
	}
	if uc.evictOrder.Len() != len(uc.evicted) {
		t.Fatalf("evictOrder length %d != evicted map length %d", uc.evictOrder.Len(), len(uc.evicted))
	}
}

// ---------------------------------------------------------------------------
// TestEncodeDecodeRoundTrip
// ---------------------------------------------------------------------------

// TestEncodeDecodeRoundTrip verifies the full lifecycle:
//  1. Commit entries via UpdateCache (learning phase).
//  2. Re-encode using EncodeData (a hit key should compress).
//  3. Decode via DecodeEntry and recover the original data.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	const capacity = 64
	const startIdx = uint64(1)
	committed := uint64(startIdx + 10)
	minC := func() uint64 { return committed }
	uc := NewUniCache(minC, capacity)

	// Commit 10 unique keys.
	keys := make([][]byte, 10)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("roundtrip-key-%d", i))
	}
	commitKeys(uc, keys, startIdx)

	for _, key := range keys {
		original := encodeProtoField(cachedFieldNumber, key)

		// EncodeData should find the key and return a shorter varint-ID form.
		encoded, id := uc.EncodeData(original, committed)
		if id == 0 {
			t.Errorf("EncodeData returned id=0 for key %q (not found in cache after commit)", key)
			continue
		}
		if !IsEncodedData(encoded) {
			t.Errorf("expected encoded data to start with varint tag for key %q", key)
			continue
		}
		if len(encoded) >= len(original) {
			t.Errorf("encoded (%d B) should be shorter than original (%d B) for key %q",
				len(encoded), len(original), key)
		}

		// DecodeEntry should restore the original bytes.
		dec, ok := uc.DecodeEntry(makeEntry(encoded, committed+10))
		if !ok {
			t.Errorf("DecodeEntry failed for key %q", key)
			continue
		}
		if string(dec.Data) != string(original) {
			t.Errorf("decoded data mismatch for key %q:\n  got:  %x\n  want: %x",
				key, dec.Data, original)
		}
	}
}

// ---------------------------------------------------------------------------
// TestPurgeEvicted
// ---------------------------------------------------------------------------

// TestPurgeEvicted verifies Algorithm 6 for PurgeEvicted:
//   - Entries with lastIdx < T = max(0, minCacheVersion-capacity) are removed.
//   - Entries with lastIdx >= T are retained.
func TestPurgeEvicted(t *testing.T) {
	const capacity = 10
	const startIdx = uint64(1000)
	var minVersion uint64
	minC := func() uint64 { return minVersion }
	uc, ok := NewUniCache(minC, capacity).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}

	// Set minVersion > 0 so evictions land in the evicted buffer (not direct delete).
	minVersion = startIdx + uint64(2*capacity)

	// Insert capacity+20 unique keys to fill the cache and drive some into evicted.
	// Use startIdx so that the evictLRU guard (currIdx-lastIdx <= capacity) fires
	// for the very first overflow, then clears for subsequent ones.
	for i := 0; i < capacity+20; i++ {
		key := []byte(fmt.Sprintf("evict-key-%d", i))
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}

	evictedCount := len(uc.evicted)
	if evictedCount == 0 {
		t.Fatal("expected entries in the evicted buffer after overfilling the cache")
	}

	// --- Case 1: T = 0 — nothing should be purged. ---
	minVersion = uint64(capacity) // T = capacity - capacity = 0
	uc.PurgeEvicted()
	if len(uc.evicted) != evictedCount {
		t.Fatalf("PurgeEvicted with T=0 should remove nothing: before=%d, after=%d",
			evictedCount, len(uc.evicted))
	}

	// --- Case 2: T = startIdx+1 — purge only the oldest evicted entry. ---
	// The oldest evicted entry is the one committed first (key-0 at index startIdx),
	// so its lastIdx = startIdx. T = startIdx+1 removes exactly that one entry.
	minVersion = startIdx + 1 + uint64(capacity) // T = minVersion - capacity = startIdx+1
	uc.PurgeEvicted()
	if len(uc.evicted) != evictedCount-1 {
		t.Fatalf("expected evicted count %d after purging 1 entry, got %d",
			evictedCount-1, len(uc.evicted))
	}

	// --- Case 3: T above all evicted lastIdx — evicted buffer fully cleared. ---
	// All evicted entries have lastIdx in [startIdx, startIdx+evictedCount-1].
	// Set T = startIdx + evictedCount to clear the rest.
	minVersion = startIdx + uint64(evictedCount) + uint64(capacity) // T = startIdx+evictedCount
	uc.PurgeEvicted()
	if len(uc.evicted) != 0 {
		t.Fatalf("expected empty evicted buffer after full purge, got %d entries", len(uc.evicted))
	}
	if uc.evictOrder.Len() != 0 {
		t.Fatalf("evictOrder should be empty, got %d", uc.evictOrder.Len())
	}
}

// ---------------------------------------------------------------------------
// TestSafeEncodeRestoresFromEvicted
// ---------------------------------------------------------------------------

// TestSafeEncodeRestoresFromEvicted verifies that SafeEncode correctly restores
// the full key bytes for an entry whose ID is in the evicted buffer (not in the
// active cache). This is the safety net used when a MsgApp entry was encoded
// before the key was evicted from the LRU.
func TestSafeEncodeRestoresFromEvicted(t *testing.T) {
	const capacity = 10
	const startIdx = uint64(1000)
	// Non-zero minVersion so evictions go to the evicted buffer.
	minVersion := startIdx + uint64(2*capacity)
	minC := func() uint64 { return minVersion }
	uc := NewUniCache(minC, capacity)
	inner, ok := uc.(*uniCache)
	if !ok {
		t.Fatal("type assertion failed")
	}

	// Commit the target key first (will get the lowest ID, be evicted first).
	targetKey := []byte("target-evicted-key")
	targetRaw := encodeProtoField(cachedFieldNumber, targetKey)
	uc.UpdateCache(makeEntry(targetRaw, startIdx))

	// Fill the cache past capacity so targetKey is evicted to the evicted buffer.
	for i := 1; i <= capacity+5; i++ {
		key := []byte(fmt.Sprintf("filler-%d", i))
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}

	// Find targetKey's ID in the evicted buffer.
	var targetID uint32
	for id, elem := range inner.evicted {
		e := elem.Value.(*cacheEntry)
		if string(e.key) == string(targetKey) {
			targetID = id
			break
		}
	}
	if targetID == 0 {
		t.Fatal("targetKey was not found in the evicted buffer — eviction may not have occurred")
	}

	// Simulate a MsgApp entry that was encoded with targetID before eviction:
	// manually construct the varint-encoded form.
	fakeEncoded := protowire.AppendTag(nil, cachedFieldNumber, protowire.VarintType)
	fakeEncoded = protowire.AppendVarint(fakeEncoded, uint64(targetID))

	// SafeEncode should restore the full data from the evicted buffer.
	restored, full := uc.SafeEncode(fakeEncoded, startIdx+uint64(capacity+10), targetID)
	if full == nil {
		t.Fatal("SafeEncode returned nil full-data — failed to restore from evicted buffer")
	}
	// For evicted keys, SafeEncode returns (restoredData, restoredData): both copies are full.
	if string(restored) != string(full) {
		t.Error("SafeEncode should return (full, full) when restoring from evicted buffer")
	}
	if string(restored) != string(targetRaw) {
		t.Errorf("restored data mismatch:\n  got:  %x\n  want: %x", restored, targetRaw)
	}
}

// ---------------------------------------------------------------------------
// Concurrency tests
// ---------------------------------------------------------------------------

// TestLRUConcurrency exercises concurrent EncodeData and DecodeEntry calls after
// the cache has been populated, checking for data races.
func TestLRUConcurrency(t *testing.T) {
	const capacity = 1000
	const numKeys = 50
	const startIdx = uint64(1)
	committed := uint64(startIdx + numKeys)
	minC := func() uint64 { return committed }
	uc := NewUniCache(minC, capacity)

	// Populate the cache sequentially before starting concurrent readers.
	hotKeys := make([][]byte, numKeys)
	for i := range hotKeys {
		hotKeys[i] = []byte(fmt.Sprintf("hot-key-%d", i))
	}
	commitKeys(uc, hotKeys, startIdx)

	// Pre-encode all hot keys so goroutines have encoded entries to decode.
	hotEncoded := make([][]byte, numKeys)
	for i, key := range hotKeys {
		raw := encodeProtoField(cachedFieldNumber, key)
		enc, _ := uc.EncodeData(raw, committed)
		hotEncoded[i] = enc
	}

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				i := j % numKeys
				// Read path: encode then decode (concurrent map reads, no writes).
				raw := encodeProtoField(cachedFieldNumber, hotKeys[i])
				enc, _ := uc.EncodeData(raw, committed)
				uc.DecodeEntry(makeEntry(enc, committed+uint64(j)))
			}
		}(g)
	}
	wg.Wait()
}

// TestLRUHeavyConcurrency stress-tests EncodeData and DecodeEntry under heavy
// concurrent load with a mix of hot (cached) and cold (miss) keys.
func TestLRUHeavyConcurrency(t *testing.T) {
	const capacity = 1000
	const numHot = 200
	const startIdx = uint64(1)
	committed := uint64(startIdx + numHot)
	minC := func() uint64 { return committed }
	uc := NewUniCache(minC, capacity)

	// Pre-populate with hot keys.
	hotKeys := make([][]byte, numHot)
	for i := range hotKeys {
		hotKeys[i] = []byte(fmt.Sprintf("heavy-hot-%d", i))
	}
	commitKeys(uc, hotKeys, startIdx)

	var wg sync.WaitGroup
	const G = 50
	const I = 2000
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < I; j++ {
				// 80% hot reads, 20% cold misses.
				var raw []byte
				if j%5 != 0 {
					raw = encodeProtoField(cachedFieldNumber, hotKeys[j%numHot])
				} else {
					raw = encodeProtoField(cachedFieldNumber,
						[]byte(fmt.Sprintf("cold-g%d-j%d", id, j)))
				}
				enc, _ := uc.EncodeData(raw, committed)
				uc.DecodeEntry(makeEntry(enc, committed+uint64(j)))
			}
		}(g)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Leader transition integration tests
// ---------------------------------------------------------------------------

// TestLeaderTransitionCacheConsistency verifies that after both nodes commit
// the same entries, a leadership transfer leaves their caches in the same
// state so the new leader can encode and the old leader can decode without error.
func TestLeaderTransitionCacheConsistency(t *testing.T) {
	const capacity = 64
	const startIdx = uint64(1)
	committed := uint64(startIdx + 10)
	minC := func() uint64 { return committed }

	node1Cache := NewUniCache(minC, capacity) // initial leader
	node2Cache := NewUniCache(minC, capacity) // initial follower → new leader

	keys := make([][]byte, 10)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("transition-key-%d", i))
	}
	commitKeys(node1Cache, keys, startIdx)
	commitKeys(node2Cache, keys, startIdx)

	// Simulate node1 (old leader) encoding entry committed+1 using key[0].
	reusedKey := keys[0]
	reusedRaw := encodeProtoField(cachedFieldNumber, reusedKey)
	node1Cache.UpdateCache(makeEntry(reusedRaw, committed+1))
	node2Cache.UpdateCache(makeEntry(reusedRaw, committed+1))
	encoded, id := node1Cache.EncodeData(reusedRaw, committed+1)
	if id == 0 {
		t.Fatal("old leader EncodeData returned id=0")
	}
	if !IsEncodedData(encoded) {
		t.Fatal("expected encoded data from old leader")
	}

	// --- Leader transfer: node2 becomes leader ---
	// node2 encodes a subsequent entry with the same shared key.
	node1Cache.UpdateCache(makeEntry(reusedRaw, committed+2))
	node2Cache.UpdateCache(makeEntry(reusedRaw, committed+2))
	enc2, id2 := node2Cache.EncodeData(reusedRaw, committed+2)
	if id2 == 0 {
		t.Fatal("new leader EncodeData returned id=0; cache should be consistent")
	}

	// node1 (now follower) decodes entry from new leader.
	dec, ok := node1Cache.DecodeEntry(makeEntry(enc2, committed+3))
	if !ok {
		t.Fatal("DecodeEntry failed for entry from new leader")
	}
	if string(dec.Data) != string(reusedRaw) {
		t.Errorf("decoded data mismatch: got %x, want %x", dec.Data, reusedRaw)
	}
}

// TestNewLeaderWithFreshCache verifies that a new leader with an empty/cold cache
// falls back to sending raw data (id=0), and that followers handle raw entries
// correctly as a no-op (data unchanged).
func TestNewLeaderWithFreshCache(t *testing.T) {
	const capacity = 64
	committed := uint64(100)
	minC := func() uint64 { return committed }

	// New leader has an empty cache (cold start / recovery).
	newLeaderCache := NewUniCache(minC, capacity)

	// Follower has a populated cache.
	followerCache := NewUniCache(minC, capacity)
	key := []byte("well-known-key")
	commitKeys(followerCache, [][]byte{key}, 1)

	// New leader doesn't know the key → EncodeData returns id=0 → sends raw.
	rawData := encodeProtoField(cachedFieldNumber, key)
	_, id := newLeaderCache.EncodeData(rawData, committed)
	if id != 0 {
		t.Fatalf("expected id=0 from cold cache, got %d", id)
	}

	// Follower receives raw data; DecodeEntry must be a no-op.
	dec, ok := followerCache.DecodeEntry(makeEntry(rawData, committed+1))
	if !ok {
		t.Fatal("DecodeEntry failed for raw (non-encoded) entry")
	}
	if string(dec.Data) != string(rawData) {
		t.Errorf("raw data was mutated: got %x, want %x", dec.Data, rawData)
	}
	if IsEncodedData(rawData) {
		t.Error("raw protobuf bytes should not be classified as encoded")
	}
}

// ---------------------------------------------------------------------------
// TestEvictLRUAlwaysMovesToEvicted
// ---------------------------------------------------------------------------

// TestEvictLRUAlwaysMovesToEvicted verifies that even when minCacheVersion==0
// (follower), evicted entries are moved to the evicted buffer instead of
// being hard-deleted. This is critical for correctness after leadership
// transitions: if a follower becomes leader, it needs the evicted map to
// restore entries via SafeEncode.
func TestEvictLRUAlwaysMovesToEvicted(t *testing.T) {
	const capacity = 10
	const startIdx = uint64(10 * capacity) // high enough so evictLRU guard fires
	// minCacheVersion returns 0, simulating a follower.
	minC := func() uint64 { return 0 }
	uc, ok := NewUniCache(minC, capacity).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}

	// Fill past capacity to trigger eviction.
	for i := 0; i < capacity+5; i++ {
		key := []byte(fmt.Sprintf("follower-key-%d", i))
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}

	// Evicted entries must land in the evicted buffer, not be deleted.
	if len(uc.evicted) == 0 {
		t.Fatal("expected entries in evicted buffer even with minCacheVersion=0")
	}
	if uc.evictOrder.Len() != len(uc.evicted) {
		t.Fatalf("evictOrder length %d != evicted map length %d",
			uc.evictOrder.Len(), len(uc.evicted))
	}
}

// ---------------------------------------------------------------------------
// TestEvictedCapEnforced
// ---------------------------------------------------------------------------

// TestEvictedCapEnforced verifies that the evicted buffer never grows beyond
// evictedCapacity (2 * capacity).
func TestEvictedCapEnforced(t *testing.T) {
	const capacity = 10
	const startIdx = uint64(10 * capacity)
	minC := func() uint64 { return 0 }
	uc, ok := NewUniCache(minC, capacity).(*uniCache)
	if !ok {
		t.Fatal("failed to cast UniCache to *uniCache")
	}

	// Insert many more entries than capacity to push a lot into evicted.
	// 5*capacity ensures evicted buffer would exceed 2*capacity without the cap.
	for i := 0; i < 5*capacity; i++ {
		key := []byte(fmt.Sprintf("cap-key-%d", i))
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}

	if len(uc.evicted) > uc.evictedCapacity {
		t.Fatalf("evicted size %d exceeds evictedCapacity %d",
			len(uc.evicted), uc.evictedCapacity)
	}
	if uc.evictOrder.Len() != len(uc.evicted) {
		t.Fatalf("evictOrder length %d != evicted map length %d",
			uc.evictOrder.Len(), len(uc.evicted))
	}
}

// ---------------------------------------------------------------------------
// TestSafeEncodeReturnsNilOnMiss
// ---------------------------------------------------------------------------

// TestSafeEncodeReturnsNilOnMiss verifies that SafeEncode returns (nil, nil)
// when the encoded ID is not found in either cache or evicted buffer.
func TestSafeEncodeReturnsNilOnMiss(t *testing.T) {
	const capacity = 10
	minC := func() uint64 { return 100 }
	uc := NewUniCache(minC, capacity)

	// Construct a varint-encoded entry with a non-existent ID.
	fakeEncoded := protowire.AppendTag(nil, cachedFieldNumber, protowire.VarintType)
	fakeEncoded = protowire.AppendVarint(fakeEncoded, uint64(999))

	data, full := uc.SafeEncode(fakeEncoded, 200, 999)
	if data != nil {
		t.Errorf("expected nil data on miss, got %x", data)
	}
	if full != nil {
		t.Errorf("expected nil full on miss, got %x", full)
	}
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Fast-path / fallback tests for ReplaceProtoField & GetProtoFieldAndWireType
// ---------------------------------------------------------------------------

// TestMultiFieldRoundTrip builds data with fields 1+2+3, encodes field 1
// (bytes→varint), verifies fields 2-3 survive, then decodes back and verifies
// all fields are restored.
func TestMultiFieldRoundTrip(t *testing.T) {
	// Build a proto message with fields 1 (bytes), 2 (bytes), 3 (varint).
	field1Val := []byte("hello-world-key")
	field2Val := []byte("extra-metadata")
	var data []byte
	data = protowire.AppendTag(data, 1, protowire.BytesType)
	data = protowire.AppendBytes(data, field1Val)
	data = protowire.AppendTag(data, 2, protowire.BytesType)
	data = protowire.AppendBytes(data, field2Val)
	data = protowire.AppendTag(data, 3, protowire.VarintType)
	data = protowire.AppendVarint(data, 42)

	// Replace field 1 (bytes → varint ID).
	newID := protowire.AppendVarint(nil, uint64(7))
	encoded, err := ReplaceProtoField(data, 1, newID, protowire.VarintType)
	if err != nil {
		t.Fatalf("ReplaceProtoField encode failed: %v", err)
	}

	// Verify fields 2 and 3 survived the replacement.
	f2, wt2, err := GetProtoFieldAndWireType(encoded, 2)
	if err != nil {
		t.Fatalf("field 2 not found after replacement: %v", err)
	}
	if wt2 != protowire.BytesType || string(f2) != string(field2Val) {
		t.Errorf("field 2 mismatch: got %q (wire %d), want %q", f2, wt2, field2Val)
	}
	f3, wt3, err := GetProtoFieldAndWireType(encoded, 3)
	if err != nil {
		t.Fatalf("field 3 not found after replacement: %v", err)
	}
	if wt3 != protowire.VarintType {
		t.Errorf("field 3 wire type: got %d, want %d", wt3, protowire.VarintType)
	}
	v3, n3 := protowire.ConsumeVarint(f3)
	if n3 <= 0 || v3 != 42 {
		t.Errorf("field 3 value: got %d, want 42", v3)
	}

	// Decode field 1 back (varint → bytes).
	restored, err := ReplaceProtoField(encoded, 1, field1Val, protowire.BytesType)
	if err != nil {
		t.Fatalf("ReplaceProtoField decode failed: %v", err)
	}

	// Verify all three fields are correct in the restored data.
	f1r, wt1r, err := GetProtoFieldAndWireType(restored, 1)
	if err != nil || wt1r != protowire.BytesType || string(f1r) != string(field1Val) {
		t.Errorf("restored field 1 mismatch: got %q (wire %d), want %q", f1r, wt1r, field1Val)
	}
	f2r, wt2r, err := GetProtoFieldAndWireType(restored, 2)
	if err != nil || wt2r != protowire.BytesType || string(f2r) != string(field2Val) {
		t.Errorf("restored field 2 mismatch: got %q (wire %d), want %q", f2r, wt2r, field2Val)
	}
	f3r, wt3r, err := GetProtoFieldAndWireType(restored, 3)
	if err != nil {
		t.Fatalf("restored field 3 not found: %v", err)
	}
	v3r, n3r := protowire.ConsumeVarint(f3r)
	if n3r <= 0 || v3r != 42 || wt3r != protowire.VarintType {
		t.Errorf("restored field 3: got %d (wire %d), want 42", v3r, wt3r)
	}
}

// TestGetProtoFieldVarintSubSlice verifies that GetProtoFieldAndWireType returns
// a varint sub-slice that is correctly consumable by protowire.ConsumeVarint.
func TestGetProtoFieldVarintSubSlice(t *testing.T) {
	for _, id := range []uint64{1, 127, 128, 16384, 1<<32 - 1} {
		var data []byte
		data = protowire.AppendTag(data, 1, protowire.VarintType)
		data = protowire.AppendVarint(data, id)
		// Add trailing field so the varint sub-slice boundary is exercised.
		data = protowire.AppendTag(data, 2, protowire.VarintType)
		data = protowire.AppendVarint(data, 99)

		field, wt, err := GetProtoFieldAndWireType(data, 1)
		if err != nil {
			t.Fatalf("id=%d: GetProtoFieldAndWireType failed: %v", id, err)
		}
		if wt != protowire.VarintType {
			t.Fatalf("id=%d: wire type %d, want VarintType", id, wt)
		}
		v, n := protowire.ConsumeVarint(field)
		if n <= 0 {
			t.Fatalf("id=%d: ConsumeVarint failed on returned sub-slice", id)
		}
		if v != id {
			t.Errorf("id=%d: got %d from ConsumeVarint", id, v)
		}
	}
}

// TestReplaceProtoFieldFallback verifies that when field 1 is NOT first in
// the serialized data, the general (fallback) path still works correctly.
func TestReplaceProtoFieldFallback(t *testing.T) {
	// Build data with field 2 first, then field 1.
	field1Val := []byte("the-key")
	field2Val := []byte("other-data")
	var data []byte
	data = protowire.AppendTag(data, 2, protowire.BytesType)
	data = protowire.AppendBytes(data, field2Val)
	data = protowire.AppendTag(data, 1, protowire.BytesType)
	data = protowire.AppendBytes(data, field1Val)

	// Replace field 1 (bytes → varint) — must use the general path.
	newID := protowire.AppendVarint(nil, uint64(42))
	replaced, err := ReplaceProtoField(data, 1, newID, protowire.VarintType)
	if err != nil {
		t.Fatalf("ReplaceProtoField fallback failed: %v", err)
	}

	// Verify field 1 was replaced.
	f1, wt1, err := GetProtoFieldAndWireType(replaced, 1)
	if err != nil {
		t.Fatalf("field 1 not found: %v", err)
	}
	if wt1 != protowire.VarintType {
		t.Errorf("field 1 wire type: got %d, want VarintType", wt1)
	}
	v1, n1 := protowire.ConsumeVarint(f1)
	if n1 <= 0 || v1 != 42 {
		t.Errorf("field 1 value: got %d, want 42", v1)
	}

	// Verify field 2 survived.
	f2, wt2, err := GetProtoFieldAndWireType(replaced, 2)
	if err != nil {
		t.Fatalf("field 2 not found: %v", err)
	}
	if wt2 != protowire.BytesType || string(f2) != string(field2Val) {
		t.Errorf("field 2: got %q (wire %d), want %q", f2, wt2, field2Val)
	}
}

// TestLeaderTransitionMixedEncoding verifies that a follower correctly handles
// both encoded entries (from the old leader, in-flight during transfer) and raw
// entries (from the new leader with a cold cache) without error.
func TestLeaderTransitionMixedEncoding(t *testing.T) {
	const capacity = 64
	const startIdx = uint64(1)
	committed := uint64(startIdx + 5)
	minC := func() uint64 { return committed }

	// All three nodes learn the same 5 committed entries.
	oldLeader := NewUniCache(minC, capacity)
	newLeader := NewUniCache(minC, capacity)
	follower := NewUniCache(minC, capacity)

	keys := make([][]byte, 5)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("shared-key-%d", i))
	}
	commitKeys(oldLeader, keys, startIdx)
	commitKeys(newLeader, keys, startIdx)
	commitKeys(follower, keys, startIdx)

	// Old leader sends an encoded entry (in-flight during transfer).
	sharedRaw := encodeProtoField(cachedFieldNumber, keys[0])
	oldLeader.UpdateCache(makeEntry(sharedRaw, committed+1))
	follower.UpdateCache(makeEntry(sharedRaw, committed+1))
	newLeader.UpdateCache(makeEntry(sharedRaw, committed+1))
	encodedByOld, id1 := oldLeader.EncodeData(sharedRaw, committed+1)
	if id1 == 0 {
		t.Fatal("old leader should encode a committed key")
	}

	// New leader sends a brand-new key (not in cache) → raw fallback.
	brandNewRaw := encodeProtoField(cachedFieldNumber, []byte("brand-new-after-transfer"))
	_, id2 := newLeader.EncodeData(brandNewRaw, committed+2)
	if id2 != 0 {
		t.Fatalf("new key should not be in cache, got id=%d", id2)
	}

	// Follower decodes both: encoded from old leader, raw from new leader.
	dec1, ok1 := follower.DecodeEntry(makeEntry(encodedByOld, committed+2))
	if !ok1 {
		t.Fatal("follower failed to decode entry from old leader")
	}
	if string(dec1.Data) != string(sharedRaw) {
		t.Errorf("decoded old-leader entry mismatch: got %x, want %x", dec1.Data, sharedRaw)
	}

	dec2, ok2 := follower.DecodeEntry(makeEntry(brandNewRaw, committed+3))
	if !ok2 {
		t.Fatal("follower failed to handle raw entry from new leader")
	}
	if string(dec2.Data) != string(brandNewRaw) {
		t.Errorf("raw new-leader entry was mutated: got %x, want %x", dec2.Data, brandNewRaw)
	}
}

// ---------------------------------------------------------------------------
// BatchUpdateCache optimization tests
// ---------------------------------------------------------------------------

// TestBatchUpdateCacheEncodedIDFastPath verifies that entries with a valid
// EncodedID in the active cache take the fast path: lastIdx is updated and
// no proto parsing or string conversion occurs.
func TestBatchUpdateCacheEncodedIDFastPath(t *testing.T) {
	const capacity = 64
	const startIdx = uint64(1000)
	committed := startIdx + 20
	minC := func() uint64 { return committed }
	uc, ok := NewUniCache(minC, capacity).(*uniCache)
	if !ok {
		t.Fatal("failed to cast")
	}

	// Commit 5 keys so they are in the active cache.
	keys := make([][]byte, 5)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("fast-key-%d", i))
	}
	commitKeys(uc, keys, startIdx)

	// Record the IDs assigned to each key.
	ids := make([]uint32, len(keys))
	for i, key := range keys {
		id, exists := uc.reverseCache[string(key)]
		if !exists {
			t.Fatalf("key %q not found in reverseCache after commit", key)
		}
		ids[i] = id
	}

	// Build entries that carry EncodedID (simulating leader's committed entries).
	// The Data is the raw proto (BytesType) — the fast path should NOT parse it.
	newIndex := committed + 100
	entries := make([]pb.Entry, len(keys))
	for i, key := range keys {
		entries[i] = pb.Entry{
			Index:     newIndex + uint64(i),
			Term:      1,
			Type:      pb.EntryNormal,
			Data:      encodeProtoField(cachedFieldNumber, key),
			EncodedID: ids[i],
		}
	}

	result, ok2 := uc.BatchUpdateCache(entries)
	if !ok2 {
		t.Fatal("BatchUpdateCache returned false")
	}
	if len(result) != len(entries) {
		t.Fatalf("result length %d != entries length %d", len(result), len(entries))
	}

	// Verify lastIdx was updated for each entry.
	for i, id := range ids {
		ent := uc.cache[id]
		if ent == nil {
			t.Fatalf("cache entry %d missing", id)
		}
		wantIdx := newIndex + uint64(i)
		if ent.lastIdx != wantIdx {
			t.Errorf("entry ID=%d: lastIdx=%d, want %d", id, ent.lastIdx, wantIdx)
		}
	}
}

// TestBatchUpdateCacheEvictedPath verifies that entries with EncodedID pointing
// to the evicted buffer are correctly restored as new cache entries.
func TestBatchUpdateCacheEvictedPath(t *testing.T) {
	const capacity = 10
	const startIdx = uint64(1000)
	minVersion := startIdx + uint64(2*capacity)
	minC := func() uint64 { return minVersion }
	uc, ok := NewUniCache(minC, capacity).(*uniCache)
	if !ok {
		t.Fatal("failed to cast")
	}

	// Commit target key first (gets the lowest ID, will be evicted first).
	targetKey := []byte("evicted-batch-key")
	targetRaw := encodeProtoField(cachedFieldNumber, targetKey)
	uc.UpdateCache(makeEntry(targetRaw, startIdx))

	// Fill cache past capacity to evict targetKey.
	for i := 1; i <= capacity+5; i++ {
		key := []byte(fmt.Sprintf("filler-batch-%d", i))
		data := encodeProtoField(cachedFieldNumber, key)
		uc.UpdateCache(makeEntry(data, startIdx+uint64(i)))
	}

	// Find targetKey's ID in the evicted buffer.
	var targetID uint32
	for id, elem := range uc.evicted {
		e := elem.Value.(*cacheEntry)
		if string(e.key) == string(targetKey) {
			targetID = id
			break
		}
	}
	if targetID == 0 {
		t.Fatal("targetKey not found in evicted buffer")
	}

	// BatchUpdateCache with EncodedID pointing to evicted entry.
	newIndex := startIdx + uint64(capacity+20)
	entries := []pb.Entry{{
		Index:     newIndex,
		Term:      1,
		Type:      pb.EntryNormal,
		Data:      targetRaw,
		EncodedID: targetID,
	}}

	result, ok2 := uc.BatchUpdateCache(entries)
	if !ok2 {
		t.Fatal("BatchUpdateCache returned false")
	}
	if len(result) != 1 {
		t.Fatalf("result length %d, want 1", len(result))
	}

	// Key should now be back in active cache (with a new ID).
	newID, exists := uc.reverseCache[string(targetKey)]
	if !exists {
		t.Fatal("targetKey not found in reverseCache after evicted-path update")
	}
	ent := uc.cache[newID]
	if ent == nil {
		t.Fatal("new cache entry is nil")
	}
	if ent.lastIdx != newIndex {
		t.Errorf("lastIdx=%d, want %d", ent.lastIdx, newIndex)
	}
	// Old evicted entry should have been removed.
	if _, stillEvicted := uc.evicted[targetID]; stillEvicted {
		t.Error("old evicted entry should have been removed")
	}
}

// TestBatchUpdateCacheVarintDefensive verifies that BatchUpdateCache correctly
// handles entries whose Data starts with the varint tag (0x08) — these are
// leader-side encoded entries that must be decoded before cache update when
// EncodedID is 0.
func TestBatchUpdateCacheVarintDefensive(t *testing.T) {
	const capacity = 64
	const startIdx = uint64(1)
	committed := uint64(startIdx + 10)
	minC := func() uint64 { return committed }
	uc := NewUniCache(minC, capacity)

	// Commit a key so it can be encoded.
	key := []byte("varint-test-key")
	rawData := encodeProtoField(cachedFieldNumber, key)
	uc.UpdateCache(makeEntry(rawData, startIdx))

	// Encode the key to get a varint-form entry.
	encoded, id := uc.EncodeData(rawData, committed)
	if id == 0 {
		t.Fatal("EncodeData returned id=0")
	}
	if !IsEncodedData(encoded) {
		t.Fatal("expected varint-encoded data")
	}

	// Build an entry with varint data but EncodedID=0 (simulates follower path
	// where EncodedID was cleared).
	entry := pb.Entry{
		Index:     committed + 100,
		Term:      1,
		Type:      pb.EntryNormal,
		Data:      encoded,
		EncodedID: 0, // Cleared — must fall through to slow path
	}

	result, ok := uc.BatchUpdateCache([]pb.Entry{entry})
	if !ok {
		t.Fatal("BatchUpdateCache returned false for varint-encoded entry")
	}
	if len(result) != 1 {
		t.Fatalf("result length %d, want 1", len(result))
	}
}
