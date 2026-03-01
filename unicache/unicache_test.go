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
