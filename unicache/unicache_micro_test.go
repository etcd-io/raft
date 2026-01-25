// microbenchmark_test.go
package unicache_test

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	pb "go.etcd.io/raft/v3/raftpb"
	ucpkg "go.etcd.io/raft/v3/unicache"
)

type benchCase struct {
	name     string
	sizeB    int
	hitRatio int
	N        int
}

func cases() []benchCase {
	const M = 16_384
	return []benchCase{
		{"1KB_hit0", 1 << 10, 0, M},
		{"1KB_hit50", 1 << 10, 50, M},
		{"1KB_hit100", 1 << 10, 100, M},

		{"2KB_hit0", 2 << 10, 0, M},
		{"2KB_hit50", 2 << 10, 50, M},
		{"2KB_hit100", 2 << 10, 100, M},

		{"4KB_hit0", 4 << 10, 0, M},
		{"4KB_hit50", 4 << 10, 50, M},
		{"4KB_hit100", 4 << 10, 100, M},
	}
}

func serializeLenDelimField1(val []byte) []byte {
	tag := uint64(1<<3 | int(protowire.BytesType))
	buf := make([]byte, 0, 10+10+len(val))
	buf = protowire.AppendVarint(buf, tag)
	buf = protowire.AppendVarint(buf, uint64(len(val)))
	buf = append(buf, val...)
	return buf
}

func primeCache(uc ucpkg.UniCache, K, sizeB int) (hotSerialized [][]byte) {
	hotSerialized = make([][]byte, K)
	for i := 0; i < K; i++ {
		v := make([]byte, sizeB)
		_, _ = crand.Read(v)
		data := serializeLenDelimField1(v)
		hotSerialized[i] = data

		entry := pb.Entry{Index: uint64(1000 + i), Data: data}
		_, _ = uc.UpdateCache(entry)
	}
	return hotSerialized
}

func makeDataset(n, sizeB, hitRatio int, hotVals [][]byte) [][]byte {
	if hitRatio < 0 {
		hitRatio = 0
	}
	if hitRatio > 100 {
		hitRatio = 100
	}
	payloads := make([][]byte, n)
	r := rand.New(rand.NewSource(1))

	newMiss := func() []byte {
		v := make([]byte, sizeB)
		_, _ = crand.Read(v)
		return serializeLenDelimField1(v)
	}

	for i := 0; i < n; i++ {
		useHit := r.Intn(100) < hitRatio && len(hotVals) > 0
		if useHit {
			payloads[i] = hotVals[r.Intn(len(hotVals))]
		} else {
			payloads[i] = newMiss()
		}
	}
	return payloads
}

func newWideOpenUniCache() ucpkg.UniCache {
	minCacheVersion := func() uint64 { return 1 << 62 }
	const huge = 1 << 30
	return ucpkg.NewUniCache(minCacheVersion, huge)
}

var headerPrinted sync.Map

func printHeaderOnce(title string) {
	if _, loaded := headerPrinted.LoadOrStore(title, true); !loaded {
		fmt.Printf("\n== %s Benchmarks ==\n", title)
		fmt.Println("Name                                                               Number Operarions                   ns/op                  MB/s                    B/op           allocs/op")
		fmt.Println(strings.Repeat("-", 150))
	}
}

func BenchmarkEncodeData(b *testing.B) {
	printHeaderOnce("Encode")
	for _, c := range cases() {
		b.Run(c.name, func(b *testing.B) {
			uc := newWideOpenUniCache()
			hot := primeCache(uc, 64, c.sizeB)
			payloads := makeDataset(c.N, c.sizeB, c.hitRatio, hot)

			b.SetBytes(int64(c.sizeB))
			b.ReportAllocs()
			b.ResetTimer()

			var sink uint32
			idx := 0
			for i := 0; i < b.N; i++ {
				p := payloads[idx]
				_, id := uc.EncodeData(p, 0)
				sink += id
				idx++
				if idx == len(payloads) {
					idx = 0
				}
			}
			if sink == 0xdead {
				b.Log(sink)
			}
		})
	}
}

func BenchmarkDecodeEntry(b *testing.B) {
	printHeaderOnce("Decode")
	for _, c := range cases() {
		b.Run(c.name, func(b *testing.B) {
			uc := newWideOpenUniCache()
			hot := primeCache(uc, 64, c.sizeB)
			payloads := makeDataset(c.N, c.sizeB, c.hitRatio, hot)

			// Pre-encode once to place IDs in hit entries.
			encoded := make([][]byte, len(payloads))
			for i, p := range payloads {
				q, _ := uc.EncodeData(p, 0)
				encoded[i] = q
			}
			entries := make([]pb.Entry, len(encoded))
			for i := range encoded {
				entries[i] = pb.Entry{Index: uint64(20_000 + i), Data: encoded[i]}
			}

			b.SetBytes(int64(c.sizeB))
			b.ReportAllocs()
			b.ResetTimer()

			sum := 0
			idx := 0
			for i := 0; i < b.N; i++ {
				e := entries[idx]
				e2, ok := uc.DecodeEntry(e)
				if ok {
					sum += int(e2.Data[0])
				} else {
					sum++
				}
				idx++
				if idx == len(entries) {
					idx = 0
				}
			}
			if sum == 0 {
				b.Fatal("unreachable")
			}
		})
	}
}

func BenchmarkUpdateCache(b *testing.B) {
	printHeaderOnce("UpdateCache")
	for _, c := range cases() {
		b.Run(c.name, func(b *testing.B) {
			uc := newWideOpenUniCache()
			// Generate a mix of keys
			hot := primeCache(uc, 64, c.sizeB)
			payloads := makeDataset(c.N, c.sizeB, c.hitRatio, hot)

			entries := make([]pb.Entry, len(payloads))
			for i := range payloads {
				entries[i] = pb.Entry{Index: uint64(30_000 + i), Data: payloads[i]}
			}

			b.SetBytes(int64(c.sizeB))
			b.ReportAllocs()
			b.ResetTimer()

			idx := 0
			for i := 0; i < b.N; i++ {
				// We don't care about the return value here, just the work
				uc.UpdateCache(entries[idx])
				idx++
				if idx == len(entries) {
					idx = 0
				}
			}
		})
	}
}

func BenchmarkSafeEncode(b *testing.B) {
	printHeaderOnce("SafeEncode")
	for _, c := range cases() {
		b.Run(c.name, func(b *testing.B) {
			uc := newWideOpenUniCache()
			hot := primeCache(uc, 64, c.sizeB)
			payloads := makeDataset(c.N, c.sizeB, c.hitRatio, hot)

			// Pre-calculate the "Encoded" state.
			// In the wild, this data comes from the Leader's 'EncodeData'.
			type prepared struct {
				data []byte
				id   uint32
			}
			inputs := make([]prepared, len(payloads))

			for i, p := range payloads {
				// We act as the leader encoding the data first
				encData, id := uc.EncodeData(p, 0)
				inputs[i] = prepared{data: encData, id: id}
			}

			// We assume the follower is up to date (safe window)
			// effectively testing the "Happy Path" overhead of proto reconstruction.
			const safeAppendIdx = 2000

			b.SetBytes(int64(c.sizeB))
			b.ReportAllocs()
			b.ResetTimer()

			idx := 0
			var sinkLen int
			for i := 0; i < b.N; i++ {
				in := inputs[idx]

				// Benchmark the decision logic + reconstruction cost
				d1, d2 := uc.SafeEncode(in.data, safeAppendIdx, in.id)

				sinkLen += len(d1) + len(d2)
				idx++
				if idx == len(inputs) {
					idx = 0
				}
			}
			if sinkLen == 0 {
				b.Fatal("unreachable")
			}
		})
	}
}
