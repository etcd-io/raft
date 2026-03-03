# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all unit tests
make test
# Equivalent: PASSES="unit" ./scripts/test.sh

# Run a single test or package
go test ./unicache/... -run TestName -v
go test . -run TestName -v -race

# Run microbenchmarks
go test ./unicache/... -bench=. -benchmem -run=^$

# Format check
make verify-gofmt

# Full static analysis (gofmt + dep + lint + mod-tidy + genproto)
make verify

# Lint only
golangci-lint run

# Regenerate protobuf (requires protoc v3.20.3)
make verify-genproto
```

## Architecture

This is a fork of `go.etcd.io/raft/v3` (module path: `github.com/jedeland10/raft`) with an active research branch implementing **RepliCache** — a bandwidth optimization for Raft log replication.

### Core Raft flow

The library models Raft as a pure state machine. External code drives it by:
1. Calling `Node.Tick()` periodically (abstract time)
2. Passing inbound messages via `Node.Step()`
3. Reading output batches from `Node.Ready()` and calling `Node.Advance()` after processing

The main state machine lives in `raft.go` (`raft` struct). `node.go` wraps it with a goroutine + channel-based `Node` interface; `rawnode.go` exposes the same logic without the goroutine layer (used by tests and embedders that manage their own event loops).

Key files:
- `raft.go` — leader election, log replication (`maybeSendAppend`), `appendEntry`
- `log.go` — `raftLog` struct: wraps `unstable` (in-flight) + `Storage` (persisted), holds the `uniCache` field
- `log_unstable.go` — entries not yet persisted to stable storage
- `rawnode.go` — `Ready()` constructs the output batch (committed entries, messages, snapshot)
- `tracker/progress.go` — per-follower state (`Match`, `Next`, `NextCacheId`)
- `storage.go` — `Storage` interface + `MemoryStorage` implementation

### RepliCache (branch `replicache-optimize`)

RepliCache reduces replication bandwidth by replacing repeated large keys in log entry `Data` with compact 32-bit IDs, using an LRU cache shared between leader and followers.

**Data path (leader side):**
1. `appendEntry` in `raft.go` calls `BatchSafeEncode` / `SafeEncode` to produce an encoded twin of each entry.
2. Encoded twins are buffered in `pendingBuf` (`pending_encoded.go`), mirroring `log_unstable` so `maybeSendAppend` can serve them.
3. `maybeSendAppend` fetches from `pendingBuf` first; falls back to raw storage on `ErrCompacted` or `ErrUnavailable`.

**Cache learning (commit path):**
- `BatchUpdateCache` is called on committed entries so the cache is updated only after quorum.
- `PurgeEvicted` cleans the eviction map per Algorithm 6: remove entries whose `lastIdx < max(0, minCacheVersion - capacity)`, where `minCacheVersion` = minimum committed index across followers (tracked via `MsgAppResp.Commit` → `tracker.MinCacheIdxMatch`).

**Key invariants:**
- `SafeEncode` uses `evicted` only as a restoration fallback (returns `(full, full)`), never re-encodes.
- `EncodedID == 0` means an entry was not encoded; `anyEncoded` pre-scan in `appendEntry` avoids allocation for 0% hit-rate batches.
- `rawnode.Ready()` pre-scans committed entries for `cachedFieldVarintTag` before allocating the decode slice; uses `unicache.IsEncodedData()` as the check.

**RepliCache files:**
- `unicache/unicache.go` — `UniCache` interface + `uniCache` struct (LRU + eviction map)
- `unicache/unicache_test.go` — unit tests
- `unicache/unicache_micro_test.go` — microbenchmarks (1–4 KB payloads, 0/50/100% hit ratios)
- `pending_encoded.go` — `pendingBuf`: encoded-twin buffer aligned with `log_unstable`
- `tracker/progress.go` — `NextCacheId` field on `Progress`
- `raftpb/kvstore_raft.proto` — protobuf additions for RepliCache fields

### Protobuf

Generated Go code lives in `raftpb/`. Use `make verify-genproto` to check consistency. Requires `protoc` v3.20.3 and the gogo/protobuf plugin.

### Testing conventions

- Data-driven tests use `cockroachdb/datadriven`; test case files are in `testdata/`.
- Race detector is enabled by default on amd64 (`--race`).
- Tests run with `-short -timeout=3m` in CI.
