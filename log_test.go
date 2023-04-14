// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newLogAppend(index, term uint64, terms ...uint64) logAppend {
	entries := make([]pb.Entry, len(terms))
	for i, t := range terms {
		entries[i].Index, entries[i].Term = index+1+uint64(i), t
	}
	return logAppend{index: index, term: term, entries: entries}
}

func TestFindConflict(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	for _, tt := range []struct {
		req    logAppend
		wantOk bool
		want   logAppend
	}{
		// Empty appends with no conflict.
		{req: newLogAppend(0, 0), wantOk: true, want: newLogAppend(0, 0)},
		{req: newLogAppend(1, 1), wantOk: true, want: newLogAppend(1, 1)},
		{req: newLogAppend(3, 3), wantOk: true, want: newLogAppend(3, 3)},
		// Empty appends with conflict.
		{req: newLogAppend(4, 4), wantOk: false},  // past the log size
		{req: newLogAppend(0, 10), wantOk: false}, // term mismatch
		{req: newLogAppend(1, 10), wantOk: false}, // term mismatch
		// No conflict, no new entries.
		{req: newLogAppend(0, 0, 1, 2, 3), wantOk: true, want: newLogAppend(3, 3)},
		{req: newLogAppend(0, 0, 1, 2), wantOk: true, want: newLogAppend(2, 2)},
		{req: newLogAppend(0, 0, 1), wantOk: true, want: newLogAppend(1, 1)},
		{req: newLogAppend(1, 1, 2, 3), wantOk: true, want: newLogAppend(3, 3)},
		{req: newLogAppend(1, 1, 2), wantOk: true, want: newLogAppend(2, 2)},
		{req: newLogAppend(2, 2, 3), wantOk: true, want: newLogAppend(3, 3)},
		// No conflict, but has new entries.
		{req: newLogAppend(0, 0, 1, 2, 3, 4, 4), wantOk: true, want: newLogAppend(3, 3, 4, 4)},
		{req: newLogAppend(1, 1, 2, 3, 4, 4), wantOk: true, want: newLogAppend(3, 3, 4, 4)},
		{req: newLogAppend(2, 2, 3, 4, 4), wantOk: true, want: newLogAppend(3, 3, 4, 4)},
		{req: newLogAppend(3, 3, 4, 4), wantOk: true, want: newLogAppend(3, 3, 4, 4)},
		// Conflicts with existing entries.
		{req: newLogAppend(0, 0, 4, 4), wantOk: true, want: newLogAppend(0, 0, 4, 4)},
		{req: newLogAppend(0, 0, 1, 2, 4, 8), wantOk: true, want: newLogAppend(2, 2, 4, 8)},
		{req: newLogAppend(1, 1, 1, 4, 4), wantOk: true, want: newLogAppend(1, 1, 1, 4, 4)},
		{req: newLogAppend(1, 1, 2, 8, 8), wantOk: true, want: newLogAppend(2, 2, 8, 8)},
		{req: newLogAppend(2, 2, 2, 2, 4), wantOk: true, want: newLogAppend(2, 2, 2, 2, 4)},
	} {
		t.Run("", func(t *testing.T) {
			raftLog := newLog(NewMemoryStorage(), raftLogger)
			require.True(t, raftLog.append(logAppend{entries: previousEnts}))
			req, ok := raftLog.findConflict(tt.req)
			assert.Equal(t, tt.want, req)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestFindConflictByTerm(t *testing.T) {
	ents := func(fromIndex uint64, terms []uint64) []pb.Entry {
		e := make([]pb.Entry, 0, len(terms))
		for i, term := range terms {
			e = append(e, pb.Entry{Term: term, Index: fromIndex + uint64(i)})
		}
		return e
	}
	for _, tt := range []struct {
		ents  []pb.Entry // ents[0] contains the (index, term) of the snapshot
		index uint64
		term  uint64
		want  uint64
	}{
		// Log starts from index 1.
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 100, term: 2, want: 100}, // ErrUnavailable
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 5, term: 6, want: 5},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 5, term: 5, want: 5},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 5, term: 4, want: 2},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 5, term: 2, want: 2},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 5, term: 1, want: 0},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 1, term: 2, want: 1},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 1, term: 1, want: 0},
		{ents: ents(0, []uint64{0, 2, 2, 5, 5, 5}), index: 0, term: 0, want: 0},
		// Log with compacted entries.
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 30, term: 3, want: 30}, // ErrUnavailable
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 14, term: 9, want: 14},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 14, term: 4, want: 14},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 14, term: 3, want: 12},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 14, term: 2, want: 9},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 11, term: 5, want: 11},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 10, term: 5, want: 10},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 10, term: 3, want: 10},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 10, term: 2, want: 9},
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 9, term: 2, want: 9}, // ErrCompacted
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 4, term: 2, want: 4}, // ErrCompacted
		{ents: ents(10, []uint64{3, 3, 3, 4, 4, 4}), index: 0, term: 0, want: 0}, // ErrCompacted
	} {
		t.Run("", func(t *testing.T) {
			st := NewMemoryStorage()
			require.NotEmpty(t, tt.ents)
			st.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{
				Index: tt.ents[0].Index,
				Term:  tt.ents[0].Term,
			}})
			l := newLog(st, raftLogger)
			require.True(t, l.append(logAppend{
				index:   tt.ents[0].Index,
				term:    tt.ents[0].Term,
				entries: tt.ents[1:],
			}))

			index, term := l.findConflictByTerm(tt.index, tt.term)
			require.Equal(t, tt.want, index)
			wantTerm, err := l.term(index)
			wantTerm = l.zeroTermOnOutOfBounds(wantTerm, err)
			require.Equal(t, wantTerm, term)
		})
	}
}

func TestIsUpToDate(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	raftLog := newLog(NewMemoryStorage(), raftLogger)
	raftLog.append(logAppend{entries: previousEnts})
	tests := []struct {
		lastIndex uint64
		term      uint64
		wUpToDate bool
	}{
		// greater term, ignore lastIndex
		{raftLog.lastIndex() - 1, 4, true},
		{raftLog.lastIndex(), 4, true},
		{raftLog.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.lastIndex() - 1, 2, false},
		{raftLog.lastIndex(), 2, false},
		{raftLog.lastIndex() + 1, 2, false},
		// equal term, equal or lager lastIndex wins
		{raftLog.lastIndex() - 1, 3, false},
		{raftLog.lastIndex(), 3, true},
		{raftLog.lastIndex() + 1, 3, true},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, tt.wUpToDate, raftLog.isUpToDate(tt.lastIndex, tt.term))
		})
	}
}

func TestAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	tests := []struct {
		req       logAppend
		windex    uint64
		wents     []pb.Entry
		wunstable uint64
	}{
		{
			newLogAppend(0, 0),
			2,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			3,
		},
		{
			newLogAppend(2, 2, 2),
			3,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}},
			3,
		},
		// conflicts with index 1
		{
			newLogAppend(0, 0, 2),
			1,
			[]pb.Entry{{Index: 1, Term: 2}},
			1,
		},
		// conflicts with index 2
		{
			newLogAppend(1, 1, 3, 3),
			3,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}, {Index: 3, Term: 3}},
			2,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			storage.Append(previousEnts)
			raftLog := newLog(storage, raftLogger)
			require.True(t, raftLog.append(tt.req))
			require.Equal(t, tt.windex, raftLog.lastIndex())
			g, err := raftLog.entries(1, noLimit)
			require.NoError(t, err)
			require.Equal(t, mustLogRange(t, tt.wents), g)
			require.Equal(t, tt.wunstable, raftLog.unstable.offset)
		})
	}
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
//  1. If an existing entry conflicts with a new one (same index
//     but different terms), delete the existing entry and all that
//     follow it
//     2.Append any new entries not already in the log
//
// If the given (index, term) does not match with the existing log:
//
//	return false
func TestLogMaybeAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	lastindex := uint64(3)
	lastterm := uint64(3)
	commit := uint64(1)

	tests := []struct {
		req       logAppend
		committed uint64

		wlasti  uint64
		wappend bool
		wcommit uint64
		wpanic  bool
	}{
		// not match: term is different
		{
			newLogAppend(lastindex, lastterm-1, 4), lastindex,
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			newLogAppend(lastindex+1, lastterm, 4), lastindex,
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			newLogAppend(lastindex, lastterm), lastindex,
			lastindex, true, lastindex, false,
		},
		{
			newLogAppend(lastindex, lastterm), lastindex + 1,
			lastindex, true, lastindex, false, // do not increase commit higher than lastnewi
		},
		{
			newLogAppend(lastindex, lastterm), lastindex - 1,
			lastindex, true, lastindex - 1, false, // commit up to the commit in the message
		},
		{
			newLogAppend(lastindex, lastterm), 0,
			lastindex, true, commit, false, // commit do not decrease
		},
		{
			newLogAppend(0, 0), lastindex,
			0, true, commit, false, // commit do not decrease
		},
		{
			newLogAppend(lastindex, lastterm, 4), lastindex,
			lastindex + 1, true, lastindex, false,
		},
		{
			newLogAppend(lastindex, lastterm, 4), lastindex + 1,
			lastindex + 1, true, lastindex + 1, false,
		},
		{
			newLogAppend(lastindex, lastterm, 4), lastindex + 2,
			lastindex + 1, true, lastindex + 1, false, // do not increase commit higher than lastnewi
		},
		{
			newLogAppend(lastindex, lastterm, 4, 4), lastindex + 2,
			lastindex + 2, true, lastindex + 2, false,
		},
		// match with the entry in the middle
		{
			newLogAppend(lastindex-1, lastterm-1, 4), lastindex,
			lastindex, true, lastindex, false,
		},
		{
			newLogAppend(lastindex-2, lastterm-2, 4), lastindex,
			lastindex - 1, true, lastindex - 1, false,
		},
		{
			newLogAppend(lastindex-3, lastterm-3, 4), lastindex,
			lastindex - 2, true, lastindex - 2, true, // conflict with existing committed entry
		},
		{
			newLogAppend(lastindex-2, lastterm-2, 4, 4), lastindex,
			lastindex, true, lastindex, false,
		},
	}

	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage(), raftLogger)
		require.True(t, raftLog.append(logAppend{entries: previousEnts}))
		raftLog.committed = commit

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			gappend := raftLog.maybeAppend(tt.req, tt.committed)
			require.Equal(t, tt.wappend, gappend)
			if gappend {
				require.Equal(t, tt.wlasti, tt.req.lastIndex())
			}
			require.Equal(t, tt.wcommit, raftLog.committed)
			if gappend && len(tt.req.entries) != 0 {
				gents, err := raftLog.slice(raftLog.lastIndex()-uint64(len(tt.req.entries))+1, raftLog.lastIndex()+1, noLimit)
				require.NoError(t, err)
				require.Equal(t, tt.req.entries, gents)
			}
		})
	}
}

// TestCompactionSideEffects ensures that all the log related functionality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	var i uint64
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	lastIndex := uint64(1000)
	unstableIndex := uint64(750)
	lastTerm := lastIndex
	storage := NewMemoryStorage()
	for i = 1; i <= unstableIndex; i++ {
		storage.Append([]pb.Entry{{Term: i, Index: i}})
	}
	raftLog := newLog(storage, raftLogger)
	for i = unstableIndex; i < lastIndex; i++ {
		require.True(t, raftLog.append(newLogAppend(i, i, i+1)))
	}

	require.True(t, raftLog.maybeCommit(lastIndex, lastTerm))
	raftLog.appliedTo(raftLog.committed, 0 /* size */)

	offset := uint64(500)
	storage.Compact(offset)
	require.Equal(t, lastIndex, raftLog.lastIndex())

	for j := offset; j <= raftLog.lastIndex(); j++ {
		require.Equal(t, j, mustTerm(raftLog.term(j)))
	}

	for j := offset; j <= raftLog.lastIndex(); j++ {
		require.True(t, raftLog.matchTerm(j, j))
	}

	unstableEnts := raftLog.nextUnstableEnts()
	require.Equal(t, 250, len(unstableEnts))
	require.Equal(t, uint64(751), unstableEnts[0].Index)

	prev := raftLog.lastIndex()
	require.True(t, raftLog.append(newLogAppend(raftLog.lastIndex(), raftLog.lastTerm(), raftLog.lastTerm()+1)))
	require.Equal(t, prev+1, raftLog.lastIndex())

	ents, err := raftLog.entries(raftLog.lastIndex(), noLimit)
	require.NoError(t, err)
	require.Equal(t, 1, len(ents))
}

func TestHasNextCommittedEnts(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied       uint64
		applying      uint64
		allowUnstable bool
		paused        bool
		snap          bool
		whasNext      bool
	}{
		{applied: 3, applying: 3, allowUnstable: true, whasNext: true},
		{applied: 3, applying: 4, allowUnstable: true, whasNext: true},
		{applied: 3, applying: 5, allowUnstable: true, whasNext: false},
		{applied: 4, applying: 4, allowUnstable: true, whasNext: true},
		{applied: 4, applying: 5, allowUnstable: true, whasNext: false},
		{applied: 5, applying: 5, allowUnstable: true, whasNext: false},
		// Don't allow unstable entries.
		{applied: 3, applying: 3, allowUnstable: false, whasNext: true},
		{applied: 3, applying: 4, allowUnstable: false, whasNext: false},
		{applied: 3, applying: 5, allowUnstable: false, whasNext: false},
		{applied: 4, applying: 4, allowUnstable: false, whasNext: false},
		{applied: 4, applying: 5, allowUnstable: false, whasNext: false},
		{applied: 5, applying: 5, allowUnstable: false, whasNext: false},
		// Paused.
		{applied: 3, applying: 3, allowUnstable: true, paused: true, whasNext: false},
		// With snapshot.
		{applied: 3, applying: 3, allowUnstable: true, snap: true, whasNext: false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLog(storage, raftLogger)
			require.True(t, raftLog.append(
				logAppend{index: snap.Metadata.Index, term: snap.Metadata.Term, entries: ents}))
			raftLog.stableTo(4, 1)
			raftLog.maybeCommit(5, 1)
			raftLog.appliedTo(tt.applied, 0 /* size */)
			raftLog.acceptApplying(tt.applying, 0 /* size */, tt.allowUnstable)
			raftLog.applyingEntsPaused = tt.paused
			if tt.snap {
				newSnap := snap
				newSnap.Metadata.Index++
				raftLog.restore(newSnap)
			}
			require.Equal(t, tt.whasNext, raftLog.hasNextCommittedEnts(tt.allowUnstable))
		})
	}
}

func TestNextCommittedEnts(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied       uint64
		applying      uint64
		allowUnstable bool
		paused        bool
		snap          bool
		wents         []pb.Entry
	}{
		{applied: 3, applying: 3, allowUnstable: true, wents: ents[:2]},
		{applied: 3, applying: 4, allowUnstable: true, wents: ents[1:2]},
		{applied: 3, applying: 5, allowUnstable: true, wents: nil},
		{applied: 4, applying: 4, allowUnstable: true, wents: ents[1:2]},
		{applied: 4, applying: 5, allowUnstable: true, wents: nil},
		{applied: 5, applying: 5, allowUnstable: true, wents: nil},
		// Don't allow unstable entries.
		{applied: 3, applying: 3, allowUnstable: false, wents: ents[:1]},
		{applied: 3, applying: 4, allowUnstable: false, wents: nil},
		{applied: 3, applying: 5, allowUnstable: false, wents: nil},
		{applied: 4, applying: 4, allowUnstable: false, wents: nil},
		{applied: 4, applying: 5, allowUnstable: false, wents: nil},
		{applied: 5, applying: 5, allowUnstable: false, wents: nil},
		// Paused.
		{applied: 3, applying: 3, allowUnstable: true, paused: true, wents: nil},
		// With snapshot.
		{applied: 3, applying: 3, allowUnstable: true, snap: true, wents: nil},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLog(storage, raftLogger)
			require.True(t, raftLog.append(
				logAppend{index: snap.Metadata.Index, term: snap.Metadata.Term, entries: ents}))
			raftLog.stableTo(4, 1)
			raftLog.maybeCommit(5, 1)
			raftLog.appliedTo(tt.applied, 0 /* size */)
			raftLog.acceptApplying(tt.applying, 0 /* size */, tt.allowUnstable)
			raftLog.applyingEntsPaused = tt.paused
			if tt.snap {
				newSnap := snap
				newSnap.Metadata.Index++
				raftLog.restore(newSnap)
			}
			require.Equal(t, mustLogRange(t, tt.wents), raftLog.nextCommittedEnts(tt.allowUnstable))
		})
	}
}

func TestAcceptApplying(t *testing.T) {
	maxSize := entryEncodingSize(100)
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		index         uint64
		allowUnstable bool
		size          entryEncodingSize
		wpaused       bool
	}{
		{index: 3, allowUnstable: true, size: maxSize - 1, wpaused: true},
		{index: 3, allowUnstable: true, size: maxSize, wpaused: true},
		{index: 3, allowUnstable: true, size: maxSize + 1, wpaused: true},
		{index: 4, allowUnstable: true, size: maxSize - 1, wpaused: true},
		{index: 4, allowUnstable: true, size: maxSize, wpaused: true},
		{index: 4, allowUnstable: true, size: maxSize + 1, wpaused: true},
		{index: 5, allowUnstable: true, size: maxSize - 1, wpaused: false},
		{index: 5, allowUnstable: true, size: maxSize, wpaused: true},
		{index: 5, allowUnstable: true, size: maxSize + 1, wpaused: true},
		// Don't allow unstable entries.
		{index: 3, allowUnstable: false, size: maxSize - 1, wpaused: true},
		{index: 3, allowUnstable: false, size: maxSize, wpaused: true},
		{index: 3, allowUnstable: false, size: maxSize + 1, wpaused: true},
		{index: 4, allowUnstable: false, size: maxSize - 1, wpaused: false},
		{index: 4, allowUnstable: false, size: maxSize, wpaused: true},
		{index: 4, allowUnstable: false, size: maxSize + 1, wpaused: true},
		{index: 5, allowUnstable: false, size: maxSize - 1, wpaused: false},
		{index: 5, allowUnstable: false, size: maxSize, wpaused: true},
		{index: 5, allowUnstable: false, size: maxSize + 1, wpaused: true},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLogWithSize(storage, raftLogger, maxSize)
			require.True(t, raftLog.append(
				logAppend{index: snap.Metadata.Index, term: snap.Metadata.Term, entries: ents}))
			raftLog.stableTo(4, 1)
			raftLog.maybeCommit(5, 1)
			raftLog.appliedTo(3, 0 /* size */)

			raftLog.acceptApplying(tt.index, tt.size, tt.allowUnstable)
			require.Equal(t, tt.wpaused, raftLog.applyingEntsPaused)
		})
	}
}

func TestAppliedTo(t *testing.T) {
	maxSize := entryEncodingSize(100)
	overshoot := entryEncodingSize(5)
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		index         uint64
		size          entryEncodingSize
		wapplyingSize entryEncodingSize
		wpaused       bool
	}{
		// Apply some of in-progress entries (applying = 5 below).
		{index: 4, size: overshoot - 1, wapplyingSize: maxSize + 1, wpaused: true},
		{index: 4, size: overshoot, wapplyingSize: maxSize, wpaused: true},
		{index: 4, size: overshoot + 1, wapplyingSize: maxSize - 1, wpaused: false},
		// Apply all of in-progress entries.
		{index: 5, size: overshoot - 1, wapplyingSize: maxSize + 1, wpaused: true},
		{index: 5, size: overshoot, wapplyingSize: maxSize, wpaused: true},
		{index: 5, size: overshoot + 1, wapplyingSize: maxSize - 1, wpaused: false},
		// Apply all of outstanding bytes.
		{index: 4, size: maxSize + overshoot, wapplyingSize: 0, wpaused: false},
		// Apply more than outstanding bytes.
		// Incorrect accounting doesn't underflow applyingSize.
		{index: 4, size: maxSize + overshoot + 1, wapplyingSize: 0, wpaused: false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLogWithSize(storage, raftLogger, maxSize)
			require.True(t, raftLog.append(
				logAppend{index: snap.Metadata.Index, term: snap.Metadata.Term, entries: ents}))
			raftLog.stableTo(4, 1)
			raftLog.maybeCommit(5, 1)
			raftLog.appliedTo(3, 0 /* size */)
			raftLog.acceptApplying(5, maxSize+overshoot, false /* allowUnstable */)

			raftLog.appliedTo(tt.index, tt.size)
			require.Equal(t, tt.index, raftLog.applied)
			require.Equal(t, uint64(5), raftLog.applying)
			require.Equal(t, tt.wapplyingSize, raftLog.applyingEntsSize)
			require.Equal(t, tt.wpaused, raftLog.applyingEntsPaused)
		})
	}
}

// TestNextUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
func TestNextUnstableEnts(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		unstable int
		wents    []pb.Entry
	}{
		{3, nil},
		{1, previousEnts},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			// append stable entries to storage
			storage := NewMemoryStorage()
			require.NoError(t, storage.Append(previousEnts[:tt.unstable-1]))

			// append unstable entries to raftlog
			raftLog := newLog(storage, raftLogger)
			require.True(t, raftLog.append(logAppend{entries: previousEnts}.skip(tt.unstable-1)))

			ents := raftLog.nextUnstableEnts()
			if l := len(ents); l > 0 {
				raftLog.stableTo(ents[l-1].Index, ents[l-1].Term)
			}
			require.Equal(t, mustLogRange(t, tt.wents), ents)
			require.Equal(t, previousEnts[len(previousEnts)-1].Index+1, raftLog.unstable.offset)
		})
	}
}

func TestCommitTo(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}}
	commit := uint64(2)
	tests := []struct {
		commit  uint64
		wcommit uint64
		wpanic  bool
	}{
		{3, 3, false},
		{1, 2, false}, // never decrease
		{4, 0, true},  // commit out of range -> panic
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			raftLog := newLog(NewMemoryStorage(), raftLogger)
			require.True(t, raftLog.append(logAppend{entries: previousEnts}))
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			require.Equal(t, tt.wcommit, raftLog.committed)
		})
	}
}

func TestStableTo(t *testing.T) {
	tests := []struct {
		stablei   uint64
		stablet   uint64
		wunstable uint64
	}{
		{1, 1, 2},
		{2, 2, 3},
		{2, 1, 1}, // bad term
		{3, 1, 1}, // bad index
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			raftLog := newLog(NewMemoryStorage(), raftLogger)
			require.True(t, raftLog.append(newLogAppend(0, 0, 1, 2)))
			raftLog.stableTo(tt.stablei, tt.stablet)
			require.Equal(t, tt.wunstable, raftLog.unstable.offset)
		})
	}
}

func TestStableToWithSnap(t *testing.T) {
	snapi, snapt := uint64(5), uint64(2)
	tests := []struct {
		stablei uint64
		stablet uint64
		newEnts []pb.Entry

		wunstable uint64
	}{
		{snapi + 1, snapt, nil, snapi + 1},
		{snapi, snapt, nil, snapi + 1},
		{snapi - 1, snapt, nil, snapi + 1},

		{snapi + 1, snapt + 1, nil, snapi + 1},
		{snapi, snapt + 1, nil, snapi + 1},
		{snapi - 1, snapt + 1, nil, snapi + 1},

		{snapi + 1, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 2},
		{snapi, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi - 1, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},

		{snapi + 1, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi - 1, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s := NewMemoryStorage()
			require.NoError(t, s.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: snapi, Term: snapt}}))
			raftLog := newLog(s, raftLogger)
			require.True(t, raftLog.append(logAppend{index: snapi, term: snapt, entries: tt.newEnts}))
			raftLog.stableTo(tt.stablei, tt.stablet)
			require.Equal(t, tt.wunstable, raftLog.unstable.offset)
		})

	}
}

// TestCompaction ensures that the number of log entries is correct after compactions.
func TestCompaction(t *testing.T) {
	tests := []struct {
		lastIndex uint64
		compact   []uint64
		wleft     []int
		wallow    bool
	}{
		// out of upper bound
		{1000, []uint64{1001}, []int{-1}, false},
		{1000, []uint64{300, 500, 800, 900}, []int{700, 500, 200, 100}, true},
		// out of lower bound
		{1000, []uint64{300, 299}, []int{700, -1}, false},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.False(t, tt.wallow)
				}
			}()
			storage := NewMemoryStorage()
			for i := uint64(1); i <= tt.lastIndex; i++ {
				storage.Append([]pb.Entry{{Index: i}})
			}
			raftLog := newLog(storage, raftLogger)
			raftLog.maybeCommit(tt.lastIndex, 0)

			raftLog.appliedTo(raftLog.committed, 0 /* size */)
			for j := 0; j < len(tt.compact); j++ {
				err := storage.Compact(tt.compact[j])
				if err != nil {
					require.False(t, tt.wallow)
					continue
				}
				require.Equal(t, tt.wleft[j], len(raftLog.allEntries()))
			}

		})
	}
}

func TestLogRestore(t *testing.T) {
	index := uint64(1000)
	term := uint64(1000)
	snap := pb.SnapshotMetadata{Index: index, Term: term}
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: snap})
	raftLog := newLog(storage, raftLogger)

	require.Zero(t, len(raftLog.allEntries()))
	require.Equal(t, index+1, raftLog.firstIndex())
	require.Equal(t, index, raftLog.committed)
	require.Equal(t, index+1, raftLog.unstable.offset)
	require.Equal(t, term, mustTerm(raftLog.term(index)))
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: offset, Term: 1}})
	l := newLog(storage, raftLogger)
	for i := uint64(1); i <= num; i++ {
		require.True(t, l.append(newLogAppend(i+offset-1, 1, 1)))
	}

	first := offset + 1
	tests := []struct {
		lo, hi        uint64
		wpanic        bool
		wErrCompacted bool
	}{
		{
			first - 2, first + 1,
			false,
			true,
		},
		{
			first - 1, first + 1,
			false,
			true,
		},
		{
			first, first,
			false,
			false,
		},
		{
			first + num/2, first + num/2,
			false,
			false,
		},
		{
			first + num - 1, first + num - 1,
			false,
			false,
		},
		{
			first + num, first + num,
			false,
			false,
		},
		{
			first + num, first + num + 1,
			true,
			false,
		},
		{
			first + num + 1, first + num + 1,
			true,
			false,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			err := l.mustCheckOutOfBounds(tt.lo, tt.hi)
			require.False(t, tt.wpanic)
			require.False(t, tt.wErrCompacted && err != ErrCompacted)
			require.False(t, !tt.wErrCompacted && err != nil)
		})
	}
}

func TestTerm(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: offset, Term: 1}})
	l := newLog(storage, raftLogger)
	for i := uint64(1); i < num; i++ {
		require.True(t, l.append(newLogAppend(offset+i-1, i, i+1)))
	}

	for i, tt := range []struct {
		idx  uint64
		term uint64
		err  error
	}{
		{idx: offset - 1, err: ErrCompacted},
		{idx: offset, term: 1},
		{idx: offset + num/2, term: num/2 + 1}, // NICE: was a bug
		{idx: offset + num - 1, term: num},
		{idx: offset + num, err: ErrUnavailable},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			term, err := l.term(tt.idx)
			require.Equal(t, tt.term, term)
			require.Equal(t, tt.err, err)
		})
	}
}

func TestTermWithUnstableSnapshot(t *testing.T) {
	storagesnapi := uint64(100)
	unstablesnapi := storagesnapi + 5

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: storagesnapi, Term: 1}})
	l := newLog(storage, raftLogger)
	l.restore(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: unstablesnapi, Term: 1}})

	for i, tt := range []struct {
		idx  uint64
		term uint64
		err  error
	}{
		// cannot get term from storage
		{idx: storagesnapi, err: ErrCompacted},
		// cannot get term from the gap between storage ents and unstable snapshot
		{idx: storagesnapi + 1, err: ErrCompacted},
		{idx: unstablesnapi - 1, err: ErrCompacted},
		// get term from unstable snapshot index
		{idx: unstablesnapi, term: 1},
		// the log beyond the unstable snapshot is empty
		{idx: unstablesnapi + 1, err: ErrUnavailable},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			term, err := l.term(tt.idx)
			require.Equal(t, tt.term, term)
			require.Equal(t, tt.err, err)
		})
	}
}

func TestSlice(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	last := offset + num
	half := offset + num/2
	halfe := pb.Entry{Index: half, Term: half}

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: offset}})
	for i = 1; i < num/2; i++ {
		storage.Append([]pb.Entry{{Index: offset + i, Term: offset + i}})
	}
	l := newLog(storage, raftLogger)
	for i = num / 2; i < num; i++ {
		require.True(t, l.append(newLogAppend(offset+i-1, offset+i-1, offset+i)))
	}

	tests := []struct {
		from  uint64
		to    uint64
		limit uint64

		w      []pb.Entry
		wpanic bool
	}{
		// test no limit
		{offset - 1, offset + 1, noLimit, nil, false},
		{offset, offset + 1, noLimit, nil, false},
		{half - 1, half + 1, noLimit, []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half, half + 1, noLimit, []pb.Entry{{Index: half, Term: half}}, false},
		{last - 1, last, noLimit, []pb.Entry{{Index: last - 1, Term: last - 1}}, false},
		{last, last + 1, noLimit, nil, true},

		// test limit
		{half - 1, half + 1, 0, []pb.Entry{{Index: half - 1, Term: half - 1}}, false},
		{half - 1, half + 1, uint64(halfe.Size() + 1), []pb.Entry{{Index: half - 1, Term: half - 1}}, false},
		{half - 2, half + 1, uint64(halfe.Size() + 1), []pb.Entry{{Index: half - 2, Term: half - 2}}, false},
		{half - 1, half + 1, uint64(halfe.Size() * 2), []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half - 1, half + 2, uint64(halfe.Size() * 3), []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}, {Index: half + 1, Term: half + 1}}, false},
		{half, half + 2, uint64(halfe.Size()), []pb.Entry{{Index: half, Term: half}}, false},
		{half, half + 2, uint64(halfe.Size() * 2), []pb.Entry{{Index: half, Term: half}, {Index: half + 1, Term: half + 1}}, false},
	}

	for j, tt := range tests {
		t.Run(fmt.Sprint(j), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			g, err := l.slice(tt.from, tt.to, entryEncodingSize(tt.limit))
			require.False(t, tt.from <= offset && err != ErrCompacted)
			require.False(t, tt.from > offset && err != nil)
			require.Equal(t, mustLogRange(t, tt.w), g)
		})
	}
}
func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}
