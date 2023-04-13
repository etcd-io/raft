// Copyright 2023 The etcd Authors
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

	pb "go.etcd.io/raft/v3/raftpb"
)

// LogRange represents a range of contiguous log entries. This entries slice has
// been verified to have the following properties:
//   - entries[i+1].Index == entries[i].Index + 1
//   - entries[i+1].Term >= entries[i].Term
//
// TODO(pavelkalinnikov): make it harder to convert from []pb.Entry to LogRange.
type LogRange []pb.Entry

// VerifyLogRange checks that the given slice represents a valid range of
// contiguous log entries that can be appended after (index, term).
func VerifyLogRange(index, term uint64, entries []pb.Entry) (LogRange, error) {
	for i := range entries {
		index++
		if got := entries[i].Index; got != index {
			return nil, fmt.Errorf("entry[%d].Index is %d, want %d", i, got, index)
		}
		curTerm := entries[i].Term
		if curTerm < term {
			return nil, fmt.Errorf("entry[%d].Term is %d, want at least %d", i, curTerm, term)
		}
		term = curTerm
	}
	return entries, nil
}

// Append appends a valid log range to this one. It is like a regular slice
// append, but verifies that the resulting slice is a valid log range.
//
// TODO(pavelkalinnikov): consider returning error instead of panic.
func (r LogRange) Append(other LogRange) LogRange {
	if lr := len(r); lr != 0 && len(other) != 0 {
		if last, index := r[lr-1].Index, other[0].Index; index != last+1 {
			panic(fmt.Sprintf("disjoint ranges: last index %d, next %d, want %d", last, index, last+1))
		}
		if last, term := r[lr-1].Term, other[0].Term; term < last {
			panic(fmt.Sprintf("appending non-monotonic term: last term %d, next %d", last, term))
		}
	}
	return append(r, other...)
}

type logAppend struct {
	index   uint64   // log index after which entries are appended
	term    uint64   // term of the entry preceding entries
	entries LogRange // entries start from index + 1
}

func (la logAppend) skip(count int) logAppend {
	if count == 0 {
		return la
	}
	return logAppend{
		index:   la.index + uint64(count),
		term:    la.entries[count-1].Term,
		entries: la.entries[count:],
	}
}

func (la logAppend) lastIndex() uint64 {
	return la.index + uint64(len(la.entries))
}

func verifyLogAppend(index, term uint64, entries []pb.Entry) (logAppend, error) {
	rng, err := VerifyLogRange(index, term, entries)
	if err != nil {
		return logAppend{}, err
	}
	return logAppend{index: index, term: term, entries: rng}, nil
}

func mustLogRange(t *testing.T, entries []pb.Entry) LogRange {
	t.Helper()
	if len(entries) == 0 {
		return entries
	}
	res, err := VerifyLogRange(entries[0].Index-1, entries[0].Term, entries)
	if err != nil {
		t.Fatalf("VerifyLogRange: %v", err)
	}
	return res
}
