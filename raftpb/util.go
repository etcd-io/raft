// Copyright 2026 The etcd Authors
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

package raftpb

// EnsureSnapshotMetadata ensures that m and all of its pointer fields are
// non-nil. If m is nil, a new SnapshotMetadata is allocated. Any nil pointer
// field is set to point to its zero value. Returns the resulting m.
func EnsureSnapshotMetadata(m *SnapshotMetadata) *SnapshotMetadata {
	if m == nil {
		m = new(SnapshotMetadata)
	}
	if m.ConfState == nil {
		m.ConfState = new(ConfState)
	}
	if m.Index == nil {
		m.Index = new(uint64)
	}
	if m.Term == nil {
		m.Term = new(uint64)
	}
	return m
}

// EnsureSnapshot ensures that s and all of its pointer fields are non-nil.
// If s is nil, a new Snapshot is allocated. Any nil pointer field is set to
// point to its zero value. Returns the resulting s.
func EnsureSnapshot(s *Snapshot) *Snapshot {
	if s == nil {
		s = new(Snapshot)
	}
	s.Metadata = EnsureSnapshotMetadata(s.Metadata)
	return s
}

// EntrySliceToPointers converts a slice of Entry values to a slice of Entry pointers.
// TODO: remove this function after we switch to use *Entry everywhere
func EntrySliceToPointers(ents []Entry) []*Entry {
	if ents == nil {
		return nil
	}
	result := make([]*Entry, len(ents))
	for i := range ents {
		result[i] = &ents[i]
	}
	return result
}

// EntrySliceFromPointers converts a slice of Entry pointers to a slice of Entry values.
// TODO: remove this function after we switch to use *Entry everywhere
func EntrySliceFromPointers(ents []*Entry) []Entry {
	if ents == nil {
		return nil
	}
	result := make([]Entry, len(ents))
	for i := range ents {
		if ents[i] != nil {
			result[i] = *ents[i]
		}
	}
	return result
}
