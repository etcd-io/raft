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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/tracker"

	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	testingSnap = pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2}},
		},
	}
)

func TestSnapshotPauseReplication(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(testingSnap)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.trk.Progress[2].BecomeSnapshot(11)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)

	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	msgs := sm.readMessages()
	require.Empty(t, msgs)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)
}

func TestSnapshotFailure(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(testingSnap)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.trk.Progress[2].Next = 1
	sm.trk.Progress[2].BecomeSnapshot(11)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)

	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgSnapStatus, Reject: true})
	require.Equal(t, uint64(1), sm.trk.Progress[2].Next)
	assert.True(t, sm.trk.Progress[2].MsgAppFlowPaused)
	require.Equal(t, tracker.StateProbe, sm.trk.Progress[2].State)
}

func TestSnapshotSucceedToReplicate(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(testingSnap)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.trk.Progress[2].Next = 1
	sm.trk.Progress[2].BecomeSnapshot(11)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)

	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgSnapStatus, Reject: false, AppliedSnapshotIndex: 11})
	require.Equal(t, uint64(12), sm.trk.Progress[2].Next)
	require.Equal(t, tracker.StateReplicate, sm.trk.Progress[2].State)
	assert.True(t, sm.trk.Progress[2].MsgAppFlowPaused)
}

func TestSnapshotSucceedToProbe(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(testingSnap)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.trk.Progress[2].Next = 1
	sm.trk.Progress[2].BecomeSnapshot(11)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)

	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgSnapStatus, Reject: false, AppliedSnapshotIndex: 10})

	require.Equal(t, uint64(11), sm.trk.Progress[2].Next)
	require.Equal(t, tracker.StateProbe, sm.trk.Progress[2].State)
	assert.True(t, sm.trk.Progress[2].MsgAppFlowPaused)
}

func TestSnapshotAbort(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(testingSnap)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.trk.Progress[2].Next = 1
	sm.trk.Progress[2].BecomeSnapshot(11)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)

	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: 11})
	// The follower entered StateReplicate and the leader send an append
	// and optimistically updated the progress (so we see 13 instead of 12).
	// There is something to append because the leader appended an empty entry
	// to the log at index 12 when it assumed leadership.
	require.Equal(t, uint64(13), sm.trk.Progress[2].Next)
	require.Equal(t, 1, sm.trk.Progress[2].Inflights.Count())
	require.Equal(t, tracker.StateReplicate, sm.trk.Progress[2].State)
}

func TestSnapshotSucceedWithoutIndex(t *testing.T) {
	// Test backward compatibility: handle MsgSnapStatus without AppliedSnapshotIndex
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(testingSnap)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.trk.Progress[2].Next = 1
	sm.trk.Progress[2].BecomeSnapshot(11)
	require.Equal(t, tracker.StateSnapshot, sm.trk.Progress[2].State)

	// Send MsgSnapStatus without AppliedSnapshotIndex (0 value)
	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgSnapStatus, Reject: false, AppliedSnapshotIndex: 0})
	
	// Should transition to probe state when AppliedSnapshotIndex is 0
	require.Equal(t, uint64(1), sm.trk.Progress[2].Next)
	require.Equal(t, uint64(0), sm.trk.Progress[2].Match)
	require.Equal(t, tracker.StateProbe, sm.trk.Progress[2].State)
	assert.True(t, sm.trk.Progress[2].MsgAppFlowPaused)
}
