// Copyright 2024 The etcd Authors
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

package scenariotests_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/rafttest"
	"go.etcd.io/raft/v3/scenariotests/netrix"
)

// TestSnapshotSentToLaggingFollower verifies the TLA+ SendSnapshot(i,j,index)
// action: when a follower's log falls behind the leader's compacted entries,
// the leader sends a MsgSnap to bring the follower up to date.
func TestSnapshotSentToLaggingFollower(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(3)

	var snapshotSetup bool

	sm := netrix.NewStateMachine()
	sm.Builder().On(
		netrix.IsMessageType(pb.MsgSnap).And(netrix.IsMessageTo(3)),
		netrix.SuccessState,
	).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Isolate node 3 on the first MsgApp from the leader (election settled).
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(1)).And(part.IsHealed()),
	).Then(part.IsolateAction(), netrix.DeliverMessage()))
	// Heal once the leader has compacted (snapshot setup is done).
	filters.AddFilter(netrix.If(
		netrix.When(func() bool { return snapshotSetup }).And(part.IsActive()),
	).Then(part.HealAction(), netrix.DeliverMessage()))
	// While partitioned, drop all traffic to/from node 3.
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "snapshot-sent-to-lagging-follower",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		ResetFunc: func() {
			part.Heal()
			snapshotSetup = false
		},
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			switch round {
			case 8:
				// Propose entries while node 3 is isolated.
				_ = e.Propose(0, []byte("a"))
				_ = e.Propose(0, []byte("b"))
			case 25:
				if !snapshotSetup {
					snapshotSetup = true
					// Update the leader's snapshot to index 12 and compact so
					// entries before index 13 are gone (firstIndex becomes 13).
					// Node 3 is at index 11; since firstIndex(13) > next(12),
					// the leader must send a snapshot instead of MsgApp.
					leaderStatus := e.Nodes[0].BasicStatus()
					snap := pb.EnsureSnapshot(nil)
					snap.Metadata.Index = new(uint64(12))
					snap.Metadata.Term = new(leaderStatus.GetTerm())
					snap.Metadata.ConfState.Voters = []uint64{1, 2, 3}
					e.Nodes[0].History = append(e.Nodes[0].History, snap)
					_ = e.Compact(0, 13) // firstIndex becomes 13
				}
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected leader to send MsgSnap to lagging follower, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestSnapshotDeliveredAndFollowedByAppend verifies that after a snapshot
// is delivered to a follower, the leader resumes log replication with
// subsequent MsgApp entries (the follower is now in sync).
func TestSnapshotDeliveredAndFollowedByAppend(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(3)

	var snapshotSetup bool
	var snapDelivered bool

	appAfterSnap := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		return snapDelivered && e.Msg.GetType() == pb.MsgApp && e.To == 3
	})

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	snapSeen := init.On(
		netrix.IsMessageType(pb.MsgSnap).And(netrix.IsMessageTo(3)),
		"snap-sent",
	)
	snapSeen.On(appAfterSnap, netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(1)).And(part.IsHealed()),
	).Then(part.IsolateAction(), netrix.DeliverMessage()))
	filters.AddFilter(netrix.If(
		netrix.When(func() bool { return snapshotSetup }).And(part.IsActive()),
	).Then(part.HealAction(), netrix.DeliverMessage()))
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "snapshot-delivered-and-followed-by-append",
		MaxRounds:    250,
		StateMachine: sm,
		Filters:      filters,
		ResetFunc: func() {
			part.Heal()
			snapshotSetup = false
			snapDelivered = false
		},
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			switch round {
			case 8:
				_ = e.Propose(0, []byte("a"))
			case 25:
				if !snapshotSetup {
					snapshotSetup = true
					leaderStatus := e.Nodes[0].BasicStatus()
					snap := pb.EnsureSnapshot(nil)
					snap.Metadata.Index = new(uint64(12))
					snap.Metadata.Term = new(leaderStatus.GetTerm())
					snap.Metadata.ConfState.Voters = []uint64{1, 2, 3}
					e.Nodes[0].History = append(e.Nodes[0].History, snap)
					_ = e.Compact(0, 12) // firstIndex becomes 13, forcing snapshot for node 3
				}
			case 35:
				snapDelivered = true // give time for snapshot to be processed
				_ = e.Propose(0, []byte("post-snap"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected leader to send MsgApp to follower after snapshot, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestCommitIndexMonotonicDuringSnapshot verifies that the leader never sends
// a MsgApp with a Commit index lower than it previously announced, even around
// snapshot installation. This is a proxy for CommittedIsDurableInv.
func TestCommitIndexMonotonicDuringSnapshot(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(3)

	var snapshotSetup bool
	maxCommit := make(map[uint64]uint64)

	commitDecreases := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() != pb.MsgApp {
			return false
		}
		nodeID := e.From
		commit := e.Msg.GetCommit()
		if prev, ok := maxCommit[nodeID]; ok && commit < prev {
			return true
		}
		if commit > maxCommit[nodeID] {
			maxCommit[nodeID] = commit
		}
		return false
	})

	sm := netrix.NewStateMachine()
	sm.Builder().On(commitDecreases, netrix.FailureState)

	filters := netrix.NewFilterSet()
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(1)).And(part.IsHealed()),
	).Then(part.IsolateAction(), netrix.DeliverMessage()))
	filters.AddFilter(netrix.If(
		netrix.When(func() bool { return snapshotSetup }).And(part.IsActive()),
	).Then(part.HealAction(), netrix.DeliverMessage()))
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "commit-index-monotonic-during-snapshot",
		MaxRounds:    150,
		StateMachine: sm,
		Filters:      filters,
		ResetFunc: func() {
			part.Heal()
			snapshotSetup = false
			maxCommit = make(map[uint64]uint64)
		},
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			switch round {
			case 8:
				_ = e.Propose(0, []byte("a"))
				_ = e.Propose(0, []byte("b"))
			case 25:
				if !snapshotSetup {
					snapshotSetup = true
					leaderStatus := e.Nodes[0].BasicStatus()
					snap := pb.EnsureSnapshot(nil)
					snap.Metadata.Index = new(uint64(12))
					snap.Metadata.Term = new(leaderStatus.GetTerm())
					snap.Metadata.ConfState.Voters = []uint64{1, 2, 3}
					e.Nodes[0].History = append(e.Nodes[0].History, snap)
					_ = e.Compact(0, 13) // firstIndex becomes 13, forcing snapshot for node 3
				}
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(),
		"CommittedIsDurableInv violated: leader commit index decreased during snapshot scenario")
}
