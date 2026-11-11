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

// TestLeaderPartitionTriggersNewElection verifies that when the leader is
// isolated after election, the remaining nodes hold a new election.
// This covers the TLA+ Timeout and RequestVote actions under network partition.
func TestLeaderPartitionTriggersNewElection(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	leaderSeen := netrix.Count("leader-seen")

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Phase 1 → 2: first MsgApp confirms leader elected.
	phase2 := init.On(netrix.IsMessageType(pb.MsgApp), "leader-elected")
	// Phase 2 → success: a follower (not node 1) starts a new election.
	phase2.On(
		netrix.IsMessageType(pb.MsgVote).And(netrix.IsMessageFrom(1).Not()),
		netrix.SuccessState,
	).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Drop all messages from node 1 once we've seen its leader MsgApp.
	filters.AddFilter(netrix.If(
		netrix.IsMessageFrom(1).And(leaderSeen.Geq(1)),
	).Then(netrix.DropMessage()))
	// Count and deliver the first MsgApp from node 1.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(1)).And(leaderSeen.Lt(1)),
	).Then(leaderSeen.Incr(), netrix.DeliverMessage()))

	tc := &netrix.TestCase{
		Name:         "leader-partition-new-election",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected new election after partitioning leader, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestMinorityCannotElectLeader verifies the TLA+ QuorumLogInv requirement:
// a minority partition (2 out of 5 nodes) cannot elect a leader.
func TestMinorityCannotElectLeader(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 5) }

	// MsgApp from minority would mean it elected a leader — violation.
	sm := netrix.NewStateMachine()
	sm.Builder().On(
		netrix.IsMessageType(pb.MsgApp).And(fromAnyOf(1, 2)),
		netrix.FailureState,
	)

	part := netrix.PartitionSides([]uint64{1, 2}, []uint64{3, 4, 5})

	filters := netrix.NewFilterSet()
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "minority-cannot-elect-leader",
		MaxRounds:    50,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, _ int) {
			tickAll(e)
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			part.Isolate()
			return e.Campaign(0)
		},
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "minority partition must not elect a leader (QuorumLogInv)")
}

// TestMajorityPartitionMakesProgress verifies that the majority side of a
// partition can elect a leader and make progress.
func TestMajorityPartitionMakesProgress(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 5) }

	sm := netrix.NewStateMachine()
	sm.Builder().On(
		netrix.IsMessageType(pb.MsgApp).And(fromAnyOf(3, 4, 5)),
		netrix.SuccessState,
	).MarkSuccess()

	part := netrix.PartitionSides([]uint64{1, 2}, []uint64{3, 4, 5})

	filters := netrix.NewFilterSet()
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "majority-partition-makes-progress",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		// Campaign from node 3 which is in the majority partition.
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			part.Isolate()
			return e.Campaign(2)
		},
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"majority partition should elect a leader, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestFollowerRejoinsCatchesUp verifies that a partitioned follower receives
// catch-up AppendEntries upon rejoining, implementing the TLA+ AppendEntries action.
func TestFollowerRejoinsCatchesUp(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(3)

	var healed bool

	// After healing, we expect node 3 (ID=3) to receive MsgApp with entries.
	catchup := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		return healed && e.Msg.GetType() == pb.MsgApp && e.To == 3 && len(e.Msg.GetEntries()) > 0
	})

	sm := netrix.NewStateMachine()
	sm.Builder().On(catchup, netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "follower-rejoins-catches-up",
		MaxRounds:    150,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			switch round {
			case 15:
				part.Isolate()
			case 20, 30, 40:
				// Propose entries while node 3 is isolated.
				_ = e.Propose(0, []byte("entry"))
			case 80:
				healed = true
				part.Heal()
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"follower should receive catch-up entries after rejoining, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}
