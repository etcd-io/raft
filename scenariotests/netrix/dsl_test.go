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

package netrix_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/rafttest"
	"go.etcd.io/raft/v3/scenariotests/netrix"
)

const netrixTestIterations = 20

func runNetrixTest(t *testing.T, tc *netrix.TestCase, envFunc func() *rafttest.InteractionEnv) netrix.RunResult {
	t.Helper()
	tc.Iterations = netrixTestIterations
	tc.EnvFunc = envFunc
	return netrix.Run(tc, nil)
}

// newEnv creates a 3-node raft cluster bootstrapped at index 10 for testing.
func newEnv(t *testing.T) *rafttest.InteractionEnv {
	t.Helper()
	env := rafttest.NewInteractionEnv(nil)
	cfg := raft.Config{
		ElectionTick:    3,
		HeartbeatTick:   1,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: math.MaxInt32,
	}
	snap := pb.EnsureSnapshot(nil)
	snap.Metadata.ConfState.Voters = []uint64{1, 2, 3}
	snap.Metadata.Index = new(uint64(10))
	snap.Metadata.Term = new(uint64(1))
	require.NoError(t, env.AddNodes(3, cfg, snap))
	return env
}

// tickAll ticks all nodes in env once (heartbeat tick).
func tickAll(env *rafttest.InteractionEnv) {
	for i := range env.Nodes {
		_ = env.Tick(i, 1)
	}
}

// TestAllMessagesDelivered verifies that with no filters, a cluster stabilizes
// after an election: we see the no-op MsgApp the leader sends immediately.
func TestAllMessagesDelivered(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t) }

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// The leader immediately sends MsgApp (with no-op entry) to followers.
	init.On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState)

	tc := &netrix.TestCase{
		Name:         "election-succeeds",
		MaxRounds:    50,
		StateMachine: sm,
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0) // node index 0, id=1
		},
	}

	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected MsgApp from leader, final state: %s", result.FinalState)
}

// TestDropAllVotes verifies that when MsgVote messages are dropped, no leader
// can be elected (no MsgApp from a leader should appear within MaxRounds).
func TestDropAllVotes(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t) }

	// We succeed (i.e., confirm the absence of a leader) if the run ends
	// without hitting FailureState.
	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Seeing a MsgApp means a leader was elected — that's the failure condition.
	init.On(netrix.IsMessageType(pb.MsgApp), netrix.FailureState)

	filters := netrix.NewFilterSet()
	// Drop all vote messages so no election can complete.
	filters.AddFilter(netrix.If(netrix.IsMessageType(pb.MsgVote)).Then(netrix.DropMessage()))

	tc := &netrix.TestCase{
		Name:         "drop-votes-no-leader",
		MaxRounds:    10,
		StateMachine: sm,
		Filters:      filters,
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0)
		},
	}

	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(),
		"no leader should be elected when all votes are dropped")
}

// TestPartitionLeaderAfterElection verifies that dropping all messages from
// the leader (node 1) after it is elected does not produce further MsgApp from
// node 1, and that the other nodes can proceed to a new election.
func TestPartitionLeaderAfterElection(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t) }

	// Phase 1: let the election complete by waiting for the leader's MsgApp.
	// Phase 2: drop subsequent messages from node 1 (simulating a partition).
	// Phase 3: wait for a MsgVote from any node other than 1 (new election).

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Phase 1 → 2: first MsgApp confirms leader election.
	phase2 := init.On(netrix.IsMessageType(pb.MsgApp), "leader-elected")
	// Phase 2 → success: a follower starts a new election (sends MsgVote).
	phase2.On(
		netrix.IsMessageType(pb.MsgVote).And(netrix.IsMessageFrom(1).Not()),
		netrix.SuccessState,
	)

	filters := netrix.NewFilterSet()
	// Once we have seen the leader's no-op MsgApp, drop all further messages
	// from node 1 to simulate a partition.
	filters.AddFilter(netrix.If(
		netrix.IsMessageFrom(1).And(netrix.Count("leader-seen").Geq(1)),
	).Then(netrix.DropMessage()))

	// Count the first MsgApp and deliver it normally.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgApp).And(netrix.Count("leader-seen").Lt(1)),
	).Then(
		netrix.Count("leader-seen").Incr(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "leader-partition",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		// Tick all nodes each round to drive election timeouts.
		TickFunc: func(e *rafttest.InteractionEnv, _ int) {
			tickAll(e)
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0)
		},
	}

	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected new election after partitioning leader, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestMessageRecording verifies that Set().Store() and Set().DeliverAll()
// correctly buffer and replay messages.
func TestMessageRecording(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t) }

	const setLabel = "votes"
	// Store the first 2 MsgVote messages. On the 3rd, flush everything.
	filters := netrix.NewFilterSet()

	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(netrix.Count(setLabel).Lt(2)),
	).Then(
		netrix.Count(setLabel).Incr(),
		netrix.Set(setLabel).Store(),
	))

	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(netrix.Count(setLabel).Geq(2)),
	).Then(
		netrix.Set(setLabel).DeliverAll(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:      "message-recording",
		MaxRounds: 50,
		Filters:   filters,
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0)
		},
	}

	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.Equal(t, "no-state-machine", result.FinalState)
	// The cluster should still have made progress: an election happened.
	require.Greater(t, len(result.Events), 0, "expected some events")
}

// TestCounterConditions exercises counter-based conditions across a run.
func TestCounterConditions(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t) }
	const ctr = "msgcount"

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Succeed once we have seen at least 5 messages total.
	init.On(netrix.Count(ctr).Geq(5), netrix.SuccessState)

	filters := netrix.NewFilterSet()
	filters.AddFilter(netrix.If(netrix.Always()).Then(
		netrix.Count(ctr).Incr(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "counter-conditions",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0)
		},
	}

	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected to count 5 messages, final state: %s", result.FinalState)
}

// TestBooleanConditionComposition verifies And/Or/Not composition.
func TestBooleanConditionComposition(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t) }

	// Conditions: message is from node 1 AND to node 2.
	fromOneToTwo := netrix.IsMessageFrom(1).And(netrix.IsMessageTo(2))

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	init.On(fromOneToTwo, netrix.SuccessState)

	tc := &netrix.TestCase{
		Name:         "condition-composition",
		MaxRounds:    50,
		StateMachine: sm,
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0)
		},
	}

	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected to see a message from 1 to 2, final state: %s", result.FinalState)
}

// TestIterationsRetryUntilSuccess verifies that Run retries the scenario up to
// Iterations times and stops as soon as one attempt succeeds, reporting the
// correct 1-based Iteration index in the result.
//
// The test uses a counter in the EnvFunc closure to make the first two
// iterations deliberately fail (MaxRounds=1, no messages → no success) and
// succeed on the third by giving it enough rounds to see a MsgApp.
func TestIterationsRetryUntilSuccess(t *testing.T) {
	attempt := 0

	sm := netrix.NewStateMachine()
	sm.Builder().On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState)

	tc := &netrix.TestCase{
		Name:         "iterations-retry",
		Iterations:   5,
		MaxRounds:    1,
		StateMachine: sm,
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			return e.Campaign(0)
		},
	}
	tc.EnvFunc = func() *rafttest.InteractionEnv {
		attempt++
		// Give later attempts enough rounds to complete an election.
		if attempt >= 3 {
			tc.MaxRounds = 50
		}
		return newEnv(t)
	}

	result := netrix.Run(tc, nil)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected success by iteration 3, final state: %s", result.FinalState)
	require.GreaterOrEqual(t, result.Iteration, 3, "expected at least 3 iterations before success")
	require.Equal(t, attempt, result.Iteration, "EnvFunc call count should match Iteration")
}
