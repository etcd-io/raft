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

// TestElectionPhases verifies the full election message sequence:
// MsgVote → MsgVoteResp (granted) → MsgApp (no-op from new leader).
// This traces the TLA+ actions Timeout → RequestVote → BecomeLeader.
func TestElectionPhases(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	voting := init.On(netrix.IsMessageType(pb.MsgVote), "voting")
	leader := voting.On(voteGranted(), "leader")
	leader.On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "election-phases",
		MaxRounds:    50,
		StateMachine: sm,
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected full election phases MsgVote→VoteResp→MsgApp, final state: %s", result.FinalState)
}

// TestElectionRequiresQuorum verifies that no leader can be elected when all
// vote messages are dropped, covering the TLA+ quorum requirement in BecomeLeader.
func TestElectionRequiresQuorum(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	// MsgApp from a leader would mean election succeeded — failure condition.
	sm := netrix.NewStateMachine()
	sm.Builder().On(netrix.IsMessageType(pb.MsgApp), netrix.FailureState)

	filters := netrix.NewFilterSet()
	filters.AddFilter(netrix.If(netrix.IsMessageType(pb.MsgVote)).Then(netrix.DropMessage()))
	filters.AddFilter(netrix.If(netrix.IsMessageType(pb.MsgVoteResp)).Then(netrix.DropMessage()))

	tc := &netrix.TestCase{
		Name:         "election-requires-quorum",
		MaxRounds:    30,
		StateMachine: sm,
		Filters:      filters,
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "no leader should be elected when all votes are dropped")
}

// TestElectionWithOneLostVote verifies that election succeeds even when one
// vote message is dropped, demonstrating fault tolerance at quorum - 1.
func TestElectionWithOneLostVote(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	sm := netrix.NewStateMachine()
	sm.Builder().On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Drop only node 1 → node 2 vote. Node 1 still has node 1 + node 3 = quorum.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(netrix.IsMessageFrom(1)).And(netrix.IsMessageTo(2)),
	).Then(netrix.DropMessage()))

	tc := &netrix.TestCase{
		Name:         "election-one-lost-vote",
		MaxRounds:    50,
		StateMachine: sm,
		Filters:      filters,
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "election should succeed with one lost vote, final state: %s", result.FinalState)
}

// TestAtMostOneLeaderPerTerm verifies the MoreThanOneLeaderInv TLA+ invariant:
// at most one node may act as leader per term at any point in time.
func TestAtMostOneLeaderPerTerm(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	termLeaders := make(map[uint64]uint64) // term → first leader node ID seen

	dualLeader := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() != pb.MsgApp {
			return false
		}
		term := e.Msg.GetTerm()
		if existing, ok := termLeaders[term]; ok && existing != e.From {
			return true // two different nodes claiming leadership in the same term
		}
		termLeaders[term] = e.From
		return false
	})

	sm := netrix.NewStateMachine()
	sm.Builder().On(dualLeader, netrix.FailureState)

	tc := &netrix.TestCase{
		Name:         "at-most-one-leader-per-term",
		MaxRounds:    100,
		StateMachine: sm,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "MoreThanOneLeaderInv violated: two leaders detected in the same term")
}

// TestLeaderElectionWithConcurrentCandidates verifies that a leader eventually
// emerges even when two candidates compete (split-vote recovery via term increment).
func TestLeaderElectionWithConcurrentCandidates(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 5) }

	sm := netrix.NewStateMachine()
	sm.Builder().On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Create a partial split so no candidate gets quorum in the first round.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(netrix.IsMessageFrom(1)).And(netrix.IsMessageTo(3)),
	).Then(netrix.DropMessage()))
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(netrix.IsMessageFrom(2)).And(netrix.IsMessageTo(4)),
	).Then(netrix.DropMessage()))

	tc := &netrix.TestCase{
		Name:         "concurrent-candidates",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			if err := e.Campaign(0); err != nil {
				return err
			}
			return e.Campaign(1)
		},
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected leader after split vote recovery, final state: %s after %d rounds", result.FinalState, result.Rounds)
}

// TestHigherTermMessageForcesStepdown verifies the TLA+ UpdateTerm / StepDownToFollower
// actions: a leader steps down upon receiving a message with a higher term.
// Node 3 is partitioned so it increments its term; upon rejoining it sends a
// higher-term MsgVote which forces the original leader to step down (observable
// as the original leader sending MsgVoteResp as a follower).
func TestHigherTermMessageForcesStepdown(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(3)

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Confirm original leader (node 1) is active.
	afterLeader := init.On(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(1)),
		"leader-seen",
	)
	// Node 1 sends MsgVoteResp as follower after stepping down.
	afterLeader.On(
		netrix.IsMessageType(pb.MsgVoteResp).And(netrix.IsMessageFrom(1)),
		netrix.SuccessState,
	).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "higher-term-forces-stepdown",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 15 {
				part.Isolate() // isolate node 3 after election
			}
			if round == 130 { // ~13 ElectionTick cycles; node 3 is far ahead in term
				part.Heal()
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected original leader to step down on higher-term message, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}
