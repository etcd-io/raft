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

const (
	maxCommitKey      = "maxCommit"
	recordedCommitKey = "recordedCommit"
	maxTermSeenKey    = "maxTermSeen"
)

func uint64ByNode(ctx *netrix.Context, key string) map[uint64]uint64 {
	if byNode, ok := netrix.GetVar[map[uint64]uint64](ctx, key); ok {
		return byNode
	}
	byNode := make(map[uint64]uint64)
	netrix.SetVar(ctx, key, byNode)
	return byNode
}

// TestCommittedEntriesNeverDiverge verifies the LogInv TLA+ invariant:
// committed log prefixes from any two nodes must be consistent (one is a
// prefix of the other). As a message-level proxy this test verifies that
// no leader ever sends a MsgApp with a Commit value lower than the highest
// Commit it previously sent — a necessary condition for log monotonicity.
func TestCommittedEntriesNeverDiverge(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	commitDecreases := netrix.Condition(func(e *netrix.Event, ctx *netrix.Context) bool {
		if e.Msg.GetType() != pb.MsgApp {
			return false
		}
		// Track per-node maximum commit index observed in MsgApp sends.
		maxCommit := uint64ByNode(ctx, maxCommitKey)
		nodeID := e.From
		commit := e.Msg.GetCommit()
		if prev, ok := maxCommit[nodeID]; ok && commit < prev {
			return true // commit index went backwards — invariant violation
		}
		if commit > maxCommit[nodeID] {
			maxCommit[nodeID] = commit
		}
		return false
	})

	sm := netrix.NewStateMachine()
	sm.Builder().On(commitDecreases, netrix.FailureState)

	tc := &netrix.TestCase{
		Name:         "committed-entries-never-diverge",
		MaxRounds:    100,
		StateMachine: sm,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round%20 == 5 {
				_ = e.Propose(0, []byte("entry"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "LogInv violated: commit index decreased for a node")
}

// TestNewLeaderHasAllCommittedEntries verifies the LeaderCompletenessInv
// TLA+ invariant: a new leader's log must contain all previously committed
// entries. After the original leader commits entries and is then partitioned,
// the new leader must eventually advance its commit index to match.
func TestNewLeaderHasAllCommittedEntries(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(1)

	// Phase 1: wait for original leader (node 1) to advance its commit index.
	// Once a sufficient commit is seen, also isolate node 1 so a new leader must take over.
	phase1Done := netrix.Condition(func(e *netrix.Event, ctx *netrix.Context) bool {
		if e.Msg.GetType() != pb.MsgApp || e.From != 1 {
			return false
		}
		if e.Msg.GetCommit() > 10 { // beyond the bootstrap commit at index 10
			netrix.SetVar(ctx, recordedCommitKey, e.Msg.GetCommit())
			return true
		}
		return false
	})

	// Phase 2: new leader (not node 1) eventually commits to at least the same index.
	newLeaderCommitOK := netrix.Condition(func(e *netrix.Event, ctx *netrix.Context) bool {
		recordedCommit, _ := netrix.GetVar[uint64](ctx, recordedCommitKey)
		return recordedCommit > 0 &&
			e.Msg.GetType() == pb.MsgApp &&
			e.From != 1 &&
			e.Msg.GetCommit() >= recordedCommit
	})

	newLeaderCommitBehind := netrix.Condition(func(e *netrix.Event, ctx *netrix.Context) bool {
		recordedCommit, _ := netrix.GetVar[uint64](ctx, recordedCommitKey)
		// A new leader sending MsgApp with a commit below a previously committed
		// index would mean it is missing committed entries.
		return recordedCommit > 0 &&
			e.Msg.GetType() == pb.MsgApp &&
			e.From != 1 &&
			// Give the new leader one term to commit its no-op before checking.
			e.Msg.GetTerm() > 2 &&
			e.Msg.GetCommit() < recordedCommit
	})

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	afterCommit := init.On(phase1Done, "entries-committed")
	afterCommit.On(newLeaderCommitOK, netrix.SuccessState).MarkSuccess()
	afterCommit.On(newLeaderCommitBehind, netrix.FailureState)

	filters := netrix.NewFilterSet()
	// Isolate node 1 once we've seen it commit beyond the bootstrap index.
	filters.AddFilter(netrix.If(
		netrix.Condition(func(_ *netrix.Event, ctx *netrix.Context) bool {
			recordedCommit, _ := netrix.GetVar[uint64](ctx, recordedCommitKey)
			return recordedCommit > 0
		}).And(part.IsHealed()),
	).Then(part.IsolateAction(), netrix.DeliverMessage()))
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "new-leader-has-all-committed-entries",
		MaxRounds:    300,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 5 {
				_ = e.Propose(0, []byte("a"))
				_ = e.Propose(0, []byte("b"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "LeaderCompletenessInv violated: new leader is missing committed entries")
	require.True(t, result.Success,
		"expected new leader to catch up to committed index, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestTermMonotonicity verifies that terms are strictly non-decreasing per
// node across all messages: a node never sends a message with a term lower
// than a term it previously sent (a necessary safety property).
func TestTermMonotonicity(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	part := netrix.IsolateNode(3)

	termDecreased := netrix.Condition(func(e *netrix.Event, ctx *netrix.Context) bool {
		maxTermSeen := uint64ByNode(ctx, maxTermSeenKey) // nodeID → highest term seen in any message from it
		nodeID := e.From
		term := e.Msg.GetTerm()
		if term == 0 {
			return false // skip local/internal messages with no term
		}
		if prev, ok := maxTermSeen[nodeID]; ok && term < prev {
			return true // term went backwards
		}
		if term > maxTermSeen[nodeID] {
			maxTermSeen[nodeID] = term
		}
		return false
	})

	sm := netrix.NewStateMachine()
	sm.Builder().On(termDecreased, netrix.FailureState)

	filters := netrix.NewFilterSet()
	// Partition then heal to drive multiple elections.
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "term-monotonicity",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		ResetFunc: func() {
			part.Heal()
		},
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 20 {
				part.Isolate()
			}
			if round == 80 {
				part.Heal()
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "term monotonicity violated: a node sent a message with a lower term")
}
