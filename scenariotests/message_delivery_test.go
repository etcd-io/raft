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

// TestDelayedVotesStillElect verifies that buffering all MsgVote messages
// for several rounds and then releasing them still results in a successful
// election (TLA+ DuplicateMessage / delayed delivery model).
func TestDelayedVotesStillElect(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	voteBuf := netrix.Set("votes")
	voteCount := netrix.Count("vote-count")
	released := netrix.Count("released")

	sm := netrix.NewStateMachine()
	sm.Builder().On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Buffer votes for the first 5 rounds (while vote count < 3).
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(voteCount.Lt(3)),
	).Then(voteCount.Incr(), voteBuf.Store()))
	// On the 4th vote (or later), flush the buffer and deliver current too.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgVote).And(voteCount.Geq(3)).And(released.Lt(1)),
	).Then(
		released.Incr(),
		voteBuf.DeliverAll(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "delayed-votes-still-elect",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"election should succeed despite delayed vote delivery, final state: %s", result.FinalState)
}

// TestDuplicateAppendEntryHandled verifies that a duplicate MsgApp (the same
// message delivered twice) is handled idempotently — the cluster still makes
// progress. This covers the TLA+ DuplicateMessage action.
func TestDuplicateAppendEntryHandled(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	appBuf := netrix.Set("first-app")
	appCount := netrix.Count("app-count")
	replayed := netrix.Count("replayed")

	// After duplicate delivery, the follower still acknowledges.
	sm := netrix.NewStateMachine()
	sm.Builder().On(appRespSuccess().And(replayed.Geq(1)), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Record the first entry-carrying MsgApp to node 2.
	filters.AddFilter(netrix.If(
		hasEntries().And(netrix.IsMessageTo(2)).And(appCount.Lt(1)),
	).Then(
		appCount.Incr(),
		appBuf.Store(),          // buffer
		netrix.DeliverMessage(), // also deliver normally
	))
	// On the next round, re-deliver the stored copy (simulating duplicate).
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgHeartbeat).And(replayed.Lt(1)).And(appCount.Geq(1)),
	).Then(
		replayed.Incr(),
		appBuf.DeliverAll(), // flush duplicate
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "duplicate-append-entry-handled",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 5 {
				_ = e.Propose(0, []byte("dup"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"cluster should progress despite duplicate AppendEntries, final state: %s", result.FinalState)
}

// TestDroppedHeartbeatsNoLeaderLoss verifies that dropping all MsgHeartbeat
// messages does not cause the cluster to lose its leader, as long as entries
// are still being replicated. The initial no-op MsgApp keeps followers up to date.
func TestDroppedHeartbeatsNoLeaderLoss(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	// After election, leader should keep sending MsgApp (either entries or
	// no-ops) even without heartbeats.
	appCount := netrix.Count("apps")
	sm := netrix.NewStateMachine()
	sm.Builder().On(appCount.Geq(5), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Drop all heartbeat messages in both directions.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgHeartbeat).Or(netrix.IsMessageType(pb.MsgHeartbeatResp)),
	).Then(netrix.DropMessage()))
	// Count MsgApp deliveries.
	filters.AddFilter(netrix.If(netrix.IsMessageType(pb.MsgApp)).Then(
		appCount.Incr(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "dropped-heartbeats-no-leader-loss",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round%10 == 3 {
				_ = e.Propose(0, []byte("x"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"cluster should keep making progress despite dropped heartbeats, final state: %s", result.FinalState)
}

// TestOutOfOrderAppendHandled verifies that delivering MsgApp messages
// out of order to a follower is handled correctly: the follower eventually
// acknowledges the entries and the leader commits them. This exercises the
// LogMatchingInv property — the follower reconciles its log correctly.
func TestOutOfOrderAppendHandled(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	appBuf := netrix.Set("app-buffer")
	buffered := netrix.Count("buffered")
	delivered := netrix.Count("delivered")

	// Success: follower node 2 eventually acknowledges after out-of-order delivery.
	sm := netrix.NewStateMachine()
	sm.Builder().On(
		appRespSuccess().And(netrix.IsMessageFrom(2)).And(delivered.Geq(1)),
		netrix.SuccessState,
	).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Buffer the first 2 entry-carrying MsgApp messages to node 2.
	filters.AddFilter(netrix.If(
		hasEntries().And(netrix.IsMessageTo(2)).And(buffered.Lt(2)),
	).Then(buffered.Incr(), appBuf.Store()))
	// On the 3rd MsgApp, flush the buffer in reverse order by delivering stored last.
	filters.AddFilter(netrix.If(
		hasEntries().And(netrix.IsMessageTo(2)).And(buffered.Geq(2)).And(delivered.Lt(1)),
	).Then(
		delivered.Incr(),
		netrix.DeliverMessage(), // deliver current (latest) first
		appBuf.DeliverAll(),     // then deliver buffered (earlier) entries
	))

	tc := &netrix.TestCase{
		Name:         "out-of-order-append-handled",
		MaxRounds:    150,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			switch round {
			case 5:
				_ = e.Propose(0, []byte("first"))
			case 7:
				_ = e.Propose(0, []byte("second"))
			case 9:
				_ = e.Propose(0, []byte("third"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"follower should handle out-of-order AppendEntries correctly, final state: %s", result.FinalState)
}
