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

// TestLeaderSendsEntriesToFollowers verifies that after election the leader
// replicates proposed entries via MsgApp (TLA+ AppendEntries action).
func TestLeaderSendsEntriesToFollowers(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	sm := netrix.NewStateMachine()
	sm.Builder().On(hasEntries(), netrix.SuccessState).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "leader-sends-entries",
		MaxRounds:    50,
		StateMachine: sm,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 5 {
				_ = e.Propose(0, []byte("hello"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected leader to send log entries to followers, final state: %s", result.FinalState)
}

// TestFollowerAcknowledgesAppend verifies that followers send MsgAppResp
// after receiving entries, which is required for the leader to advance
// commitIndex (TLA+ AdvanceCommitIndex action).
func TestFollowerAcknowledgesAppend(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	acks := netrix.Count("acks")
	sm := netrix.NewStateMachine()
	sm.Builder().On(acks.Geq(2), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(netrix.If(appRespSuccess()).Then(
		acks.Incr(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "follower-acknowledges-append",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 5 {
				_ = e.Propose(0, []byte("data"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected both followers to acknowledge appended entries, final state: %s", result.FinalState)
}

// TestHeartbeatsSentByLeader verifies that a leader periodically sends
// MsgHeartbeat messages to maintain its leadership (TLA+ Heartbeat(i,j) action).
func TestHeartbeatsSentByLeader(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	hb := netrix.Count("hb")
	sm := netrix.NewStateMachine()
	sm.Builder().On(hb.Geq(3), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(netrix.If(netrix.IsMessageType(pb.MsgHeartbeat)).Then(
		hb.Incr(),
		netrix.DeliverMessage(),
	))

	tc := &netrix.TestCase{
		Name:         "heartbeats-sent-by-leader",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected at least 3 heartbeats from leader, final state: %s", result.FinalState)
}

// TestDroppedAppendRetriedByLeader verifies that the leader retries dropped
// MsgApp messages, ensuring eventual delivery despite unreliable networks.
func TestDroppedAppendRetriedByLeader(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	dropped := netrix.Count("dropped")

	// After we drop the first entry MsgApp to node 2, the leader will retry.
	retryDetected := netrix.IsMessageType(pb.MsgApp).
		And(hasEntries()).
		And(netrix.IsMessageTo(2)).
		And(dropped.Geq(1))

	sm := netrix.NewStateMachine()
	sm.Builder().On(retryDetected, netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Drop the first MsgApp with entries to node 2.
	filters.AddFilter(netrix.If(
		netrix.IsMessageType(pb.MsgApp).And(hasEntries()).And(netrix.IsMessageTo(2)).And(dropped.Lt(1)),
	).Then(dropped.Incr(), netrix.DropMessage()))

	tc := &netrix.TestCase{
		Name:         "dropped-append-retried",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 5 {
				_ = e.Propose(0, []byte("x"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "expected leader to retry dropped AppendEntries, final state: %s", result.FinalState)
}

// TestCommitRequiresMajority verifies that commit can proceed with only a
// majority quorum, even when minority nodes are unreachable (QuorumLogInv).
// In a 5-node cluster, commit proceeds when 3 nodes (majority) acknowledge.
func TestCommitRequiresMajority(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 5) }

	// Count only acknowledgements from nodes in the majority partition.
	acks := netrix.Count("majority-acks")
	majorityAck := appRespSuccess().And(fromAnyOf(2, 3))

	sm := netrix.NewStateMachine()
	sm.Builder().On(acks.Geq(2), netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Permanently partition nodes 4 and 5.
	filters.AddFilter(netrix.If(toAnyOf(4, 5).Or(fromAnyOf(4, 5))).Then(netrix.DropMessage()))
	// Count acks from the majority.
	filters.AddFilter(netrix.If(majorityAck).Then(acks.Incr(), netrix.DeliverMessage()))

	tc := &netrix.TestCase{
		Name:         "commit-requires-majority",
		MaxRounds:    100,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 5 {
				_ = e.Propose(0, []byte("data"))
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success, "commit should proceed with majority quorum only, final state: %s", result.FinalState)
}
