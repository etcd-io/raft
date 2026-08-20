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
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/rafttest"
	"go.etcd.io/raft/v3/scenariotests/netrix"
)

// TestPreVoteMessagesSentBeforeRealVote verifies that with PreVote enabled,
// the cluster sends MsgPreVote before MsgVote, implementing the pre-vote
// protocol to prevent term disruption.
func TestPreVoteMessagesSentBeforeRealVote(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv {
		return newEnvWithOpts(t, 3, func(c *raft.Config) { c.PreVote = true })
	}

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	prevoting := init.On(netrix.IsMessageType(pb.MsgPreVote), "prevoting")
	voting := prevoting.On(netrix.IsMessageType(pb.MsgVote), "voting")
	voting.On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "prevote-before-real-vote",
		MaxRounds:    100,
		StateMachine: sm,
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected PreVote→Vote→MsgApp sequence, final state: %s", result.FinalState)
}

// TestPreVoteGrantedLeadsToElection verifies that a granted PreVote response
// causes the candidate to proceed with a real MsgVote.
func TestPreVoteGrantedLeadsToElection(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv {
		return newEnvWithOpts(t, 3, func(c *raft.Config) { c.PreVote = true })
	}

	// Track which node sent the granted PreVoteResp.
	var preVoteGrantedFrom uint64

	preVoteGrantedCond := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() == pb.MsgPreVoteResp && !e.Msg.GetReject() {
			preVoteGrantedFrom = e.To // the candidate receiving the grant
			return true
		}
		return false
	})

	realVoteFollows := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		return preVoteGrantedFrom != 0 &&
			e.Msg.GetType() == pb.MsgVote &&
			e.From == preVoteGrantedFrom
	})

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	granted := init.On(preVoteGrantedCond, "pre-granted")
	granted.On(realVoteFollows, netrix.SuccessState).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "prevote-granted-leads-to-election",
		MaxRounds:    100,
		StateMachine: sm,
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected real MsgVote to follow granted PreVote, final state: %s", result.FinalState)
}

// TestPreVotePreventsTrumpByStaleNode verifies that PreVote prevents a
// partitioned node with an inflated term from disrupting the current leader
// when it rejoins. With PreVote, the stale node's pre-vote is rejected because
// the cluster is healthy; the cluster leader is not disrupted.
func TestPreVotePreventsTrumpByStaleNode(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv {
		return newEnvWithOpts(t, 3, func(c *raft.Config) { c.PreVote = true })
	}

	part := netrix.IsolateNode(3)

	// The leader (node 1) should continue to send heartbeats after the stale
	// node rejoins, indicating it was NOT disrupted.
	hbAfterHeal := netrix.IsMessageType(pb.MsgHeartbeat).And(netrix.IsMessageFrom(1)).And(part.IsHealed())

	// Failure: a MsgVote with a very high term from node 3 reaches the cluster
	// BEFORE a pre-vote exchange (would indicate PreVote was bypassed).
	var leaderTerm uint64
	directHighTermVote := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() == pb.MsgApp && e.From == 1 && leaderTerm == 0 {
			leaderTerm = e.Msg.GetTerm()
		}
		// A real MsgVote from node 3 with term more than 1 above the leader's term
		// would mean the stale node is disrupting without going through PreVote.
		return !part.Active() && leaderTerm > 0 &&
			e.Msg.GetType() == pb.MsgVote && e.From == 3 &&
			e.Msg.GetTerm() > leaderTerm+1
	})

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	init.On(directHighTermVote, netrix.FailureState)
	init.On(hbAfterHeal, netrix.SuccessState).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "prevote-prevents-trump-by-stale-node",
		MaxRounds:    300,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 15 {
				part.Isolate()
			}
			if round == 120 {
				part.Heal()
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(), "PreVote should prevent stale node from directly disrupting the leader")
	require.True(t, result.Success,
		"expected leader to continue sending heartbeats after stale node rejoins, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}
