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

// TestCheckQuorumLeaderStepsDown verifies that a leader steps down when it
// cannot hear from a quorum within ElectionTick rounds (StepDownToFollower
// action from the TLA+ spec). After stepping down the former leader responds
// to the new election with MsgVoteResp, which is the observable signal.
func TestCheckQuorumLeaderStepsDown(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv {
		return newEnvWithOpts(t, 3, func(c *raft.Config) {
			c.CheckQuorum = true
			c.ElectionTick = 5
			c.HeartbeatTick = 1
		})
	}

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Confirm node 1 is the leader.
	afterLeader := init.On(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(1)),
		"leader-seen",
	)
	// Node 1 steps down and grants a vote as follower.
	afterLeader.On(
		netrix.IsMessageType(pb.MsgVoteResp).And(netrix.IsMessageFrom(1)),
		netrix.SuccessState,
	).MarkSuccess()

	filters := netrix.NewFilterSet()
	// Drop all responses going back to node 1 (heartbeat responses, AppResp).
	filters.AddFilter(netrix.If(
		netrix.IsMessageTo(1).And(
			netrix.IsMessageType(pb.MsgHeartbeatResp).Or(netrix.IsMessageType(pb.MsgAppResp)),
		),
	).Then(netrix.DropMessage()))

	tc := &netrix.TestCase{
		Name:         "checkquorum-leader-steps-down",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		TickFunc:     func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc:    func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected leader to step down when quorum unresponsive (CheckQuorum), final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestCheckQuorumNotTriggeredWithActiveFollowers verifies that a leader does
// NOT step down prematurely when followers are actively responding to heartbeats.
func TestCheckQuorumNotTriggeredWithActiveFollowers(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv {
		return newEnvWithOpts(t, 3, func(c *raft.Config) {
			c.CheckQuorum = true
			c.ElectionTick = 5
			c.HeartbeatTick = 1
		})
	}

	// Track the first leader to detect premature step-down.
	var leaderID uint64

	prematureStepdown := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() == pb.MsgApp && leaderID == 0 {
			leaderID = e.From
		}
		// If the leader sends MsgVote it stepped down and started a new election.
		return leaderID != 0 && e.Msg.GetType() == pb.MsgVote && e.From == leaderID
	})

	sm := netrix.NewStateMachine()
	sm.Builder().On(prematureStepdown, netrix.FailureState)

	tc := &netrix.TestCase{
		Name:         "checkquorum-not-triggered-with-active-followers",
		MaxRounds:    50,
		StateMachine: sm,
		ResetFunc: func() {
			leaderID = 0
		},
		TickFunc:  func(e *rafttest.InteractionEnv, _ int) { tickAll(e) },
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.False(t, result.IsFailure(),
		"leader should not step down when followers are responding (premature CheckQuorum)")
}
