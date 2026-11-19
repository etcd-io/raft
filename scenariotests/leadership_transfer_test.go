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

// TestLeadershipTransferComplete verifies the full leadership transfer
// protocol: MsgTimeoutNow from current leader → MsgVote from transferee →
// MsgApp from new leader (campaignTransfer path).
func TestLeadershipTransferComplete(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	transferInitiated := false

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Leader (node 1) sends MsgTimeoutNow to transferee (node 3).
	timeoutNow := init.On(
		netrix.IsMessageType(pb.MsgTimeoutNow).And(netrix.IsMessageTo(3)),
		"timeout-now",
	)
	// Transferee (node 3) starts an election.
	newVote := timeoutNow.On(
		netrix.IsMessageType(pb.MsgVote).And(netrix.IsMessageFrom(3)),
		"new-vote",
	)
	// Transferee (node 3) becomes the new leader.
	newVote.On(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(3)),
		netrix.SuccessState,
	).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "leadership-transfer-complete",
		MaxRounds:    150,
		StateMachine: sm,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 15 && !transferInitiated {
				transferInitiated = true
				// Transfer leadership from node 1 (index 0) to node 3 (ID=3).
				e.Nodes[0].TransferLeader(3)
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected complete leadership transfer to node 3, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}

// TestLeadershipTransferCatchesUpLaggingTarget verifies that the leader
// catches up a lagging transferee with MsgApp entries before sending
// MsgTimeoutNow, ensuring the transferee is up to date before taking over.
func TestLeadershipTransferCatchesUpLaggingTarget(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv {
		return newEnvWithOpts(t, 3, func(c *raft.Config) {
			c.ElectionTick = 20
			c.HeartbeatTick = 1
		})
	}

	part := netrix.IsolateNode(3)
	var transferInitiated bool

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	// Leader first catches up node 3 with entries.
	catchUp := init.On(
		hasEntries().And(netrix.IsMessageTo(3)),
		"catch-up",
	)
	// Then sends MsgTimeoutNow to the now-current node 3.
	timeoutNow := catchUp.On(
		netrix.IsMessageType(pb.MsgTimeoutNow).And(netrix.IsMessageTo(3)),
		"timeout-now",
	)
	// Node 3 takes over as leader.
	timeoutNow.On(
		netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageFrom(3)),
		netrix.SuccessState,
	).MarkSuccess()

	filters := netrix.NewFilterSet()
	filters.AddFilter(part.Filter())

	tc := &netrix.TestCase{
		Name:         "leadership-transfer-catches-up-lagging-target",
		MaxRounds:    200,
		StateMachine: sm,
		Filters:      filters,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			switch round {
			case 5:
				part.Isolate()
			case 8:
				// Propose entries while node 3 is lagging.
				_ = e.Propose(0, []byte("entry"))
			case 20:
				part.Heal()
			case 25:
				if !transferInitiated {
					transferInitiated = true
					e.Nodes[0].TransferLeader(3)
				}
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected leader to catch up transferee before handing off leadership, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}
