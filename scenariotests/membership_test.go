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

// TestAddServerViaConfChange verifies that proposing a ConfChange to add a
// new server causes the leader to replicate a ConfChange entry (MsgApp with
// EntryConfChange/V2), covering the TLA+ AddNewServer action.
func TestAddServerViaConfChange(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }

	sm := netrix.NewStateMachine()
	sm.Builder().On(hasConfChangeEntry(), netrix.SuccessState).MarkSuccess()

	proposed := false
	tc := &netrix.TestCase{
		Name:         "add-server-via-conf-change",
		MaxRounds:    100,
		StateMachine: sm,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 10 && !proposed {
				proposed = true
				_ = e.ProposeConfChange(0, &pb.ConfChangeV2{
					Changes: []*pb.ConfChangeSingle{{
						Type:   pb.ConfChangeAddNode.Enum(),
						NodeId: new(uint64(4)),
					}},
				})
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected leader to replicate ConfChange entry for new server, final state: %s", result.FinalState)
}

// TestAddLearnerReceivesReplication verifies that after a learner is added
// via ConfChange, it receives MsgApp log replication from the leader
// (TLA+ AddLearner action).
func TestAddLearnerReceivesReplication(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 3) }
	require := require.New(t)

	proposed := false
	learnerReplicated := netrix.IsMessageType(pb.MsgApp).And(netrix.IsMessageTo(4))

	sm := netrix.NewStateMachine()
	sm.Builder().On(learnerReplicated, netrix.SuccessState).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "add-learner-receives-replication",
		MaxRounds:    150,
		StateMachine: sm,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 10 && !proposed {
				proposed = true
				_ = e.ProposeConfChange(0, &pb.ConfChangeV2{
					Changes: []*pb.ConfChangeSingle{{
						Type:   pb.ConfChangeAddLearnerNode.Enum(),
						NodeId: new(uint64(4)),
					}},
				})
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error {
			// Add node 4 as a learner to the environment.
			nodeCfg := *e.Nodes[0].Config
			nodeCfg.Logger = nil
			if err := e.AddNodes(1, nodeCfg, nil); err != nil {
				return err
			}
			return e.Campaign(0)
		},
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(result.Err)
	require.True(result.Success,
		"expected learner to receive MsgApp after being added, final state: %s", result.FinalState)
}

// TestRemoveServerNoLeadershipLoss verifies that removing a follower via
// ConfChange does not cause a leadership disruption (TLA+ DeleteServer action).
// The leader should continue sending heartbeats after the removal is applied.
func TestRemoveServerNoLeadershipLoss(t *testing.T) {
	envFunc := func() *rafttest.InteractionEnv { return newEnv(t, 4) }

	var removalProposed bool
	var leaderID uint64

	// Track the first leader.
	leaderDetect := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() == pb.MsgApp && leaderID == 0 {
			leaderID = e.From
		}
		return false
	})

	// After removal, the leader should send heartbeats — not step down.
	leaderContinues := netrix.Condition(func(e *netrix.Event, _ *netrix.Context) bool {
		return removalProposed &&
			leaderID != 0 &&
			e.Msg.GetType() == pb.MsgHeartbeat &&
			e.From == leaderID
	})

	sm := netrix.NewStateMachine()
	init := sm.Builder()
	init.On(leaderDetect, "__initial__") // side-effect only, stays in initial
	init.On(leaderContinues, netrix.SuccessState).MarkSuccess()

	tc := &netrix.TestCase{
		Name:         "remove-server-no-leadership-loss",
		MaxRounds:    200,
		StateMachine: sm,
		TickFunc: func(e *rafttest.InteractionEnv, round int) {
			tickAll(e)
			if round == 20 && !removalProposed {
				removalProposed = true
				_ = e.ProposeConfChange(0, &pb.ConfChangeV2{
					Changes: []*pb.ConfChangeSingle{{
						Type:   pb.ConfChangeRemoveNode.Enum(),
						NodeId: new(uint64(4)),
					}},
				})
			}
		},
		SetupFunc: func(e *rafttest.InteractionEnv) error { return e.Campaign(0) },
	}
	result := runNetrixTest(t, tc, envFunc)
	require.NoError(t, result.Err)
	require.True(t, result.Success,
		"expected leader to continue after server removal, final state: %s after %d rounds",
		result.FinalState, result.Rounds)
}
