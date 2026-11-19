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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/rafttest"
	"go.etcd.io/raft/v3/scenariotests/netrix"
)

const scenarioTestIterations = 20

func runNetrixTest(t *testing.T, tc *netrix.TestCase, envFunc func() *rafttest.InteractionEnv) netrix.RunResult {
	t.Helper()
	tc.Iterations = scenarioTestIterations
	tc.EnvFunc = envFunc
	return netrix.Run(tc, nil)
}

// newEnv creates an n-node raft cluster (voters 1..n) bootstrapped at index 10.
func newEnv(t *testing.T, n int) *rafttest.InteractionEnv {
	t.Helper()
	return newEnvWithOpts(t, n, nil)
}

// newEnvWithOpts creates an n-node cluster and calls mutate on the config before adding nodes.
func newEnvWithOpts(t *testing.T, n int, mutate func(*raft.Config)) *rafttest.InteractionEnv {
	t.Helper()
	env := rafttest.NewInteractionEnv(nil)
	cfg := raft.Config{
		ElectionTick:    10,
		HeartbeatTick:   1,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: math.MaxInt32,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	voters := make([]uint64, n)
	for i := range voters {
		voters[i] = uint64(i + 1)
	}
	snap := pb.EnsureSnapshot(nil)
	snap.Metadata.ConfState.Voters = voters
	snap.Metadata.Index = new(uint64(10))
	snap.Metadata.Term = new(uint64(1))
	require.NoError(t, env.AddNodes(n, cfg, snap))
	return env
}

// tickAll advances all nodes by one tick.
func tickAll(env *rafttest.InteractionEnv) {
	for i := range env.Nodes {
		_ = env.Tick(i, 1)
	}
}

// hasEntries is a Condition that matches MsgApp messages carrying non-empty log entries.
func hasEntries() netrix.Condition {
	return func(e *netrix.Event, _ *netrix.Context) bool {
		return e.Msg.GetType() == pb.MsgApp && len(e.Msg.GetEntries()) > 0
	}
}

// voteGranted is a Condition that matches MsgVoteResp with Reject=false.
func voteGranted() netrix.Condition {
	return func(e *netrix.Event, _ *netrix.Context) bool {
		return e.Msg.GetType() == pb.MsgVoteResp && !e.Msg.GetReject()
	}
}

// preVoteGranted is a Condition that matches MsgPreVoteResp with Reject=false.
func preVoteGranted() netrix.Condition {
	return func(e *netrix.Event, _ *netrix.Context) bool {
		return e.Msg.GetType() == pb.MsgPreVoteResp && !e.Msg.GetReject()
	}
}

// appRespSuccess is a Condition that matches MsgAppResp with Reject=false.
func appRespSuccess() netrix.Condition {
	return func(e *netrix.Event, _ *netrix.Context) bool {
		return e.Msg.GetType() == pb.MsgAppResp && !e.Msg.GetReject()
	}
}

// hasConfChangeEntry is a Condition true for MsgApp carrying a ConfChange entry.
func hasConfChangeEntry() netrix.Condition {
	return func(e *netrix.Event, _ *netrix.Context) bool {
		if e.Msg.GetType() != pb.MsgApp {
			return false
		}
		for _, ent := range e.Msg.GetEntries() {
			if ent.GetType() == pb.EntryConfChange || ent.GetType() == pb.EntryConfChangeV2 {
				return true
			}
		}
		return false
	}
}

// fromAnyOf is a Condition true when the message's From ID is in the given set.
func fromAnyOf(ids ...uint64) netrix.Condition {
	set := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return func(e *netrix.Event, _ *netrix.Context) bool {
		return set[e.From]
	}
}

// toAnyOf is a Condition true when the message's To ID is in the given set.
func toAnyOf(ids ...uint64) netrix.Condition {
	set := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return func(e *netrix.Event, _ *netrix.Context) bool {
		return set[e.To]
	}
}
