// Copyright 2019 The etcd Authors
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

package raft_test

import (
	"math"
	"testing"

	"github.com/cockroachdb/datadriven"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/rafttest"
)

func TestInteraction(t *testing.T) {
	// NB: if this test fails, run `go test ./raft -rewrite` and inspect the
	// diff. Only commit the changes if you understand what caused them and if
	// they are desired.
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		env := rafttest.NewInteractionEnv(&rafttest.InteractionOpts{
			SetRandomizedElectionTimeout: raft.SetRandomizedElectionTimeout,
		})
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return env.Handle(t, *d)
		})
	})
}

func TestAsyncStorageWritesCandidateCrashSafety(t *testing.T) {
	const initialIndex = uint64(10)

	newEnv := func(preVote bool) *rafttest.InteractionEnv {
		snapshot := raftpb.EnsureSnapshot(nil)
		snapshot.Metadata.Index = new(initialIndex)
		snapshot.Metadata.Term = new(uint64(1))
		snapshot.Metadata.ConfState = &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		cfg := raft.Config{
			ElectionTick:    10,
			HeartbeatTick:   1,
			MaxSizePerMsg:   math.MaxUint64,
			MaxInflightMsgs: math.MaxInt32,
			Applied:         initialIndex,
			PreVote:         preVote,
		}
		env := rafttest.NewInteractionEnv(&rafttest.InteractionOpts{
			OnConfig: func(cfg *raft.Config) {
				cfg.AsyncStorageWrites = cfg.ID == 1
			},
		})
		if err := env.AddNodes(3, cfg, snapshot); err != nil {
			t.Fatalf("test setup: add nodes: %v", err)
		}
		return env
	}

	// Check pre-vote independently: its higher-term responses can otherwise
	// advance the restarted candidate's term and mask the crash schedule below.
	preVoteEnv := newEnv(true)
	if err := preVoteEnv.Campaign(0); err != nil {
		t.Fatalf("test setup: start pre-vote campaign: %v", err)
	}
	if err := preVoteEnv.ProcessReady(0); err != nil {
		t.Fatalf("test setup: process pre-vote Ready: %v", err)
	}
	if n := preVoteEnv.DeliverMsgs(raftpb.MsgPreVote, rafttest.Recipient{ID: 2}, rafttest.Recipient{ID: 3}); n != 2 {
		t.Fatalf("safety invariant: MsgPreVote requests must remain immediately deliverable; delivered %d", n)
	}
	if len(preVoteEnv.Nodes[0].AppendWork) == 0 {
		t.Fatalf("test setup: pre-vote append work was not pending")
	}

	env := newEnv(false)
	if env.Nodes[0].Config.PreVote {
		t.Fatalf("test setup: crash schedule unexpectedly enables PreVote")
	}

	processReady := func(idx int) {
		t.Helper()
		if err := env.ProcessReady(idx); err != nil {
			t.Fatalf("test setup: process Ready for node %d: %v", idx+1, err)
		}
	}
	processAppend := func() {
		t.Helper()
		if err := env.ProcessAppendThread(0); err != nil {
			t.Fatalf("test setup: process node 1 append: %v", err)
		}
	}
	isLeader := func() bool {
		return env.Nodes[0].RawNode.BasicStatus().RaftState == raft.StateLeader
	}
	voteRecipients := []rafttest.Recipient{{ID: 2}, {ID: 3}}
	deliverVoteRound := func() int {
		t.Helper()
		delivered := env.DeliverMsgs(raftpb.MsgVote, voteRecipients...)
		for _, idx := range []int{1, 2} {
			processReady(idx)
		}
		env.DeliverMsgs(raftpb.MsgVoteResp, rafttest.Recipient{ID: 1})
		processReady(0)
		return delivered
	}

	if err := env.Campaign(0); err != nil {
		t.Fatalf("test setup: start campaign: %v", err)
	}
	processReady(0)
	deliverVoteRound()

	// If the vote requests are held back, complete the append work that publishes
	// them. Otherwise, leave node 1's append thread stalled through the crash.
	for attempts := 0; attempts < 8 && !isLeader(); attempts++ {
		if len(env.Nodes[0].AppendWork) == 0 {
			if !env.Nodes[0].RawNode.HasReady() {
				break
			}
			processReady(0)
		}
		if len(env.Nodes[0].AppendWork) == 0 {
			break
		}
		processAppend()
		env.DeliverMsgs(-1, rafttest.Recipient{ID: 1})
		processReady(0)
		deliverVoteRound()
	}
	if !isLeader() {
		t.Fatalf("test setup: node 1 did not become leader after its append work completed")
	}

	// Keep node 1's local append thread stalled while node 2 receives the no-op
	// and x. Node 3 receives neither entry before node 1 crashes.
	processReady(0)
	if n := env.DeliverMsgs(raftpb.MsgApp, rafttest.Recipient{ID: 2}); n == 0 {
		t.Fatalf("test setup: leader did not send its initial entry to node 2")
	}
	processReady(1)
	env.DeliverMsgs(raftpb.MsgAppResp, rafttest.Recipient{ID: 1})
	processReady(0)

	if err := env.Propose(0, []byte("x")); err != nil {
		t.Fatalf("test setup: propose x: %v", err)
	}
	processReady(0)
	if n := env.DeliverMsgs(raftpb.MsgApp, rafttest.Recipient{ID: 2}); n == 0 {
		t.Fatalf("test setup: leader did not send x to node 2")
	}
	processReady(1)
	env.DeliverMsgs(raftpb.MsgAppResp, rafttest.Recipient{ID: 1})
	processReady(0)

	lastIndex, err := env.Nodes[1].Storage.LastIndex()
	if err != nil {
		t.Fatalf("test setup: read node 2 last index: %v", err)
	}
	entries, err := env.Nodes[1].Storage.Entries(lastIndex, lastIndex+1, math.MaxUint64)
	if err != nil || len(entries) != 1 || string(entries[0].GetData()) != "x" {
		t.Fatalf("test setup: node 2 did not persist x at its last index %d: entries=%v err=%v", lastIndex, entries, err)
	}

	// A crash discards node 1's queued append work, but not its MemoryStorage or
	// the state machine's already-applied index.
	node1 := &env.Nodes[0]
	node1.AppendWork = nil
	node1.ApplyWork = nil
	env.Messages = nil
	cfg := *node1.Config
	cfg.Applied = node1.History[len(node1.History)-1].GetMetadata().GetIndex()
	restarted, err := raft.NewRawNode(&cfg)
	if err != nil {
		t.Fatalf("test setup: restart node 1: %v", err)
	}
	node1.RawNode = restarted

	if err := env.Campaign(0); err != nil {
		t.Fatalf("test setup: restart campaign: %v", err)
	}
	if err := env.Stabilize(); err != nil {
		t.Fatalf("test setup: stabilize restarted campaign: %v", err)
	}
	if !isLeader() {
		status := env.Nodes[0].RawNode.BasicStatus()
		t.Fatalf("test setup: restarted node 1 did not become leader: status=%+v trace:\n%s", status, env.Output.Builder.String())
	}
	if err := env.Propose(0, []byte("y")); err != nil {
		t.Fatalf("test setup: propose y: %v", err)
	}
	if err := env.Stabilize(); err != nil {
		t.Fatalf("test setup: stabilize y: %v", err)
	}

	type appliedEntry struct {
		index   uint64
		term    uint64
		payload string
	}
	appliedByNode := make([][]appliedEntry, len(env.Nodes))
	for nodeIdx, node := range env.Nodes {
		previous := node.History[0].GetData()
		for _, state := range node.History[1:] {
			data := state.GetData()
			if len(data) < len(previous) {
				t.Fatalf("test setup: node %d history shrank from %d to %d bytes", nodeIdx+1, len(previous), len(data))
			}
			payload := string(data[len(previous):])
			if payload != "" {
				appliedByNode[nodeIdx] = append(appliedByNode[nodeIdx], appliedEntry{
					index:   state.GetMetadata().GetIndex(),
					term:    state.GetMetadata().GetTerm(),
					payload: payload,
				})
			}
			previous = data
		}
	}

	appliedY := func(nodeIdx int) bool {
		for _, entry := range appliedByNode[nodeIdx] {
			if entry.payload == "y" {
				return true
			}
		}
		return false
	}
	if !appliedY(0) || !appliedY(2) {
		t.Fatalf("test setup: schedule did not apply y on nodes 1 and 3")
	}

	type entryID struct {
		index uint64
		term  uint64
	}
	type firstApplication struct {
		nodeIdx int
		payload string
	}
	applications := make(map[entryID]firstApplication)
	for nodeIdx, entries := range appliedByNode {
		for _, entry := range entries {
			id := entryID{index: entry.index, term: entry.term}
			if prev, ok := applications[id]; ok && prev.payload != entry.payload {
				t.Fatalf("safety invariant: replicas %d and %d applied %q and %q at (index, term)=(%d, %d)", prev.nodeIdx+1, nodeIdx+1, prev.payload, entry.payload, entry.index, entry.term)
			}
			applications[id] = firstApplication{nodeIdx: nodeIdx, payload: entry.payload}
		}
	}
}
