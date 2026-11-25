// Copyright 2026 The etcd Authors
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

//go:build with_tla

package raft_test

import (
	"bytes"
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/datadriven"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/rafttest"
)

type traceLogRecord struct {
	TS    int64              `json:"ts"`
	Tag   string             `json:"tag"`
	Event *raft.TracingEvent `json:"event"`
}

type ndjsonTraceLogger struct {
	mu      sync.Mutex
	seq     int64
	records []traceLogRecord
}

func (l *ndjsonTraceLogger) TraceEvent(ev *raft.TracingEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	l.records = append(l.records, traceLogRecord{
		TS:    l.seq,
		Tag:   "trace",
		Event: ev,
	})
}

func (l *ndjsonTraceLogger) ndjson() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var buf bytes.Buffer
	for _, rec := range l.records {
		line, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func nodeEntryTerm(t *testing.T, n *rafttest.Node, index uint64) uint64 {
	t.Helper()
	term, err := n.Storage.Term(index)
	if err != nil {
		t.Fatalf("node %d Term(%d): %v", n.Config.ID, index, err)
	}
	return term
}

// TestTraceIndirectCommitLeaderCompleteness reproduces a 3-node Raft execution
// where entry 3 (written in term 1) is indirectly committed in term 3 while a
// stale leader from term 2 still holds a conflicting entry at index 3 (term 2).
//
// Set ETCD_TLA_TRACE_OUT=/path/to/trace.ndjson to dump the captured NDJSON
// trace for validation with tla/Traceetcdraft.tla.
func TestTraceIndirectCommitLeaderCompleteness(t *testing.T) {
	traceLog := &ndjsonTraceLogger{}
	env := rafttest.NewInteractionEnv(&rafttest.InteractionOpts{
		OnConfig: func(c *raft.Config) {
			c.TraceLogger = traceLog
		},
		SetRandomizedElectionTimeout: raft.SetRandomizedElectionTimeout,
	})

	datadriven.RunTest(t, "testdata/leader_completeness_indirect_commit.txt", func(t *testing.T, d *datadriven.TestData) string {
		return env.Handle(t, *d)
	})

	if len(env.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(env.Nodes))
	}

	st1 := env.Nodes[0].BasicStatus()
	st2 := env.Nodes[1].BasicStatus()
	st3 := env.Nodes[2].BasicStatus()

	if st1.RaftState != raft.StateLeader || st1.GetTerm() != 3 || st1.GetCommit() != 4 {
		t.Fatalf("node 1 unexpected status: state=%s term=%d commit=%d", st1.RaftState, st1.GetTerm(), st1.GetCommit())
	}
	if st2.RaftState != raft.StateLeader || st2.GetTerm() != 2 || st2.GetCommit() != 2 {
		t.Fatalf("node 2 unexpected status: state=%s term=%d commit=%d", st2.RaftState, st2.GetTerm(), st2.GetCommit())
	}
	if st3.RaftState != raft.StateFollower || st3.GetTerm() != 3 || st3.GetCommit() != 4 {
		t.Fatalf("node 3 unexpected status: state=%s term=%d commit=%d", st3.RaftState, st3.GetTerm(), st3.GetCommit())
	}

	if got := nodeEntryTerm(t, &env.Nodes[0], 3); got != 1 {
		t.Fatalf("node 1 entry 3 term = %d, want 1", got)
	}
	if got := nodeEntryTerm(t, &env.Nodes[0], 4); got != 3 {
		t.Fatalf("node 1 entry 4 term = %d, want 3", got)
	}
	if got := nodeEntryTerm(t, &env.Nodes[1], 3); got != 2 {
		t.Fatalf("node 2 entry 3 term = %d, want 2", got)
	}
	if got := nodeEntryTerm(t, &env.Nodes[2], 3); got != 1 {
		t.Fatalf("node 3 entry 3 term = %d, want 1", got)
	}
	if got := nodeEntryTerm(t, &env.Nodes[2], 4); got != 3 {
		t.Fatalf("node 3 entry 4 term = %d, want 3", got)
	}

	raw, err := traceLog.ndjson()
	if err != nil {
		t.Fatalf("marshal NDJSON trace: %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	if len(lines) == 0 {
		t.Fatalf("expected non-empty NDJSON trace")
	}

	var sawNode2LeaderT2, sawNode1LeaderT3, sawNode1Commit4T3 bool
	for i, line := range lines {
		var rec traceLogRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("line %d: unmarshal trace record: %v", i+1, err)
		}
		if rec.Tag != "trace" || rec.Event == nil {
			t.Fatalf("line %d: invalid trace record tag=%q event=%v", i+1, rec.Tag, rec.Event)
		}
		ev := rec.Event
		if ev.Name == "BecomeLeader" && ev.NodeID == "2" && ev.State.Term == 2 {
			sawNode2LeaderT2 = true
		}
		if ev.Name == "BecomeLeader" && ev.NodeID == "1" && ev.State.Term == 3 {
			sawNode1LeaderT3 = true
		}
		if ev.Name == "Commit" && ev.NodeID == "1" && ev.State.Term == 3 && ev.State.Commit == 4 {
			sawNode1Commit4T3 = true
		}
	}
	if !sawNode2LeaderT2 || !sawNode1LeaderT3 || !sawNode1Commit4T3 {
		t.Fatalf("missing expected trace events: node2LeaderT2=%v node1LeaderT3=%v node1Commit4T3=%v",
			sawNode2LeaderT2, sawNode1LeaderT3, sawNode1Commit4T3)
	}

	if outPath := os.Getenv("ETCD_TLA_TRACE_OUT"); outPath != "" {
		if err := os.WriteFile(outPath, raw, 0o644); err != nil {
			t.Fatalf("write ETCD_TLA_TRACE_OUT %q: %v", outPath, err)
		}
	}
}
