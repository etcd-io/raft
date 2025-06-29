// Copyright 2015 The etcd Authors
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

package raft

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3/raftpb"
)

// readyWithTimeout selects from n.Ready() with a 1-second timeout. It
// panics on timeout, which is better than the indefinite wait that
// would occur if this channel were read without being wrapped in a
// select.
func readyWithTimeout(n Node) Ready {
	select {
	case rd := <-n.Ready():
		if nn, ok := n.(*nodeTestHarness); ok {
			n = nn.node
		}
		if nn, ok := n.(*node); ok {
			nn.rn.raft.logger.Infof("emitted ready: %s", DescribeReady(rd, nil))
		}
		return rd
	case <-time.After(time.Second):
		panic("timed out waiting for ready")
	}
}

// TestNodeStep ensures that node.Step sends msgProp to propc chan
// and other kinds of messages to recvc chan.
func TestNodeStep(t *testing.T) {
	for i, msgn := range raftpb.MessageType_name {
		n := &node{
			propc: make(chan msgWithResult, 1),
			recvc: make(chan raftpb.Message, 1),
		}
		msgt := raftpb.MessageType(i)
		n.Step(t.Context(), raftpb.Message{Type: msgt})
		// Proposal goes to proc chan. Others go to recvc chan.
		if msgt == raftpb.MsgProp {
			select {
			case <-n.propc:
			default:
				t.Errorf("%d: cannot receive %s on propc chan", msgt, msgn)
			}
		} else {
			if IsLocalMsg(msgt) {
				select {
				case <-n.recvc:
					t.Errorf("%d: step should ignore %s", msgt, msgn)
				default:
				}
			} else {
				select {
				case <-n.recvc:
				default:
					t.Errorf("%d: cannot receive %s on recvc chan", msgt, msgn)
				}
			}
		}
	}
}

// TestNodeStepUnblock should Cancel and Stop should unblock Step()
func TestNodeStepUnblock(t *testing.T) {
	// a node without buffer to block step
	n := &node{
		propc: make(chan msgWithResult),
		done:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(t.Context())
	stopFunc := func() { close(n.done) }

	tests := []struct {
		unblock func()
		werr    error
	}{
		{stopFunc, ErrStopped},
		{cancel, context.Canceled},
	}

	for i, tt := range tests {
		errc := make(chan error, 1)
		go func() {
			err := n.Step(ctx, raftpb.Message{Type: raftpb.MsgProp})
			errc <- err
		}()
		tt.unblock()
		select {
		case err := <-errc:
			assert.Equal(t, tt.werr, err, "#%d", i)
			// clean up side-effect
			if ctx.Err() != nil {
				ctx = t.Context()
			}
			select {
			case <-n.done:
				n.done = make(chan struct{})
			default:
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("#%d: failed to unblock step", i)
		}
	}
}

// TestNodePropose ensures that node.Propose sends the given proposal to the underlying raft.
func TestNodePropose(t *testing.T) {
	var msgs []raftpb.Message
	appendStep := func(_ *raft, m raftpb.Message) error {
		t.Log(DescribeMessage(m, nil))
		if m.Type == raftpb.MsgAppResp {
			return nil // injected by (*raft).advance
		}
		msgs = append(msgs, m)
		return nil
	}

	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	r := rn.raft
	go n.run()
	require.NoError(t, n.Campaign(t.Context()))
	for {
		rd := <-n.Ready()
		s.Append(rd.Entries)
		// change the step function to appendStep until this raft becomes leader
		if rd.SoftState.Lead == r.id {
			r.step = appendStep
			n.Advance()
			break
		}
		n.Advance()
	}
	n.Propose(t.Context(), []byte("somedata"))
	n.Stop()

	require.Len(t, msgs, 1)
	assert.Equal(t, raftpb.MsgProp, msgs[0].Type)
	assert.Equal(t, []byte("somedata"), msgs[0].Entries[0].Data)
}

// TestDisableProposalForwarding ensures that proposals are not forwarded to
// the leader when DisableProposalForwarding is true.
func TestDisableProposalForwarding(t *testing.T) {
	r1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg3 := newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg3.DisableProposalForwarding = true
	r3 := newRaft(cfg3)
	nt := newNetwork(r1, r2, r3)

	// elect r1 as leader
	nt.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgHup})

	var testEntries = []raftpb.Entry{{Data: []byte("testdata")}}

	// send proposal to r2(follower) where DisableProposalForwarding is false
	r2.Step(raftpb.Message{From: 2, To: 2, Type: raftpb.MsgProp, Entries: testEntries})

	// verify r2(follower) does forward the proposal when DisableProposalForwarding is false
	require.Len(t, r2.msgs, 1)

	// send proposal to r3(follower) where DisableProposalForwarding is true
	r3.Step(raftpb.Message{From: 3, To: 3, Type: raftpb.MsgProp, Entries: testEntries})

	// verify r3(follower) does not forward the proposal when DisableProposalForwarding is true
	require.Empty(t, r3.msgs)
}

// TestNodeReadIndexToOldLeader ensures that raftpb.MsgReadIndex to old leader
// gets forwarded to the new leader and 'send' method does not attach its term.
func TestNodeReadIndexToOldLeader(t *testing.T) {
	r1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	nt := newNetwork(r1, r2, r3)

	// elect r1 as leader
	nt.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgHup})

	var testEntries = []raftpb.Entry{{Data: []byte("testdata")}}

	// send readindex request to r2(follower)
	r2.Step(raftpb.Message{From: 2, To: 2, Type: raftpb.MsgReadIndex, Entries: testEntries})

	// verify r2(follower) forwards this message to r1(leader) with term not set
	require.Len(t, r2.msgs, 1)
	readIndxMsg1 := raftpb.Message{From: 2, To: 1, Type: raftpb.MsgReadIndex, Entries: testEntries}
	require.Equal(t, readIndxMsg1, r2.msgs[0])

	// send readindex request to r3(follower)
	r3.Step(raftpb.Message{From: 3, To: 3, Type: raftpb.MsgReadIndex, Entries: testEntries})

	// verify r3(follower) forwards this message to r1(leader) with term not set as well.
	require.Len(t, r3.msgs, 1)
	readIndxMsg2 := raftpb.Message{From: 3, To: 1, Type: raftpb.MsgReadIndex, Entries: testEntries}
	require.Equal(t, readIndxMsg2, r3.msgs[0])

	// now elect r3 as leader
	nt.send(raftpb.Message{From: 3, To: 3, Type: raftpb.MsgHup})

	// let r1 steps the two messages previously we got from r2, r3
	r1.Step(readIndxMsg1)
	r1.Step(readIndxMsg2)

	// verify r1(follower) forwards these messages again to r3(new leader)
	require.Len(t, r1.msgs, 2)
	readIndxMsg3 := raftpb.Message{From: 2, To: 3, Type: raftpb.MsgReadIndex, Entries: testEntries}
	require.Equal(t, readIndxMsg3, r1.msgs[0])
	readIndxMsg3 = raftpb.Message{From: 3, To: 3, Type: raftpb.MsgReadIndex, Entries: testEntries}
	require.Equal(t, readIndxMsg3, r1.msgs[1])
}

// TestNodeProposeConfig ensures that node.ProposeConfChange sends the given configuration proposal
// to the underlying raft.
func TestNodeProposeConfig(t *testing.T) {
	var msgs []raftpb.Message
	appendStep := func(_ *raft, m raftpb.Message) error {
		if m.Type == raftpb.MsgAppResp {
			return nil // injected by (*raft).advance
		}
		msgs = append(msgs, m)
		return nil
	}

	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	r := rn.raft
	go n.run()
	n.Campaign(t.Context())
	for {
		rd := <-n.Ready()
		s.Append(rd.Entries)
		// change the step function to appendStep until this raft becomes leader
		if rd.SoftState.Lead == r.id {
			r.step = appendStep
			n.Advance()
			break
		}
		n.Advance()
	}
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1}
	ccdata, err := cc.Marshal()
	require.NoError(t, err)
	n.ProposeConfChange(t.Context(), cc)
	n.Stop()

	require.Len(t, msgs, 1)
	assert.Equal(t, raftpb.MsgProp, msgs[0].Type)
	assert.Equal(t, ccdata, msgs[0].Entries[0].Data)
}

// TestNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
func TestNodeProposeAddDuplicateNode(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	ctx, cancel, n := newNodeTestHarness(t.Context(), t, cfg)
	defer cancel()
	n.Campaign(ctx)
	allCommittedEntries := make([]raftpb.Entry, 0)
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	goroutineStopped := make(chan struct{})
	applyConfChan := make(chan struct{})

	rd := readyWithTimeout(n)
	s.Append(rd.Entries)
	n.Advance()

	go func() {
		defer close(goroutineStopped)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.Tick()
			case rd := <-n.Ready():
				t.Log(DescribeReady(rd, nil))
				s.Append(rd.Entries)
				applied := false
				for _, e := range rd.CommittedEntries {
					allCommittedEntries = append(allCommittedEntries, e)
					switch e.Type {
					case raftpb.EntryNormal:
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						cc.Unmarshal(e.Data)
						n.ApplyConfChange(cc)
						applied = true
					}
				}
				n.Advance()
				if applied {
					applyConfChan <- struct{}{}
				}
			}
		}
	}()

	cc1 := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1}
	ccdata1, _ := cc1.Marshal()
	n.ProposeConfChange(ctx, cc1)
	<-applyConfChan

	// try add the same node again
	n.ProposeConfChange(ctx, cc1)
	<-applyConfChan

	// the new node join should be ok
	cc2 := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2}
	ccdata2, _ := cc2.Marshal()
	n.ProposeConfChange(ctx, cc2)
	<-applyConfChan

	cancel()
	<-goroutineStopped

	assert.Len(t, allCommittedEntries, 4)
	assert.Equal(t, ccdata1, allCommittedEntries[1].Data)
	assert.Equal(t, ccdata2, allCommittedEntries[3].Data)
}

// TestBlockProposal ensures that node will block proposal when it does not
// know who is the current leader; node will accept proposal when it knows
// who is the current leader.
func TestBlockProposal(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	go n.run()
	defer n.Stop()

	errc := make(chan error, 1)
	go func() {
		errc <- n.Propose(t.Context(), []byte("somedata"))
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case err := <-errc:
		t.Errorf("err = %v, want blocking", err)
	default:
	}

	n.Campaign(t.Context())
	rd := <-n.Ready()
	s.Append(rd.Entries)
	n.Advance()
	select {
	case err := <-errc:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Errorf("blocking proposal, want unblocking")
	}
}

func TestNodeProposeWaitDropped(t *testing.T) {
	var msgs []raftpb.Message
	droppingMsg := []byte("test_dropping")
	dropStep := func(_ *raft, m raftpb.Message) error {
		if m.Type == raftpb.MsgProp && strings.Contains(m.String(), string(droppingMsg)) {
			t.Logf("dropping message: %v", m.String())
			return ErrProposalDropped
		}
		if m.Type == raftpb.MsgAppResp {
			// This is produced by raft internally, see (*raft).advance.
			return nil
		}
		msgs = append(msgs, m)
		return nil
	}

	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	r := rn.raft
	go n.run()
	n.Campaign(t.Context())
	for {
		rd := <-n.Ready()
		s.Append(rd.Entries)
		// change the step function to dropStep until this raft becomes leader
		if rd.SoftState.Lead == r.id {
			r.step = dropStep
			n.Advance()
			break
		}
		n.Advance()
	}
	proposalTimeout := time.Millisecond * 100
	ctx, cancel := context.WithTimeout(t.Context(), proposalTimeout)
	// propose with cancel should be cancelled earyly if dropped
	assert.Equal(t, ErrProposalDropped, n.Propose(ctx, droppingMsg))
	cancel()

	n.Stop()
	require.Empty(t, msgs)
}

// TestNodeTick ensures that node.Tick() will increase the
// elapsed of the underlying raft state machine.
func TestNodeTick(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	r := rn.raft
	go n.run()
	elapsed := r.electionElapsed
	n.Tick()

	for len(n.tickc) != 0 {
		time.Sleep(100 * time.Millisecond)
	}

	n.Stop()
	assert.Equal(t, elapsed+1, r.electionElapsed)
}

// TestNodeStop ensures that node.Stop() blocks until the node has stopped
// processing, and that it is idempotent
func TestNodeStop(t *testing.T) {
	rn := newTestRawNode(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	n := newNode(rn)
	donec := make(chan struct{})

	go func() {
		n.run()
		close(donec)
	}()

	status := n.Status()
	n.Stop()

	select {
	case <-donec:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for node to stop!")
	}

	emptyStatus := Status{}
	assert.NotEqual(t, emptyStatus, status)

	// Further status should return be empty, the node is stopped.
	assert.Equal(t, emptyStatus, n.Status())

	// Subsequent Stops should have no effect.
	n.Stop()
}

// TestNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
func TestNodeStart(t *testing.T) {
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1}
	ccdata, err := cc.Marshal()
	require.NoError(t, err)
	wants := []Ready{
		{
			HardState: raftpb.HardState{Term: 1, Commit: 1, Vote: 0},
			Entries: []raftpb.Entry{
				{Type: raftpb.EntryConfChange, Term: 1, Index: 1, Data: ccdata},
			},
			CommittedEntries: []raftpb.Entry{
				{Type: raftpb.EntryConfChange, Term: 1, Index: 1, Data: ccdata},
			},
			MustSync: true,
		},
		{
			HardState:        raftpb.HardState{Term: 2, Commit: 2, Vote: 1},
			Entries:          []raftpb.Entry{{Term: 2, Index: 3, Data: []byte("foo")}},
			CommittedEntries: []raftpb.Entry{{Term: 2, Index: 2, Data: nil}},
			MustSync:         true,
		},
		{
			HardState:        raftpb.HardState{Term: 2, Commit: 3, Vote: 1},
			Entries:          nil,
			CommittedEntries: []raftpb.Entry{{Term: 2, Index: 3, Data: []byte("foo")}},
			MustSync:         false,
		},
	}
	storage := NewMemoryStorage()
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	StartNode(c, []Peer{{ID: 1}})
	ctx, cancel, n := newNodeTestHarness(t.Context(), t, c, Peer{ID: 1})
	defer cancel()

	{
		rd := <-n.Ready()
		require.Equal(t, wants[0], rd)
		storage.Append(rd.Entries)
		n.Advance()
	}

	require.NoError(t, n.Campaign(ctx))

	{
		// Persist vote.
		rd := <-n.Ready()
		storage.Append(rd.Entries)
		n.Advance()
		// Append empty entry.
		rd = <-n.Ready()
		storage.Append(rd.Entries)
		n.Advance()
	}

	n.Propose(ctx, []byte("foo"))
	{
		rd := <-n.Ready()
		assert.Equal(t, wants[1], rd)
		storage.Append(rd.Entries)
		n.Advance()
	}

	{
		rd := <-n.Ready()
		assert.Equal(t, wants[2], rd)
		storage.Append(rd.Entries)
		n.Advance()
	}

	select {
	case rd := <-n.Ready():
		t.Errorf("unexpected Ready: %+v", rd)
	case <-time.After(time.Millisecond):
	}
}

func TestNodeRestart(t *testing.T) {
	entries := []raftpb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := raftpb.HardState{Term: 1, Commit: 1}

	want := Ready{
		// No HardState is emitted because there was no change.
		HardState: raftpb.HardState{},
		// commit up to index commit index in st
		CommittedEntries: entries[:st.Commit],
		// MustSync is false because no HardState or new entries are provided.
		MustSync: false,
	}

	storage := NewMemoryStorage()
	storage.SetHardState(st)
	storage.Append(entries)
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	n := RestartNode(c)
	defer n.Stop()
	assert.Equal(t, want, <-n.Ready())
	n.Advance()

	select {
	case rd := <-n.Ready():
		t.Errorf("unexpected Ready: %+v", rd)
	case <-time.After(time.Millisecond):
	}
}

func TestNodeRestartFromSnapshot(t *testing.T) {
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
			Index:     2,
			Term:      1,
		},
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := raftpb.HardState{Term: 1, Commit: 3}

	want := Ready{
		// No HardState is emitted because nothing changed relative to what is
		// already persisted.
		HardState: raftpb.HardState{},
		// commit up to index commit index in st
		CommittedEntries: entries,
		// MustSync is only true when there is a new HardState or new entries;
		// neither is the case here.
		MustSync: false,
	}

	s := NewMemoryStorage()
	s.SetHardState(st)
	s.ApplySnapshot(snap)
	s.Append(entries)
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	n := RestartNode(c)
	defer n.Stop()
	if assert.Equal(t, want, <-n.Ready()) {
		n.Advance()
	}

	select {
	case rd := <-n.Ready():
		t.Errorf("unexpected Ready: %+v", rd)
	case <-time.After(time.Millisecond):
	}
}

func TestNodeAdvance(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1))
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	ctx, cancel, n := newNodeTestHarness(t.Context(), t, c)
	defer cancel()

	n.Campaign(ctx)
	// Persist vote.
	rd := readyWithTimeout(n)
	storage.Append(rd.Entries)
	n.Advance()
	// Append empty entry.
	rd = readyWithTimeout(n)
	storage.Append(rd.Entries)
	n.Advance()

	n.Propose(ctx, []byte("foo"))
	rd = readyWithTimeout(n)
	storage.Append(rd.Entries)
	n.Advance()
	select {
	case <-n.Ready():
	case <-time.After(100 * time.Millisecond):
		t.Errorf("expect Ready after Advance, but there is no Ready available")
	}
}

func TestSoftStateEqual(t *testing.T) {
	tests := []struct {
		st *SoftState
		we bool
	}{
		{&SoftState{}, true},
		{&SoftState{Lead: 1}, false},
		{&SoftState{RaftState: StateLeader}, false},
	}
	for i, tt := range tests {
		assert.Equal(t, tt.we, tt.st.equal(&SoftState{}), "#%d", i)
	}
}

func TestIsHardStateEqual(t *testing.T) {
	tests := []struct {
		ht raftpb.HardState
		we bool
	}{
		{emptyState, true},
		{raftpb.HardState{Vote: 1}, false},
		{raftpb.HardState{Commit: 1}, false},
		{raftpb.HardState{Term: 1}, false},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.we, isHardStateEqual(tt.ht, emptyState), "#%d", i)
	}
}

func TestNodeProposeAddLearnerNode(t *testing.T) {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	go n.run()
	n.Campaign(t.Context())
	stop := make(chan struct{})
	done := make(chan struct{})
	applyConfChan := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n.Tick()
			case rd := <-n.Ready():
				s.Append(rd.Entries)
				t.Logf("raft: %v", rd.Entries)
				for _, ent := range rd.Entries {
					if ent.Type != raftpb.EntryConfChange {
						continue
					}
					var cc raftpb.ConfChange
					cc.Unmarshal(ent.Data)
					state := n.ApplyConfChange(cc)
					assert.True(t, len(state.Learners) > 0 && state.Learners[0] == cc.NodeID && cc.NodeID == 2,
						"apply conf change should return new added learner: %v", state.String())
					assert.Len(t, state.Voters, 1,
						"add learner should not change the nodes: %v", state.String())

					t.Logf("apply raft conf %v changed to: %v", cc, state.String())
					applyConfChan <- struct{}{}
				}
				n.Advance()
			}
		}
	}()
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}
	n.ProposeConfChange(t.Context(), cc)
	<-applyConfChan
	close(stop)
	<-done
	n.Stop()
}

func TestAppendPagination(t *testing.T) {
	const maxSizePerMsg = 2048
	n := newNetworkWithConfig(func(c *Config) {
		c.MaxSizePerMsg = maxSizePerMsg
	}, nil, nil, nil)

	seenFullMessage := false
	// Inspect all messages to see that we never exceed the limit, but
	// we do see messages of larger than half the limit.
	n.msgHook = func(m raftpb.Message) bool {
		if m.Type == raftpb.MsgApp {
			size := 0
			for _, e := range m.Entries {
				size += len(e.Data)
			}
			assert.LessOrEqual(t, size, maxSizePerMsg, "sent MsgApp that is too large")
			if size > maxSizePerMsg/2 {
				seenFullMessage = true
			}
		}
		return true
	}

	n.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgHup})

	// Partition the network while we make our proposals. This forces
	// the entries to be batched into larger messages.
	n.isolate(1)
	blob := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 5; i++ {
		n.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgProp, Entries: []raftpb.Entry{{Data: blob}}})
	}
	n.recover()

	// After the partition recovers, tick the clock to wake everything
	// back up and send the messages.
	n.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgBeat})
	assert.True(t, seenFullMessage, "didn't see any messages more than half the max size; something is wrong with this test")
}

func TestCommitPagination(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	cfg.MaxCommittedSizePerReady = 2048
	ctx, cancel, n := newNodeTestHarness(t.Context(), t, cfg)
	defer cancel()
	n.Campaign(ctx)

	// Persist vote.
	rd := readyWithTimeout(n)
	s.Append(rd.Entries)
	n.Advance()
	// Append empty entry.
	rd = readyWithTimeout(n)
	s.Append(rd.Entries)
	n.Advance()
	// Apply empty entry.
	rd = readyWithTimeout(n)
	require.Len(t, rd.CommittedEntries, 1)

	s.Append(rd.Entries)
	n.Advance()

	blob := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 3; i++ {
		require.NoError(t, n.Propose(ctx, blob), "#%d", i)
	}

	// First the three proposals have to be appended.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Entries, 3)

	s.Append(rd.Entries)
	n.Advance()

	// The 3 proposals will commit in two batches.
	rd = readyWithTimeout(n)
	require.Len(t, rd.CommittedEntries, 2)

	s.Append(rd.Entries)
	n.Advance()
	rd = readyWithTimeout(n)
	require.Len(t, rd.CommittedEntries, 1)

	s.Append(rd.Entries)
	n.Advance()
}

func TestCommitPaginationWithAsyncStorageWrites(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	cfg.MaxCommittedSizePerReady = 2048
	cfg.AsyncStorageWrites = true
	ctx, cancel, n := newNodeTestHarness(t.Context(), t, cfg)
	defer cancel()
	n.Campaign(ctx)

	// Persist vote.
	rd := readyWithTimeout(n)
	require.Len(t, rd.Messages, 1)
	m := rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageAppend, m.Type)
	require.NoError(t, s.Append(m.Entries))
	for _, resp := range m.Responses {
		require.NoError(t, n.Step(ctx, resp))
	}
	// Append empty entry.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Messages, 1)
	m = rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageAppend, m.Type)
	require.NoError(t, s.Append(m.Entries))
	for _, resp := range m.Responses {
		require.NoError(t, n.Step(ctx, resp))
	}
	// Apply empty entry.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Messages, 2)
	for _, m := range rd.Messages {
		switch m.Type {
		case raftpb.MsgStorageAppend:
			require.NoError(t, s.Append(m.Entries))
			for _, resp := range m.Responses {
				require.NoError(t, n.Step(ctx, resp))
			}
		case raftpb.MsgStorageApply:
			require.Len(t, m.Entries, 1)
			require.Len(t, m.Responses, 1)
			require.NoError(t, n.Step(ctx, m.Responses[0]))
		default:
			t.Fatalf("unexpected: %v", m)
		}
	}

	// Propose first entry.
	blob := []byte(strings.Repeat("a", 1024))
	require.NoError(t, n.Propose(ctx, blob))

	// Append first entry.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Messages, 1)
	m = rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageAppend, m.Type)
	require.Len(t, m.Entries, 1)
	require.NoError(t, s.Append(m.Entries))
	for _, resp := range m.Responses {
		require.NoError(t, n.Step(ctx, resp))
	}

	// Propose second entry.
	require.NoError(t, n.Propose(ctx, blob))

	// Append second entry. Don't apply first entry yet.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Messages, 2)
	var applyResps []raftpb.Message
	for _, m := range rd.Messages {
		switch m.Type {
		case raftpb.MsgStorageAppend:
			require.NoError(t, s.Append(m.Entries))
			for _, resp := range m.Responses {
				require.NoError(t, n.Step(ctx, resp))
			}
		case raftpb.MsgStorageApply:
			require.Len(t, m.Entries, 1)
			require.Len(t, m.Responses, 1)
			applyResps = append(applyResps, m.Responses[0])
		default:
			t.Fatalf("unexpected: %v", m)
		}
	}

	// Propose third entry.
	require.NoError(t, n.Propose(ctx, blob))

	// Append third entry. Don't apply second entry yet.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Messages, 2)
	for _, m := range rd.Messages {
		switch m.Type {
		case raftpb.MsgStorageAppend:
			require.NoError(t, s.Append(m.Entries))
			for _, resp := range m.Responses {
				require.NoError(t, n.Step(ctx, resp))
			}
		case raftpb.MsgStorageApply:
			require.Len(t, m.Entries, 1)
			require.Len(t, m.Responses, 1)
			applyResps = append(applyResps, m.Responses[0])
		default:
			t.Fatalf("unexpected: %v", m)
		}
	}

	// Third entry should not be returned to be applied until first entry's
	// application is acknowledged.
	drain := true
	for drain {
		select {
		case rd := <-n.Ready():
			for _, m := range rd.Messages {
				require.NotEqual(t, raftpb.MsgStorageApply, m.Type, "unexpected message: %v", m)
			}
		case <-time.After(10 * time.Millisecond):
			drain = false
		}
	}

	// Acknowledged first entry application.
	require.NoError(t, n.Step(ctx, applyResps[0]))
	applyResps = applyResps[1:]

	// Third entry now returned for application.
	rd = readyWithTimeout(n)
	require.Len(t, rd.Messages, 1)
	m = rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageApply, m.Type)
	require.Len(t, m.Entries, 1)
	applyResps = append(applyResps, m.Responses[0])

	// Acknowledged second and third entry application.
	for _, resp := range applyResps {
		require.NoError(t, n.Step(ctx, resp))
	}
	applyResps = nil
}

type ignoreSizeHintMemStorage struct {
	*MemoryStorage
}

func (s *ignoreSizeHintMemStorage) Entries(lo, hi uint64, _ uint64) ([]raftpb.Entry, error) {
	return s.MemoryStorage.Entries(lo, hi, math.MaxUint64)
}

// TestNodeCommitPaginationAfterRestart regression tests a scenario in which the
// Storage's Entries size limitation is slightly more permissive than Raft's
// internal one. The original bug was the following:
//
//   - node learns that index 11 (or 100, doesn't matter) is committed
//   - nextCommittedEnts returns index 1..10 in CommittedEntries due to size limiting.
//     However, index 10 already exceeds maxBytes, due to a user-provided impl of Entries.
//   - Commit index gets bumped to 10
//   - the node persists the HardState, but crashes before applying the entries
//   - upon restart, the storage returns the same entries, but `slice` takes a different code path
//     (since it is now called with an upper bound of 10) and removes the last entry.
//   - Raft emits a HardState with a regressing commit index.
//
// A simpler version of this test would have the storage return a lot less entries than dictated
// by maxSize (for example, exactly one entry) after the restart, resulting in a larger regression.
// This wouldn't need to exploit anything about Raft-internal code paths to fail.
func TestNodeCommitPaginationAfterRestart(t *testing.T) {
	s := &ignoreSizeHintMemStorage{
		MemoryStorage: newTestMemoryStorage(withPeers(1)),
	}
	persistedHardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 10,
	}

	s.hardState = persistedHardState
	s.ents = make([]raftpb.Entry, 10)
	var size uint64
	for i := range s.ents {
		ent := raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
			Data:  []byte("a"),
		}

		s.ents[i] = ent
		size += uint64(ent.Size())
	}

	cfg := newTestConfig(1, 10, 1, s)
	// Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
	// not be included in the initial rd.CommittedEntries. However, our storage will ignore
	// this and *will* return it (which is how the Commit index ended up being 10 initially).
	cfg.MaxSizePerMsg = size - uint64(s.ents[len(s.ents)-1].Size()) - 1

	rn, err := NewRawNode(cfg)
	require.NoError(t, err)

	n := newNode(rn)
	go n.run()
	defer n.Stop()

	rd := readyWithTimeout(&n)
	assert.False(t, !IsEmptyHardState(rd.HardState) && rd.HardState.Commit < persistedHardState.Commit,
		"HardState regressed: Commit %d -> %d\nCommitting:\n%+v",
		persistedHardState.Commit, rd.HardState.Commit,
		DescribeEntries(rd.CommittedEntries, func(data []byte) string { return fmt.Sprintf("%q", data) }))
}
