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

package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRawNodeBootstrapStorage(t *testing.T) {
	for _, tt := range []struct {
		name string
		hs   *pb.HardState
	}{
		{"nil", nil},
		{"empty", &pb.HardState{}},
		{"zero", &pb.HardState{Term: new(uint64(0)), Vote: new(uint64(0)), Commit: new(uint64(0))}},
		{"term", &pb.HardState{Term: new(uint64(5))}},
		{"vote", &pb.HardState{Vote: new(uint64(2))}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage()
			// A configuration-only snapshot at index and term zero is allowed.
			require.NoError(t, s.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
				ConfState: &pb.ConfState{Voters: []uint64{1}},
			}}))
			require.NoError(t, s.SetHardState(tt.hs))
			rn, err := NewRawNode(newTestConfig(1, 10, 1, s))
			require.NoError(t, err)
			err = rn.Bootstrap([]Peer{{ID: 1}})
			if IsEmptyHardState(tt.hs) {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.False(t, rn.HasReady())
			}
		})
	}
}

func TestRawNodeBootstrapPreservesVote(t *testing.T) {
	for _, bootstrap := range []bool{false, true} {
		t.Run(fmt.Sprint(bootstrap), func(t *testing.T) {
			s := NewMemoryStorage()
			rn, err := NewRawNode(newTestConfig(1, 10, 1, s))
			require.NoError(t, err)
			persist := func() []*pb.Message {
				var msgs []*pb.Message
				for rn.HasReady() {
					rd := rn.Ready()
					require.NoError(t, s.Append(rd.Entries))
					if !IsEmptyHardState(rd.HardState) {
						require.NoError(t, s.SetHardState(rd.HardState))
					}
					msgs = append(msgs, rd.Messages...)
					for _, ent := range rd.CommittedEntries {
						if ent.GetType() == pb.EntryConfChange {
							cc := new(pb.ConfChange)
							require.NoError(t, proto.Unmarshal(ent.Data, cc))
							rn.ApplyConfChange(cc)
						}
					}
					rn.Advance(rd)
				}
				return msgs
			}

			// Grant candidate 2 a vote and persist it before restarting.
			req := &pb.Message{Type: pb.MsgVote.Enum(), From: new(uint64(2)), To: new(uint64(1)),
				Term: new(uint64(5)), LogTerm: new(uint64(5)), Index: new(uint64(3))}
			require.NoError(t, rn.Step(req))
			msgs := persist()
			require.Len(t, msgs, 1)
			require.Equal(t, pb.MsgVoteResp, msgs[0].GetType())
			require.False(t, msgs[0].GetReject())
			hs, _, err := s.InitialState()
			require.NoError(t, err)
			require.Equal(t, uint64(5), hs.GetTerm())
			require.Equal(t, uint64(2), hs.GetVote())
			rn, err = NewRawNode(newTestConfig(1, 10, 1, s))
			require.NoError(t, err)

			if bootstrap {
				if err := rn.Bootstrap([]Peer{{ID: 1}, {ID: 2}, {ID: 3}}); err == nil {
					t.Error("Bootstrap succeeded on a Storage with a persisted HardState")
				}
				if st := rn.BasicStatus(); st.GetTerm() != 5 || st.GetVote() != 2 {
					t.Errorf("Bootstrap changed election state: %s", st.HardState)
				}
				if rn.HasReady() {
					t.Error("Bootstrap produced work on a Storage with a persisted HardState")
				}
				persist()
			}

			// An up-to-date candidate must not receive a second vote in term 5.
			req.From = new(uint64(3))
			require.NoError(t, rn.Step(req))
			msgs = persist()
			require.Len(t, msgs, 1)
			require.Equal(t, pb.MsgVoteResp, msgs[0].GetType())
			if !msgs[0].GetReject() {
				t.Error("granted another candidate a vote in term 5")
			}
			hs, _, err = s.InitialState()
			require.NoError(t, err)
			if hs.GetTerm() != 5 || hs.GetVote() != 2 {
				t.Errorf("persisted election state changed: %s", hs)
			}
		})
	}
}
