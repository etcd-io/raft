// Copyright 2022 The etcd Authors
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
	"runtime"
	"strings"
	"testing"

	fuzz "github.com/AdaLogics/go-fuzz-headers"

	pb "go.etcd.io/raft/v3/raftpb"
)

func getMsgType(i int) pb.MessageType {
	allTypes := map[int]pb.MessageType{0: pb.MsgHup,
		1:  pb.MsgBeat,
		2:  pb.MsgProp,
		3:  pb.MsgApp,
		4:  pb.MsgAppResp,
		5:  pb.MsgVote,
		6:  pb.MsgVoteResp,
		7:  pb.MsgSnap,
		8:  pb.MsgHeartbeat,
		9:  pb.MsgHeartbeatResp,
		10: pb.MsgUnreachable,
		11: pb.MsgSnapStatus,
		12: pb.MsgCheckQuorum,
		13: pb.MsgTransferLeader,
		14: pb.MsgTimeoutNow,
		15: pb.MsgReadIndex,
		16: pb.MsgReadIndexResp,
		17: pb.MsgPreVote,
		18: pb.MsgPreVoteResp}
	return allTypes[i%len(allTypes)]
}

// All cases in shouldReport represent known errors in etcd
// as these are reported via manually added panics.
func shouldReport(err string) bool {
	if strings.Contains(err, "stepped empty MsgProp") {
		return false
	}
	if strings.Contains(err, "Was the raft log corrupted, truncated, or lost?") {
		return false
	}
	if strings.Contains(err, "ConfStates not equivalent after sorting:") {
		return false
	}
	if strings.Contains(err, "term should be set when sending ") {
		return false
	}
	if (strings.Contains(err, "unable to restore config")) && (strings.Contains(err, "removed all voters")) {
		return false
	}
	if strings.Contains(err, "ENCOUNTERED A PANIC OR FATAL") {
		return false
	}
	if strings.Contains(err, "need non-empty snapshot") {
		return false
	}
	if strings.Contains(err, "index, ") && strings.Contains(err, ", is out of range [") {
		return false
	}

	return true
}

func catchPanics() {
	if r := recover(); r != nil {
		var errMsg string
		switch r.(type) {
		case string:
			errMsg = r.(string)
		case runtime.Error:
			errMsg = r.(runtime.Error).Error()
		}
		if shouldReport(errMsg) {
			// Getting to this point means that the fuzzer
			// did not stop because of a manually added panic.
			panic(errMsg)
		}
	}
}

func FuzzStep(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		defer SetLogger(getLogger())
		SetLogger(discardLogger)

		defer catchPanics()
		f := fuzz.NewConsumer(data)
		msg := pb.Message{}
		err := f.GenerateStruct(&msg)
		if err != nil {
			return
		}

		msgTypeIndex, err := f.GetInt()
		if err != nil {
			return
		}
		msg.Type = getMsgType(msgTypeIndex)

		cfg := newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
		cfg.Logger = &ZapRaftLogger{}
		r := newRaft(cfg)
		r.becomeCandidate()
		r.becomeLeader()
		r.prs.Progress[2].BecomeReplicate()
		_ = r.Step(msg)
		_ = r.readMessages()
	})
}

type ZapRaftLogger struct {
}

func (zl *ZapRaftLogger) Debug(_ ...interface{}) {}

func (zl *ZapRaftLogger) Debugf(_ string, _ ...interface{}) {}

func (zl *ZapRaftLogger) Error(_ ...interface{}) {}

func (zl *ZapRaftLogger) Errorf(_ string, _ ...interface{}) {}

func (zl *ZapRaftLogger) Info(_ ...interface{}) {}

func (zl *ZapRaftLogger) Infof(_ string, _ ...interface{}) {}

func (zl *ZapRaftLogger) Warning(_ ...interface{}) {}

func (zl *ZapRaftLogger) Warningf(_ string, _ ...interface{}) {}

func (zl *ZapRaftLogger) Fatal(_ ...interface{}) {
	panic("ENCOUNTERED A PANIC OR FATAL")
}

func (zl *ZapRaftLogger) Fatalf(_ string, _ ...interface{}) {
	panic("ENCOUNTERED A PANIC OR FATAL")
}

func (zl *ZapRaftLogger) Panic(_ ...interface{}) {
	panic("ENCOUNTERED A PANIC OR FATAL")
}

func (zl *ZapRaftLogger) Panicf(_ string, _ ...interface{}) {
	panic("ENCOUNTERED A PANIC OR FATAL")
}
