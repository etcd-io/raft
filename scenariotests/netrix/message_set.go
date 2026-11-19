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

package netrix

import pb "go.etcd.io/raft/v3/raftpb"

// MessageSet provides conditions and actions for a named collection of
// raft messages stored in the Context's VarSet.
type MessageSet struct {
	label string
}

// Set returns a MessageSet identified by the given label.
func Set(label string) MessageSet {
	return MessageSet{label: label}
}

// Store returns an Action that appends the current event's message to this set.
// The message is stored but not delivered (drop semantics). Use together with
// DeliverAll to replay stored messages later.
func (s MessageSet) Store() Action {
	return func(e *Event, ctx *Context) []*pb.Message {
		if e.Msg == nil {
			return nil
		}
		msgs := ctx.getMsgs(s.label)
		msgs = append(msgs, e.Msg)
		ctx.setMsgs(s.label, msgs)
		// Storing suppresses delivery of the original message.
		return nil
	}
}

// DeliverAll returns an Action that delivers all messages currently stored in
// this set and clears the set. Any message from the current event is not
// included (use DeliverMessage() separately if needed).
func (s MessageSet) DeliverAll() Action {
	return func(_ *Event, ctx *Context) []*pb.Message {
		msgs := ctx.getMsgs(s.label)
		ctx.setMsgs(s.label, nil)
		return msgs
	}
}

// Contains returns a Condition that is true when the current event's message
// is already stored in this set (compared by pointer identity).
func (s MessageSet) Contains() Condition {
	return func(e *Event, ctx *Context) bool {
		if e.Msg == nil {
			return false
		}
		for _, m := range ctx.getMsgs(s.label) {
			if m == e.Msg {
				return true
			}
		}
		return false
	}
}

// Count returns a Counter that tracks the number of messages in this set.
// The counter label is derived from the set label so it stays in sync.
// Note: the counter is updated by Store/DeliverAll automatically through the
// VarSet length; use Len() condition for length-based guards.
func (s MessageSet) Count() Counter {
	return Count(s.label + ".__count__")
}

// Len returns a Condition that is true when the number of messages in the set
// satisfies the given predicate.
func (s MessageSet) Len(pred func(int) bool) Condition {
	return func(_ *Event, ctx *Context) bool {
		return pred(len(ctx.getMsgs(s.label)))
	}
}
