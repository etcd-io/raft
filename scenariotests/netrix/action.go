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

// Action determines which messages to deliver as a consequence of an event.
// It returns the set of messages that should be forwarded to their recipients.
// Returning an empty slice means the message is dropped.
type Action func(*Event, *Context) []*pb.Message

// DeliverMessage returns an Action that delivers the message associated with
// the current event to its intended recipient.
func DeliverMessage() Action {
	return func(e *Event, _ *Context) []*pb.Message {
		if e.Msg == nil {
			return nil
		}
		return []*pb.Message{e.Msg}
	}
}

// DropMessage returns an Action that drops the message (delivers nothing).
func DropMessage() Action {
	return func(*Event, *Context) []*pb.Message {
		return nil
	}
}

// RecordMessageAs returns an Action that stores the current event's message
// in Context.VarSet under the given label (appending to any existing set),
// and delivers the message normally.
func RecordMessageAs(label string) Action {
	return func(e *Event, ctx *Context) []*pb.Message {
		if e.Msg == nil {
			return nil
		}
		msgs := ctx.getMsgs(label)
		msgs = append(msgs, e.Msg)
		ctx.setMsgs(label, msgs)
		return []*pb.Message{e.Msg}
	}
}

// composeActions chains multiple Actions: the returned messages are the union
// of all non-nil results from each action. If any action drops the message
// (returns nil/empty), only the delivering actions contribute.
// Used internally by If().Then() to apply a list of actions in order.
func composeActions(actions []Action) Action {
	return func(e *Event, ctx *Context) []*pb.Message {
		var out []*pb.Message
		for _, a := range actions {
			out = append(out, a(e, ctx)...)
		}
		// Deduplicate by pointer identity so that multiple DeliverMessage()
		// calls don't double-deliver.
		return deduplicateMsgs(out)
	}
}

func deduplicateMsgs(msgs []*pb.Message) []*pb.Message {
	seen := make(map[*pb.Message]struct{}, len(msgs))
	out := msgs[:0]
	for _, m := range msgs {
		if _, ok := seen[m]; !ok {
			seen[m] = struct{}{}
			out = append(out, m)
		}
	}
	return out
}
