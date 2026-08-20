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

// Condition is a predicate evaluated against the current Event and Context.
// It returns true when the condition is satisfied.
type Condition func(*Event, *Context) bool

// And returns a Condition that is true when both c and other are true.
func (c Condition) And(other Condition) Condition {
	return func(e *Event, ctx *Context) bool {
		return c(e, ctx) && other(e, ctx)
	}
}

// Or returns a Condition that is true when either c or other is true.
func (c Condition) Or(other Condition) Condition {
	return func(e *Event, ctx *Context) bool {
		return c(e, ctx) || other(e, ctx)
	}
}

// Not returns a Condition that negates c.
func (c Condition) Not() Condition {
	return func(e *Event, ctx *Context) bool {
		return !c(e, ctx)
	}
}

// IsMessageSend returns true when the event represents a message being sent.
func IsMessageSend() Condition {
	return func(e *Event, _ *Context) bool {
		return e.Type == MessageSendEventType
	}
}

// IsMessageReceive returns true when the event represents a message being
// delivered to a node.
func IsMessageReceive() Condition {
	return func(e *Event, _ *Context) bool {
		return e.Type == MessageReceiveEventType
	}
}

// IsMessageType returns true when the event carries a raft message of the
// given type.
func IsMessageType(t pb.MessageType) Condition {
	return func(e *Event, _ *Context) bool {
		return e.Msg != nil && e.Msg.GetType() == t
	}
}

// IsMessageFrom returns true when the event's message originates from the
// node with the given ID.
func IsMessageFrom(id uint64) Condition {
	return func(e *Event, _ *Context) bool {
		return e.From == id
	}
}

// IsMessageTo returns true when the event's message is addressed to the node
// with the given ID.
func IsMessageTo(id uint64) Condition {
	return func(e *Event, _ *Context) bool {
		return e.To == id
	}
}

// When returns a Condition that evaluates the provided function on every event.
// Use it to close over external state that changes between rounds:
//
//	netrix.When(func() bool { return snapshotSetup })
func When(f func() bool) Condition {
	return func(*Event, *Context) bool { return f() }
}

// Always returns a Condition that is always true.
func Always() Condition {
	return func(*Event, *Context) bool { return true }
}

// Never returns a Condition that is always false.
func Never() Condition {
	return func(*Event, *Context) bool { return false }
}
