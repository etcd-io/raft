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

// Counter provides conditions and an action for a named integer counter stored
// in the Context's VarSet. The counter starts at 0 and is incremented by Incr.
type Counter struct {
	label string
}

// Count returns a Counter identified by the given label.
func Count(label string) Counter {
	return Counter{label: label}
}

// Incr returns an Action that increments the counter by 1. The current
// event's message (if any) is delivered unchanged.
func (c Counter) Incr() Action {
	return func(e *Event, ctx *Context) []*pb.Message {
		ctx.setInt(c.label, ctx.getInt(c.label)+1)
		if e.Msg != nil {
			return []*pb.Message{e.Msg}
		}
		return nil
	}
}

// Lt returns a Condition that is true when the counter value is less than n.
func (c Counter) Lt(n int) Condition {
	return func(_ *Event, ctx *Context) bool {
		return ctx.getInt(c.label) < n
	}
}

// Gt returns a Condition that is true when the counter value is greater than n.
func (c Counter) Gt(n int) Condition {
	return func(_ *Event, ctx *Context) bool {
		return ctx.getInt(c.label) > n
	}
}

// Leq returns a Condition that is true when the counter value is ≤ n.
func (c Counter) Leq(n int) Condition {
	return func(_ *Event, ctx *Context) bool {
		return ctx.getInt(c.label) <= n
	}
}

// Geq returns a Condition that is true when the counter value is ≥ n.
func (c Counter) Geq(n int) Condition {
	return func(_ *Event, ctx *Context) bool {
		return ctx.getInt(c.label) >= n
	}
}

// LtF returns a Condition that is true when the counter is less than the value
// returned by valF for the current event.
func (c Counter) LtF(valF func(*Event, *Context) int) Condition {
	return func(e *Event, ctx *Context) bool {
		return ctx.getInt(c.label) < valF(e, ctx)
	}
}

// GtF returns a Condition that is true when the counter is greater than the
// value returned by valF.
func (c Counter) GtF(valF func(*Event, *Context) int) Condition {
	return func(e *Event, ctx *Context) bool {
		return ctx.getInt(c.label) > valF(e, ctx)
	}
}
