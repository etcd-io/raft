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

// Partition models a network partition between two groups of nodes.
//
// Activate and heal the partition via the Actions returned by Isolate and Heal,
// composing them inside If().Then() filter rules or alongside other actions:
//
//	filters.AddFilter(netrix.If(triggerCond).Then(part.Isolate(), netrix.DropMessage()))
//	filters.AddFilter(netrix.If(healCond).Then(part.Heal()))
//
// Register the partition's drop rule once with Filter:
//
//	filters.AddFilter(part.Filter())
//
// Use IsActive / IsHealed as Conditions in state-machine transitions.
//
// The zero value is not useful; construct via IsolateNode, IsolateNodes, or PartitionSides.
type Partition struct {
	active bool
	// crosses reports whether an event crosses the partition boundary.
	crosses func(e *Event) bool
}

// IsolateNode returns a Partition that drops all messages to or from nodeID.
func IsolateNode(nodeID uint64) *Partition {
	return &Partition{
		crosses: func(e *Event) bool { return e.From == nodeID || e.To == nodeID },
	}
}

// IsolateNodes returns a Partition that drops all messages to or from any node
// in nodeIDs, cutting that group off from the rest of the cluster.
func IsolateNodes(nodeIDs ...uint64) *Partition {
	set := make(map[uint64]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		set[id] = true
	}
	return &Partition{
		crosses: func(e *Event) bool { return set[e.From] || set[e.To] },
	}
}

// PartitionSides returns a Partition that drops all messages crossing between
// sideA and sideB. Messages within each side are unaffected.
func PartitionSides(sideA, sideB []uint64) *Partition {
	a := make(map[uint64]bool, len(sideA))
	for _, id := range sideA {
		a[id] = true
	}
	b := make(map[uint64]bool, len(sideB))
	for _, id := range sideB {
		b[id] = true
	}
	return &Partition{
		crosses: func(e *Event) bool {
			return (a[e.From] && b[e.To]) || (b[e.From] && a[e.To])
		},
	}
}

// Isolate activates the partition immediately. Use from SetupFunc or TickFunc.
func (p *Partition) Isolate() { p.active = true }

// Heal deactivates the partition immediately. Use from SetupFunc or TickFunc.
func (p *Partition) Heal() { p.active = false }

// IsolateAction returns an Action that activates the partition as a side-effect
// and passes the current message through unchanged. Compose inside If().Then():
//
//	filters.AddFilter(netrix.If(triggerCond).Then(part.IsolateAction()))
func (p *Partition) IsolateAction() Action {
	return func(e *Event, _ *Context) []*pb.Message {
		p.active = true
		if e.Msg != nil {
			return []*pb.Message{e.Msg}
		}
		return nil
	}
}

// HealAction returns an Action that deactivates the partition as a side-effect
// and passes the current message through unchanged. Compose inside If().Then():
//
//	filters.AddFilter(netrix.If(healCond).Then(part.HealAction()))
func (p *Partition) HealAction() Action {
	return func(e *Event, _ *Context) []*pb.Message {
		p.active = false
		if e.Msg != nil {
			return []*pb.Message{e.Msg}
		}
		return nil
	}
}

// Active reports whether the partition is currently in effect.
// Use this when you need a plain bool — e.g. inside a Condition closure that
// also captures other state. For standalone filter conditions prefer IsActive.
func (p *Partition) Active() bool { return p.active }

// IsActive returns a Condition that is true on every event while the partition
// is in effect.
func (p *Partition) IsActive() Condition {
	return func(_ *Event, _ *Context) bool { return p.active }
}

// IsHealed returns a Condition that is true on every event after the partition
// has been healed.
func (p *Partition) IsHealed() Condition {
	return func(_ *Event, _ *Context) bool { return !p.active }
}

// Filter returns a FilterFunc that drops any message crossing the partition
// boundary while the partition is active. Register it once with FilterSet.AddFilter.
func (p *Partition) Filter() FilterFunc {
	return func(e *Event, _ *Context) ([]*pb.Message, bool) {
		if p.active && p.crosses(e) {
			return nil, true // drop and mark handled
		}
		return nil, false
	}
}
