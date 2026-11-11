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

// FilterFunc handles a single event and returns the messages to deliver plus a
// bool indicating whether this function handled the event (true) or deferred to
// the next filter (false). This mirrors the Netrix FilterFunc signature.
type FilterFunc func(*Event, *Context) ([]*pb.Message, bool)

// FilterSet is an ordered list of FilterFuncs. For each event the filters are
// tried in order until one returns handled=true. If no filter matches, the
// default filter is used (deliver the message).
type FilterSet struct {
	filters        []FilterFunc
	defaultFilters []FilterFunc
}

// NewFilterSet creates a FilterSet with the default filter (deliver the message).
func NewFilterSet() *FilterSet {
	return &FilterSet{
		defaultFilters: []FilterFunc{defaultDeliverFilter},
	}
}

// NewFilterSetWithDefault creates a FilterSet using the provided default filters
// instead of the built-in deliver-all default.
func NewFilterSetWithDefault(defaults ...FilterFunc) *FilterSet {
	return &FilterSet{defaultFilters: defaults}
}

// AddFilter appends a FilterFunc to this set. Filters are evaluated in the
// order they are added.
func (fs *FilterSet) AddFilter(f FilterFunc) {
	fs.filters = append(fs.filters, f)
}

// Apply evaluates all filters against the event and returns the messages to
// deliver.
func (fs *FilterSet) Apply(e *Event, ctx *Context) []*pb.Message {
	for _, f := range fs.filters {
		msgs, handled := f(e, ctx)
		if handled {
			return msgs
		}
	}
	for _, f := range fs.defaultFilters {
		msgs, handled := f(e, ctx)
		if handled {
			return msgs
		}
	}
	return nil
}

// defaultDeliverFilter is the built-in fallback: deliver the message.
func defaultDeliverFilter(e *Event, _ *Context) ([]*pb.Message, bool) {
	if e.Type == MessageSendEventType && e.Msg != nil {
		return []*pb.Message{e.Msg}, true
	}
	return nil, false
}

// IfBuilder is the intermediate type returned by If(). Call Then() to produce
// a FilterFunc.
type IfBuilder struct {
	cond Condition
}

// If begins an If().Then() filter expression.
func If(cond Condition) *IfBuilder {
	return &IfBuilder{cond: cond}
}

// Then returns a FilterFunc that, when the condition is true, applies all
// provided actions in sequence and returns their combined message output.
// The filter is considered handled (true) when the condition matches,
// regardless of whether messages are delivered.
func (b *IfBuilder) Then(actions ...Action) FilterFunc {
	combined := composeActions(actions)
	return func(e *Event, ctx *Context) ([]*pb.Message, bool) {
		if !b.cond(e, ctx) {
			return nil, false
		}
		return combined(e, ctx), true
	}
}
