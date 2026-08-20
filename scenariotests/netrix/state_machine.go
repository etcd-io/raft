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

// Sentinel state labels used as targets in StateMachine transitions.
const (
	SuccessState = "__success__"
	FailureState = "__failure__"
)

// smState is a node in the state machine graph.
type smState struct {
	label       string
	transitions []smTransition
	isSuccess   bool
}

type smTransition struct {
	cond  Condition
	label string
}

// StateMachine is a deterministic finite automaton over the event stream.
// Transitions are labelled with Conditions; the first matching transition
// fires. The machine starts in its initial state and the run succeeds if
// it reaches a success state before the test ends or a failure transition fires.
type StateMachine struct {
	states  map[string]*smState
	initial string
	current string
}

// NewStateMachine creates a StateMachine with an unnamed initial state.
func NewStateMachine() *StateMachine {
	initial := &smState{label: "__initial__"}
	sm := &StateMachine{
		states:  map[string]*smState{"__initial__": initial},
		initial: "__initial__",
		current: "__initial__",
	}
	// Pre-register terminal states.
	sm.states[SuccessState] = &smState{label: SuccessState, isSuccess: true}
	sm.states[FailureState] = &smState{label: FailureState}
	return sm
}

// Builder returns a StateBuilder anchored at the initial state.
func (sm *StateMachine) Builder() *StateBuilder {
	return &StateBuilder{sm: sm, label: sm.initial}
}

// Step advances the state machine by one event. It evaluates transitions from
// the current state in order and fires the first one whose condition is true.
func (sm *StateMachine) Step(e *Event, ctx *Context) {
	if sm.IsTerminal() {
		return
	}
	cur := sm.states[sm.current]
	for _, t := range cur.transitions {
		if t.cond(e, ctx) {
			sm.current = t.label
			return
		}
	}
}

// IsSuccess reports whether the state machine is in a success state.
func (sm *StateMachine) IsSuccess() bool {
	st, ok := sm.states[sm.current]
	return ok && st.isSuccess
}

// IsFailure reports whether the state machine has reached the failure state.
func (sm *StateMachine) IsFailure() bool {
	return sm.current == FailureState
}

// IsTerminal reports whether the state machine has reached a terminal state
// (success or failure).
func (sm *StateMachine) IsTerminal() bool {
	return sm.IsSuccess() || sm.IsFailure()
}

// CurrentState returns the label of the current state.
func (sm *StateMachine) CurrentState() string {
	return sm.current
}

// Reset returns the state machine to its initial state. Useful for running the
// same TestCase multiple times.
func (sm *StateMachine) Reset() {
	sm.current = sm.initial
}

// getOrCreate returns the smState for label, creating it if necessary.
func (sm *StateMachine) getOrCreate(label string) *smState {
	if st, ok := sm.states[label]; ok {
		return st
	}
	st := &smState{label: label}
	sm.states[label] = st
	return st
}

// StateBuilder provides a fluent API for adding transitions from a given state.
type StateBuilder struct {
	sm    *StateMachine
	label string
}

// On adds a transition from the current state to the named target state,
// firing when cond is true. Returns a StateBuilder rooted at the target so
// chains can be constructed:
//
//	init.On(cond1, "a").On(cond2, "b").On(cond3, SuccessState)
func (sb *StateBuilder) On(cond Condition, target string) *StateBuilder {
	src := sb.sm.getOrCreate(sb.label)
	sb.sm.getOrCreate(target) // ensure target exists
	src.transitions = append(src.transitions, smTransition{cond: cond, label: target})
	return &StateBuilder{sm: sb.sm, label: target}
}

// MarkSuccess marks the state this builder is anchored at as a success state.
func (sb *StateBuilder) MarkSuccess() *StateBuilder {
	st := sb.sm.getOrCreate(sb.label)
	st.isSuccess = true
	return sb
}
