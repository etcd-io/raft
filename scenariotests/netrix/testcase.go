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

import "go.etcd.io/raft/v3/rafttest"

// TestCase represents a single Netrix unit test scenario. It combines a filter
// set (which controls message delivery) with a state machine (which asserts a
// property over the execution) and optional setup logic.
type TestCase struct {
	// Name identifies the test scenario.
	Name string

	// MaxRounds is the maximum number of stabilize iterations before the run
	// is considered timed out. Zero means no limit (run until the state
	// machine reaches a terminal state or no more work remains).
	MaxRounds int

	// Iterations is the number of times the scenario is retried from a clean
	// environment. The run succeeds if any iteration succeeds. Zero or 1 means
	// run once. EnvFunc must be set when Iterations > 1.
	Iterations int

	// EnvFunc constructs a fresh InteractionEnv for each iteration. Required
	// when Iterations > 1; ignored when Iterations <= 1.
	EnvFunc func() *rafttest.InteractionEnv

	// ResetFunc resets any test-local state captured by filters, conditions,
	// setup, or tick callbacks before each iteration. Optional.
	ResetFunc func()

	// StateMachine asserts a property over the event stream. If nil, the test
	// succeeds as long as no error occurs and MaxRounds is not exceeded.
	StateMachine *StateMachine

	// Filters controls message delivery. If nil, all messages are delivered.
	Filters *FilterSet

	// SetupFunc is called before the main run loop to initialize the
	// InteractionEnv (e.g., add nodes, campaign, propose entries). May be nil.
	SetupFunc func(*rafttest.InteractionEnv) error

	// TickFunc is called once per round before processing begins, allowing
	// tests to advance logical clocks. If nil, no ticks are issued.
	// The function receives the current round number (1-based).
	TickFunc func(env *rafttest.InteractionEnv, round int)
}
