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

import (
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/rafttest"
)

// RunResult summarises the outcome of a TestCase run.
type RunResult struct {
	// Success is true when the state machine reached a success state (or no
	// state machine was provided and the run completed without error).
	Success bool
	// FinalState is the label of the state machine's final state, or
	// "no-state-machine" when none was provided.
	FinalState string
	// Rounds is the number of stabilize-like iterations performed.
	Rounds int
	// Iteration is the 1-based index of the iteration that produced this
	// result. Zero when Iterations <= 1 (single run).
	Iteration int
	// Events is the ordered list of events observed during the run.
	Events []*Event
	// Err holds any fatal error encountered during the run.
	Err error
}

// IsFailure reports whether the run ended in the FailureState.
func (r RunResult) IsFailure() bool {
	return r.FinalState == FailureState
}

// Run executes the TestCase against the provided InteractionEnv.
//
// When tc.Iterations > 1, Run ignores env and instead calls tc.EnvFunc to
// obtain a fresh environment for each iteration, resetting the state machine
// between attempts. The run succeeds as soon as any iteration succeeds.
//
// When tc.Iterations <= 1, Run behaves as a single run against env (backwards
// compatible).
func Run(tc *TestCase, env *rafttest.InteractionEnv) RunResult {
	iterations := tc.Iterations
	if iterations <= 1 {
		return runOnce(tc, env)
	}
	var last RunResult
	for i := 0; i < iterations; i++ {
		if tc.StateMachine != nil {
			tc.StateMachine.Reset()
		}
		if tc.ResetFunc != nil {
			tc.ResetFunc()
		}
		last = runOnce(tc, tc.EnvFunc())
		if last.Success {
			last.Iteration = i + 1
			return last
		}
	}
	last.Iteration = iterations
	return last
}

// runOnce drives the TestCase against env for a single attempt.
//
// It processes all pending raft Ready states and async threads each round,
// intercepts the in-flight message pool and routes each message through the
// TestCase's FilterSet, then steps the StateMachine. The loop ends when no
// more work remains, the state machine reaches a terminal state, or MaxRounds
// is exceeded.
func runOnce(tc *TestCase, env *rafttest.InteractionEnv) RunResult {
	ctx := NewContext()

	// Run optional setup.
	if tc.SetupFunc != nil {
		if err := tc.SetupFunc(env); err != nil {
			return RunResult{Err: fmt.Errorf("setup: %w", err)}
		}
	}

	var events []*Event
	eventSeq := 0
	nextEventID := func() string {
		eventSeq++
		return fmt.Sprintf("e%d", eventSeq)
	}

	filters := tc.Filters
	if filters == nil {
		filters = NewFilterSet()
	}

	sm := tc.StateMachine

	round := 0
	for {
		if tc.MaxRounds > 0 && round >= tc.MaxRounds {
			break
		}
		round++

		// Issue ticks for this round if configured.
		anyWork := tc.TickFunc != nil
		if tc.TickFunc != nil {
			tc.TickFunc(env, round)
		}

		// Process all pending Ready states.
		for i := range env.Nodes {
			if env.Nodes[i].HasReady() {
				anyWork = true
				if err := env.ProcessReady(i); err != nil {
					return RunResult{Err: fmt.Errorf("process-ready node %d: %w", i+1, err), Events: events, Rounds: round}
				}
			}
		}

		// Process async append/apply work.
		for i := range env.Nodes {
			for len(env.Nodes[i].AppendWork) > 0 {
				anyWork = true
				if err := env.ProcessAppendThread(i); err != nil {
					return RunResult{Err: fmt.Errorf("append-thread node %d: %w", i+1, err), Events: events, Rounds: round}
				}
			}
			for len(env.Nodes[i].ApplyWork) > 0 {
				anyWork = true
				if err := env.ProcessApplyThread(i); err != nil {
					return RunResult{Err: fmt.Errorf("apply-thread node %d: %w", i+1, err), Events: events, Rounds: round}
				}
			}
		}

		// Intercept in-flight messages and route through the filter set.
		pending := env.Messages
		env.Messages = nil

		for _, msg := range pending {
			anyWork = true
			e := &Event{
				ID:   nextEventID(),
				From: msg.GetFrom(),
				To:   msg.GetTo(),
				Msg:  msg,
				Type: MessageSendEventType,
			}
			ctx.MessagePool[e.ID] = msg
			events = append(events, e)

			toDeliver := filters.Apply(e, ctx)

			// Step the state machine on the send event before delivery.
			if sm != nil {
				sm.Step(e, ctx)
				if sm.IsTerminal() {
					deliverAll(env, toDeliver)
					return RunResult{
						Success:    sm.IsSuccess(),
						FinalState: sm.CurrentState(),
						Rounds:     round,
						Events:     events,
					}
				}
			}

			// Deliver decided messages.
			deliverAll(env, toDeliver)

			// Synthesize receive events for delivered messages.
			for _, m := range toDeliver {
				if raft.IsLocalMsgTarget(m.GetTo()) {
					continue
				}
				re := &Event{
					ID:   nextEventID(),
					From: m.GetFrom(),
					To:   m.GetTo(),
					Msg:  m,
					Type: MessageReceiveEventType,
				}
				events = append(events, re)
				if sm != nil {
					sm.Step(re, ctx)
					if sm.IsTerminal() {
						return RunResult{
							Success:    sm.IsSuccess(),
							FinalState: sm.CurrentState(),
							Rounds:     round,
							Events:     events,
						}
					}
				}
			}
		}

		if !anyWork {
			// Fixed point reached.
			break
		}
	}

	// Determine result.
	if sm == nil {
		return RunResult{Success: true, FinalState: "no-state-machine", Rounds: round, Events: events}
	}
	return RunResult{
		Success:    sm.IsSuccess(),
		FinalState: sm.CurrentState(),
		Rounds:     round,
		Events:     events,
	}
}

// deliverAll steps each message into its destination node.
func deliverAll(env *rafttest.InteractionEnv, msgs []*pb.Message) {
	for _, msg := range msgs {
		if raft.IsLocalMsgTarget(msg.GetTo()) {
			// Re-queue local messages for the append/apply thread.
			env.Messages = append(env.Messages, msg)
			continue
		}
		toIdx := int(msg.GetTo() - 1)
		if toIdx < 0 || toIdx >= len(env.Nodes) {
			continue
		}
		_ = env.Nodes[toIdx].Step(msg)
	}
}
