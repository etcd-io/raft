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

// Package netrix implements the Netrix DSL for testing distributed protocol
// implementations, as described in "A Domain Specific Language for Testing
// Consensus Implementations" (Dragoi, Enea, Nagendra, Srivas, 2023).
//
// It builds on top of the rafttest package and provides composable
// Conditions, Actions, Filters, and StateMachines for expressing test
// scenarios and asserting properties of the Raft implementation.
package netrix

import (
	pb "go.etcd.io/raft/v3/raftpb"
)

// EventType categorizes events produced during test execution.
type EventType int

const (
	// MessageSendEventType is an outbound message from a node entering the
	// in-flight pool.
	MessageSendEventType EventType = iota
	// MessageReceiveEventType is a message delivered to a destination node.
	MessageReceiveEventType
)

// Event represents a single computation step observed during test execution.
// It mirrors the Netrix Event abstraction from the paper.
type Event struct {
	// ID uniquely identifies this event within a run.
	ID string
	// From is the sending node ID (0 if not applicable).
	From uint64
	// To is the receiving node ID (0 if not applicable).
	To uint64
	// Msg is the raft message associated with this event (nil for non-message events).
	Msg *pb.Message
	// Type classifies the event.
	Type EventType
}

// Context is passed to every Condition and Action invocation. It is shared
// across all filter and state machine steps within a single test run, letting
// filters accumulate state (counters, recorded messages) across events.
type Context struct {
	// MessagePool stores all in-flight messages indexed by their event ID.
	MessagePool map[string]*pb.Message
	// VarSet is a generic key-value store for inter-filter state sharing.
	// Counters (int) and MessageSets ([]*pb.Message) are stored here by label.
	VarSet map[string]any
}

// NewContext creates an empty Context ready for use in a test run.
func NewContext() *Context {
	return &Context{
		MessagePool: make(map[string]*pb.Message),
		VarSet:      make(map[string]any),
	}
}

// GetVar retrieves a typed value from VarSet.
func GetVar[T any](c *Context, key string) (T, bool) {
	v, ok := c.VarSet[key].(T)
	return v, ok
}

// SetVar stores a typed value in VarSet.
func SetVar[T any](c *Context, key string, val T) {
	c.VarSet[key] = val
}

func (c *Context) getInt(key string) int {
	v, _ := GetVar[int](c, key)
	return v
}

func (c *Context) setInt(key string, val int) {
	SetVar(c, key, val)
}

func (c *Context) getMsgs(key string) []*pb.Message {
	v, _ := GetVar[[]*pb.Message](c, key)
	return v
}

func (c *Context) setMsgs(key string, msgs []*pb.Message) {
	SetVar(c, key, msgs)
}
