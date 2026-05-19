// Copyright 2019 The etcd Authors
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

package rafttest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// Handle is the entrypoint for data-driven interaction testing. Commands and
// parameters are parsed from the supplied TestData. Errors during data parsing
// are reported via the supplied *testing.T; errors from the raft nodes and the
// storage engine are reported to the output buffer.
func (env *InteractionEnv) Handle(t *testing.T, d datadriven.TestData) string {
	env.Output.Reset()
	var err error
	switch d.Cmd {
	case "propose-conf-change":
		// Propose a configuration change, or transition out of a previously
		// proposed joint configuration change that requested explicit
		// transitions. When adding nodes, this command can be used to
		// logically add nodes to the configuration, but add-nodes is needed
		// to "create" the nodes.
		//
		// propose-conf-change node_id [v1=<bool>] [transition=<string>]
		// command string
		// See ConfChangesFromString for command string format.
		// Arguments are:
		//    node_id - the node proposing the configuration change.
		//    v1 - make one change at a time, false by default.
		//    transition - "auto" (the default), "explicit" or "implicit".
		// Example:
		//
		// propose-conf-change 1 transition=explicit
		// v1 v3 l4 r5
		//
		// Example:
		//
		// propose-conf-change 2 v1=true
		// v5
		err = env.handleProposeConfChange(t, d)
	default:
		err = env.handleMultiCommand(t, d)
	}
	// NB: the highest log level suppresses all output, including that of the
	// handlers. This comes in useful during setup which can be chatty.
	// However, errors are always logged.
	if err != nil {
		if env.Output.Quiet() {
			return err.Error()
		}
		env.Output.WriteString(err.Error())
	}
	if env.Output.Len() == 0 {
		return "ok"
	}
	return env.Output.String()
}

func (env *InteractionEnv) handleMultiCommand(t *testing.T, d datadriven.TestData) error {
	err := env.handleSingle(t, d.Cmd, d.CmdArgs)
	if err != nil {
		return err
	}

	commands := d.Input
	for len(commands) != 0 {
		command := commands
		next := strings.IndexByte(commands, '\n')
		if next == -1 {
			commands = ""
		} else {
			command, commands = commands[:next], commands[next+1:]
		}
		cmd, args, err := datadriven.ParseLine(command)
		if err != nil {
			t.Fatalf("could not parse the command line %q: %v", command, err)
		}

		if err := env.handleSingle(t, cmd, args); err != nil {
			if env.Output.Quiet() {
				return err
			}
			env.Output.WriteString(err.Error())
		}
	}

	return nil
}

func (env *InteractionEnv) handleSingle(t *testing.T, cmd string, args []datadriven.CmdArg) error {
	switch cmd {
	case "_breakpoint":
		// This is a helper case to attach a debugger to when a problem needs
		// to be investigated in a longer test file. In such a case, add the
		// following stanza immediately before the interesting behavior starts:
		//
		// _breakpoint:
		// ----
		// ok
		//
		// and set a breakpoint on the `case` above.
	case "add-nodes":
		// Example:
		//
		// add-nodes <number-of-nodes-to-add> voters=(1 2 3) learners=(4 5) index=2 content=foo async-storage-writes=true
		return env.handleAddNodes(t, args)
	case "campaign":
		// Example:
		//
		// campaign <id-of-candidate>
		return env.handleCampaign(t, args)
	case "compact":
		// Example:
		//
		// compact <id> <new-first-index>
		return env.handleCompact(t, args)
	case "deliver-msgs":
		// Deliver the messages for a given recipient.
		//
		// Example:
		//
		// deliver-msgs <idx> type=MsgApp drop=(2,3)
		return env.handleDeliverMsgs(t, args)
	case "process-ready":
		// Example:
		//
		// process-ready 3
		return env.handleProcessReady(t, args)
	case "process-append-thread":
		// Example:
		//
		// process-append-thread 3
		return env.handleProcessAppendThread(t, args)
	case "process-apply-thread":
		// Example:
		//
		// process-apply-thread 3
		return env.handleProcessApplyThread(t, args)
	case "log-level":
		// Set the log level. NONE disables all output, including from the test
		// harness (except errors).
		//
		// Example:
		//
		// log-level WARN
		return env.handleLogLevel(args)
	case "raft-log":
		// Print the Raft log.
		//
		// Example:
		//
		// raft-log 3
		return env.handleRaftLog(t, args)
	case "raft-state":
		// Print Raft state of all nodes (whether the node is leading,
		// following, etc.). The information for node n is based on
		// n's view.
		return env.handleRaftState()
	case "set-randomized-election-timeout":
		// Set the randomized election timeout for the given node. Will be reset
		// again when the node changes state.
		//
		// Example:
		//
		// set-randomized-election-timeout 1 timeout=5
		return env.handleSetRandomizedElectionTimeout(t, args)
	case "stabilize":
		// Deliver messages to and run process-ready on the set of IDs until
		// no more work is to be done. If no ids are given, all nodes are used.
		//
		// Example:
		//
		// stabilize 1 4
		return env.handleStabilize(t, args)
	case "status":
		// Print Raft status.
		//
		// Example:
		//
		// status 5
		return env.handleStatus(t, args)
	case "tick-election":
		// Tick an election timeout interval for the given node (but beware the
		// randomized timeout).
		//
		// Example:
		//
		// tick-election 3
		return env.handleTickElection(t, args)
	case "tick-heartbeat":
		// Tick a heartbeat interval.
		//
		// Example:
		//
		// tick-heartbeat 3
		return env.handleTickHeartbeat(t, args)
	case "transfer-leadership":
		// Transfer the Raft leader.
		//
		// Example:
		//
		// transfer-leadership from=1 to=4
		return env.handleTransferLeadership(t, args)
	case "forget-leader":
		// Forgets the current leader of the given node.
		//
		// Example:
		//
		// forget-leader 1
		return env.handleForgetLeader(t, args)
	case "send-snapshot":
		// Sends a snapshot to a node. Takes the source and destination node.
		// The message will be queued, but not delivered automatically.
		//
		// Example: send-snapshot 1 3
		return env.handleSendSnapshot(t, args)
	case "propose":
		// Propose an entry.
		//
		// Example:
		//
		// propose 1 foo
		return env.handlePropose(t, args)
	case "report-unreachable":
		// Calls <1st>.ReportUnreachable(<2nd>).
		//
		// Example:
		// report-unreachable 1 2
		return env.handleReportUnreachable(t, args)
	default:
	}
	return fmt.Errorf("unknown command")
}

func firstAsInt(t *testing.T, args []datadriven.CmdArg) int {
	t.Helper()
	n, err := strconv.Atoi(args[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func firstAsNodeIdx(t *testing.T, args []datadriven.CmdArg) int {
	t.Helper()
	n := firstAsInt(t, args)
	return n - 1
}

func nodeIdxs(t *testing.T, args []datadriven.CmdArg) []int {
	var ints []int
	for i := 0; i < len(args); i++ {
		if len(args[i].Vals) != 0 {
			continue
		}
		n, err := strconv.Atoi(args[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		ints = append(ints, n-1)
	}
	return ints
}
