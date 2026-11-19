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

// Package scenariotests contains executable scenario tests for the raft
// protocol, covering safety invariants from the TLA+ specification
// (tla/etcdraft.tla) and etcd-specific extensions (PreVote, CheckQuorum,
// leadership transfer).
//
// Each test file covers one class of scenarios:
//
//   - election_test.go          election mechanics and safety
//   - partition_test.go         network partition scenarios
//   - log_replication_test.go   AppendEntries flow and commit rules
//   - leader_safety_test.go     TLA+ safety invariants
//   - prevote_test.go           PreVote extension
//   - checkquorum_test.go       CheckQuorum extension
//   - membership_test.go        dynamic membership changes
//   - leadership_transfer_test.go  leader transfer protocol
//   - message_delivery_test.go  message reliability scenarios
//   - snapshot_test.go          snapshot installation
package scenariotests_test
