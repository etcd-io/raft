# Scenario Tests

Executable scenario tests for the etcd/raft protocol using the [Netrix DSL](./netrix).
Each test drives a cluster through the `rafttest.InteractionEnv` with message-level filter and
state-machine control, then asserts a safety or liveness outcome.

Spec references below point to the TLA+ specification at `tla/etcdraft.tla` unless noted otherwise.

---

## Election (`election_test.go`)

### `TestElectionPhases`

Traces the full election sequence — `MsgVote` → `MsgVoteResp` (granted) → `MsgApp` (leader no-op) — through a three-state machine.

**Spec:** `Timeout` → `RequestVote` → `BecomeLeader` actions.

---

### `TestElectionRequiresQuorum`

Drops every `MsgVote` and verifies that no `MsgApp` is ever produced, i.e., no leader is elected.

**Spec:** Quorum requirement in `BecomeLeader`.

---

### `TestElectionWithOneLostVote`

Drops one vote on one link and verifies that election still succeeds — the candidate reaches quorum through the remaining nodes.

**Spec:** Quorum tolerance (fault-tolerant `BecomeLeader`).

---

### `TestAtMostOneLeaderPerTerm`

Tracks `term → first leader` in an external map; fails if two different nodes claim leadership in the same term.

**Spec:** `MoreThanOneLeaderInv`.

---

### `TestLeaderElectionWithConcurrentCandidates`

Forces a split vote in a 5-node cluster (two simultaneous candidates, cross-candidate votes dropped), then verifies that a leader eventually emerges through term increment and retry.

**Spec:** Split-vote recovery path of `BecomeLeader`.

---

### `TestHigherTermMessageForcesStepdown`

Isolates node 3 long enough for it to accumulate a higher term, then heals the partition. Verifies that node 3's high-term `MsgVote` forces the sitting leader to step down and respond as a follower.

**Spec:** `UpdateTerm` / `StepDownToFollower` actions.

---

## Partitions (`partition_test.go`)

### `TestLeaderPartitionTriggersNewElection`

Isolates the current leader; verifies that the remaining majority elects a new leader (observes `MsgApp` from a non-original node).

**Spec:** `Timeout` and `RequestVote` under network partition.

---

### `TestMinorityCannotElectLeader`

Splits a 5-node cluster 2/3; verifies that the minority side (nodes 1–2) never produces a `MsgApp`, i.e., cannot elect a leader.

**Spec:** `QuorumLogInv` (quorum write requirement).

---

### `TestMajorityPartitionMakesProgress`

Splits 2/3 and verifies the majority side (nodes 3–5) successfully elects a leader and makes progress.

**Spec:** Liveness of `BecomeLeader` on the majority side.

---

### `TestFollowerRejoinsCatchesUp`

Isolates a follower, advances the cluster, heals the partition, and verifies the follower receives catch-up `MsgApp` entries.

**Spec:** `AppendEntries` action (log catch-up after partition heal).

---

## Log Replication (`log_replication_test.go`)

### `TestLeaderSendsEntriesToFollowers`

Proposes an entry and verifies the leader sends a non-empty `MsgApp` to followers.

**Spec:** `AppendEntries` action.

---

### `TestFollowerAcknowledgesAppend`

Verifies that at least two followers reply with a successful `MsgAppResp`, which is the precondition for `AdvanceCommitIndex`.

**Spec:** `AdvanceCommitIndex` action.

---

### `TestHeartbeatsSentByLeader`

Counts `MsgHeartbeat` messages and verifies the leader emits at least three heartbeats, confirming periodic leadership maintenance.

**Spec:** `Heartbeat(i,j)` action.

---

### `TestDroppedAppendRetriedByLeader`

Drops the first `MsgApp` and verifies the leader retries, ensuring eventual delivery under unreliable links.

**Spec:** Retry liveness requirement (implicit in `AppendEntries`).

---

### `TestCommitRequiresMajority`

In a 5-node cluster, permanently drops traffic to/from the two minority nodes; verifies that the remaining majority can still commit entries (majority acknowledgements reach the leader).

**Spec:** `QuorumLogInv`.

---

## Leader Safety (`leader_safety_test.go`)

### `TestCommittedEntriesNeverDiverge`

Tracks the maximum `Commit` value seen per leader across all `MsgApp` messages; fails if any leader ever announces a lower commit index than it previously announced.

**Spec:** `LogInv` (log monotonicity / committed entries never retracted).

---

### `TestNewLeaderHasAllCommittedEntries`

Commits entries on node 1, isolates it, waits for a new leader to emerge, and verifies the new leader's commit index matches or exceeds the old leader's commit index.

**Spec:** `LeaderCompletenessInv`.

---

### `TestTermMonotonicity`

Tracks the maximum term seen per sender across all messages; fails if any node ever sends a message with a lower term than it previously used.

**Spec:** Term monotonicity (necessary safety property of the Raft term mechanism).

---

## CheckQuorum (`checkquorum_test.go`)

### `TestCheckQuorumLeaderStepsDown`

With `CheckQuorum=true`, drops all `MsgHeartbeatResp` and `MsgAppResp` going to the leader; verifies the leader steps down after `ElectionTick` rounds by observing it send a `MsgVoteResp` as a follower.

**Spec:** `StepDownToFollower` action (CheckQuorum extension).

---

### `TestCheckQuorumNotTriggeredWithActiveFollowers`

With `CheckQuorum=true` and followers responding normally, verifies the leader never sends `MsgVote` (i.e., does not step down prematurely).

**Spec:** Liveness guard of the CheckQuorum extension.

---

## PreVote (`prevote_test.go`)

### `TestPreVoteMessagesSentBeforeRealVote`

With `PreVote=true`, verifies that `MsgPreVote` is sent before `MsgVote` during election, preventing term disruption from isolated candidates.

**Spec:** Pre-vote protocol (etcd extension to Raft, originally proposed by Ongaro).

---

### `TestPreVoteGrantedLeadsToElection`

Verifies that a granted `MsgPreVoteResp` causes the candidate to proceed with a real `MsgVote`.

**Spec:** Pre-vote protocol state-machine (etcd extension).

---

### `TestPreVotePreventsTrumpByStaleNode`

Isolates node 3 until it accumulates a high term, then heals; verifies that with PreVote enabled, the stale node's pre-vote is rejected by the healthy cluster and the sitting leader is not disrupted.

**Spec:** Term-disruption prevention property of the pre-vote protocol (etcd extension).

---

## Leadership Transfer (`leadership_transfer_test.go`)

### `TestLeadershipTransferComplete`

Initiates a `TransferLeader` call and verifies the full handoff sequence: `MsgTimeoutNow` from the current leader → `MsgVote` from the transferee → `MsgApp` from the new leader.

**Spec:** `campaignTransfer` path (etcd leadership transfer extension).

---

### `TestLeadershipTransferCatchesUpLaggingTarget`

Isolates the transfer target (node 3), advances the log, heals, then initiates transfer; verifies the leader sends catch-up `MsgApp` entries to the target before issuing `MsgTimeoutNow`.

**Spec:** Log catch-up requirement of the etcd leadership transfer extension.

---

## Membership Changes (`membership_test.go`)

### `TestAddServerViaConfChange`

Proposes a `ConfChangeV2` to add a server and verifies the leader replicates a `MsgApp` carrying an `EntryConfChange` entry.

**Spec:** `AddNewServer` action.

---

### `TestAddLearnerReceivesReplication`

Adds a learner node (node 4) via `ConfChange` and verifies the leader sends `MsgApp` to it, confirming the learner is included in replication.

**Spec:** `AddLearner` action.

---

### `TestRemoveServerNoLeadershipLoss`

Proposes a `ConfChange` to remove a follower and verifies the leader continues to send heartbeats afterward, confirming no leadership disruption.

**Spec:** `DeleteServer` action.

---

## Message Delivery (`message_delivery_test.go`)

### `TestDelayedVotesStillElect`

Buffers all `MsgVote` messages for several rounds and then releases them; verifies that delayed delivery still results in a successful election.

**Spec:** Delayed message delivery model (`DuplicateMessage` / eventual delivery assumption).

---

### `TestDuplicateAppendEntryHandled`

Delivers a `MsgApp` once normally and once again as a replay; verifies the cluster makes progress despite the duplicate, confirming idempotent append handling.

**Spec:** `DuplicateMessage` action.

---

### `TestDroppedHeartbeatsNoLeaderLoss`

Drops all `MsgHeartbeat` and `MsgHeartbeatResp` messages; verifies the leader is not lost as long as `MsgApp` replication keeps followers active.

**Spec:** Liveness under heartbeat loss (AppendEntries subsumes Heartbeat for follower timeout reset).

---

### `TestOutOfOrderAppendHandled`

Buffers two `MsgApp` messages and replays them in reverse order; verifies the follower eventually acknowledges entries and the leader commits them.

**Spec:** `LogMatchingInv` (out-of-order delivery tolerance).

---

## Snapshots (`snapshot_test.go`)

### `TestSnapshotSentToLaggingFollower`

Isolates node 3, compacts the log to index 13 on the leader (beyond the follower's `nextIndex`), heals, and verifies the leader sends `MsgSnap` to bring the follower up to date.

**Spec:** `SendSnapshot(i,j,index)` action.

---

### `TestSnapshotDeliveredAndFollowedByAppend`

Verifies that after a `MsgSnap` is delivered and the follower installs the snapshot, the leader resumes sending `MsgApp` entries to that follower.

**Spec:** Post-snapshot replication resumption (implicit in `SendSnapshot` + `AppendEntries` sequencing).

---

### `TestCommitIndexMonotonicDuringSnapshot`

Tracks `max(Commit)` per leader across all `MsgApp` messages around a snapshot installation cycle; fails if the commit index ever decreases.

**Spec:** `CommittedIsDurableInv`.
