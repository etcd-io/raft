---------- MODULE MCetcdraft ----------
\* Model checking wrapper for etcdraft

EXTENDS etcdraft

\* Constraint constants

CONSTANT Value

CONSTANT ReconfigurationLimit
ASSUME ReconfigurationLimit \in Nat

CONSTANT MaxTermLimit
ASSUME MaxTermLimit \in Nat

CONSTANT RequestLimit
ASSUME RequestLimit \in Nat

\* Fault injection limits
CONSTANT RestartLimit
ASSUME RestartLimit \in Nat

CONSTANT DropLimit
ASSUME DropLimit \in Nat

CONSTANT DuplicateLimit
ASSUME DuplicateLimit \in Nat

CONSTANT StepDownLimit
ASSUME StepDownLimit \in Nat

CONSTANT HeartbeatLimit
ASSUME HeartbeatLimit \in Nat

CONSTANT MaxTimeoutLimit
ASSUME MaxTimeoutLimit \in Nat

CONSTANT SnapshotLimit
ASSUME SnapshotLimit \in Nat

CONSTANT CompactLimit
ASSUME CompactLimit \in Nat

CONSTANT ConfChangeLimit
ASSUME ConfChangeLimit \in Nat

CONSTANT ReportUnreachableLimit
ASSUME ReportUnreachableLimit \in Nat

CONSTANT MaxMsgBufferLimit
ASSUME MaxMsgBufferLimit \in Nat

CONSTANT MaxPendingMsgLimit
ASSUME MaxPendingMsgLimit \in Nat

CONSTANT PartitionLimit
ASSUME PartitionLimit \in Nat

\* Constraint variables
VARIABLE constraintCounters

faultVars == <<constraintCounters>>

etcd == INSTANCE etcdraft

\* Bootstrap initialization
BootstrapLog ==
    LET prevConf(y) == IF Len(y) = 0 THEN {} ELSE y[Len(y)].value.newconf
    IN FoldSeq(LAMBDA x, y: Append(y, [ term  |-> 1, type |-> ConfigEntry, value |-> [ newconf |-> prevConf(y) \union {x}, learners |-> {} ] ]), <<>>, SetToSeq(InitServer))

\* Bootstrap: InitServer members start with term 1 and config log entries
etcdInitServerVars == /\ currentTerm = [i \in Server |-> IF i \in InitServer THEN 1 ELSE 0]
                      /\ state       = [i \in Server |-> Follower]
                      /\ votedFor    = [i \in Server |-> Nil]

etcdInitLogVars == /\ log = [i \in Server |-> IF i \in InitServer
                                              THEN [offset |-> 1, entries |-> BootstrapLog, snapshotIndex |-> 0, snapshotTerm |-> 0]
                                              ELSE [offset |-> 1, entries |-> <<>>, snapshotIndex |-> 0, snapshotTerm |-> 0]]
                   /\ historyLog = [i \in Server |-> IF i \in InitServer THEN BootstrapLog ELSE <<>>]
                   /\ commitIndex = [i \in Server |-> IF i \in InitServer THEN Cardinality(InitServer) ELSE 0]
                   /\ applied = [i \in Server |-> IF i \in InitServer THEN Cardinality(InitServer) ELSE 0]

etcdInitConfigVars == /\ config = [i \in Server |-> [ jointConfig |-> IF i \in InitServer THEN <<InitServer, {}>> ELSE <<{}, {}>>, learners |-> {}, autoLeave |-> FALSE]]
                      /\ reconfigCount = 0 \* bootstrap configs not counted
                      \* Bootstrap config entries are already applied (committed at Cardinality(InitServer))
                      /\ appliedConfigIndex = [i \in Server |-> IF i \in InitServer THEN Cardinality(InitServer) ELSE 0]

etcdInitDurableState ==
    durableState = [ i \in Server |-> [
        currentTerm |-> IF i \in InitServer THEN 1 ELSE 0,
        votedFor |-> Nil,
        log |-> IF i \in InitServer THEN Cardinality(InitServer) ELSE 0,
        entries |-> IF i \in InitServer THEN BootstrapLog ELSE <<>>,  \* Persisted log entries
        snapshotIndex |-> 0,
        snapshotTerm |-> 0,
        snapshotHistory |-> <<>>,  \* History covered by snapshot (initially empty)
        commitIndex |-> IF i \in InitServer THEN Cardinality(InitServer) ELSE 0,
        config |-> [ jointConfig |-> IF i \in InitServer THEN <<InitServer, {}>> ELSE <<{}, {}>>, learners |-> {}, autoLeave |-> FALSE]
    ]]

\* Constrained actions

MCAddNewServer(i, j) ==
    /\ reconfigCount < ReconfigurationLimit
    /\ etcd!AddNewServer(i, j)

MCDeleteServer(i, j) ==
    /\ reconfigCount < ReconfigurationLimit
    /\ etcd!DeleteServer(i, j)

MCAddLearner(i, j) ==
    /\ reconfigCount < ReconfigurationLimit
    /\ etcd!AddLearner(i, j)

\* MaxTermLimit should be >= 3 to avoid over-constraining candidate quorum
MCTimeout(i) ==
    /\ currentTerm[i] < MaxTermLimit
    /\ constraintCounters.timeout < MaxTimeoutLimit
    /\ etcd!Timeout(i)
    /\ constraintCounters' = [constraintCounters EXCEPT !.timeout = @ + 1]

MCClientRequest(i, v) ==
    /\ constraintCounters.request < RequestLimit
    /\ etcd!ClientRequest(i, v)
    /\ constraintCounters' = [constraintCounters EXCEPT !.request = @ + 1]

MCRestart(i) ==
    /\ constraintCounters.restart < RestartLimit
    /\ etcd!Restart(i)
    /\ constraintCounters' = [constraintCounters EXCEPT !.restart = @ + 1]

MCDropMessage(m) ==
    /\ constraintCounters.drop < DropLimit
    /\ etcd!DropMessage(m)
    /\ constraintCounters' = [constraintCounters EXCEPT !.drop = @ + 1]

MCDuplicateMessage(m) ==
    /\ constraintCounters.duplicate < DuplicateLimit
    /\ etcd!DuplicateMessage(m)
    /\ constraintCounters' = [constraintCounters EXCEPT !.duplicate = @ + 1]

MCHeartbeat(i, j) ==
    /\ constraintCounters.heartbeat < HeartbeatLimit
    /\ etcd!Heartbeat(i, j)
    /\ constraintCounters' = [constraintCounters EXCEPT !.heartbeat = @ + 1]

MCStepDown(i) ==
    /\ constraintCounters.stepDown < StepDownLimit
    /\ etcd!StepDownToFollower(i)
    /\ constraintCounters' = [constraintCounters EXCEPT !.stepDown = @ + 1]

MCSendSnapshot(i, j) ==
    /\ constraintCounters.snapshot < SnapshotLimit
    /\ etcd!SendSnapshot(i, j)
    /\ constraintCounters' = [constraintCounters EXCEPT !.snapshot = @ + 1]

MCCompactLog(i, newStart) ==
    /\ constraintCounters.compact < CompactLimit
    /\ etcd!CompactLog(i, newStart)
    /\ constraintCounters' = [constraintCounters EXCEPT !.compact = @ + 1]

MCManualSendSnapshot(i, j) ==
    /\ constraintCounters.snapshot < SnapshotLimit
    /\ etcd!ManualSendSnapshot(i, j)
    /\ constraintCounters' = [constraintCounters EXCEPT !.snapshot = @ + 1]

MCSendSnapshotWithCompaction(i, j, idx) ==
    /\ constraintCounters.snapshot < SnapshotLimit
    /\ etcd!SendSnapshotWithCompaction(i, j, idx)
    /\ constraintCounters' = [constraintCounters EXCEPT !.snapshot = @ + 1]

MCChangeConf(i) ==
    /\ constraintCounters.confChange < ConfChangeLimit
    /\ etcd!ChangeConf(i)
    /\ constraintCounters' = [constraintCounters EXCEPT !.confChange = @ + 1]

MCChangeConfAndSend(i) ==
    /\ constraintCounters.confChange < ConfChangeLimit
    /\ etcd!ChangeConfAndSend(i)
    /\ constraintCounters' = [constraintCounters EXCEPT !.confChange = @ + 1]

MCClientRequestAndSend(i, v) ==
    /\ constraintCounters.request < RequestLimit
    /\ etcd!ClientRequestAndSend(i, v)
    /\ constraintCounters' = [constraintCounters EXCEPT !.request = @ + 1]

MCReportUnreachable(i, j) ==
    /\ constraintCounters.reportUnreachable < ReportUnreachableLimit
    /\ etcd!ReportUnreachable(i, j)
    /\ constraintCounters' = [constraintCounters EXCEPT !.reportUnreachable = @ + 1]

\* Deduplicate AppendEntries: one per (source, dest, term) pair
MCSend(msg) ==
    /\ ~ \E n \in DOMAIN messages \union DOMAIN pendingMessages:
        /\ n.mdest = msg.mdest
        /\ n.msource = msg.msource
        /\ n.mterm = msg.mterm
        /\ n.mtype = AppendEntriesRequest
        /\ msg.mtype = AppendEntriesRequest
    /\ ~ \E n \in DOMAIN messages \union DOMAIN pendingMessages:
        /\ n.mdest = msg.msource
        /\ n.msource = msg.mdest
        /\ n.mterm = msg.mterm
        /\ n.mtype = AppendEntriesResponse
        /\ msg.mtype = AppendEntriesRequest
    /\ etcd!Send(msg)

\* Network partition actions

MCCreatePartition(group1) ==
    /\ constraintCounters.partition < PartitionLimit
    /\ LET partitionAssignment == [i \in Server |-> IF i \in group1 THEN 1 ELSE 2]
       IN etcd!CreatePartition(partitionAssignment)
    /\ constraintCounters' = [constraintCounters EXCEPT !.partition = @ + 1]

MCHealPartition ==
    /\ etcd!HealPartition
    /\ UNCHANGED constraintCounters

MCInit ==
    /\ etcd!Init
    /\ constraintCounters = [restart |-> 0, drop |-> 0, duplicate |-> 0, stepDown |-> 0, heartbeat |-> 0, snapshot |-> 0, compact |-> 0, confChange |-> 0, reportUnreachable |-> 0, timeout |-> 0, request |-> 0, partition |-> 0]

\* Next state relations

MCNextAsync(i) ==
    \/ /\ \E j \in Server : etcd!RequestVote(i, j)
       /\ UNCHANGED faultVars
    \/ /\ etcd!BecomeLeader(i)
       /\ UNCHANGED faultVars
    \/ \E v \in Value: MCClientRequest(i, v)
    \* \/ \E v \in Value: MCClientRequestAndSend(i, v)
    \/ /\ etcd!AdvanceCommitIndex(i)
       /\ UNCHANGED faultVars
    \* Allows nextIndex below log offset to explore bug scenarios (e.g., 76f1249).
    \* Correct behavior is enforced by invariants, not action constraints.
    \/ /\ \E j \in Server :
           /\ \E e \in nextIndex[i][j]..LastIndex(log[i])+1 : etcd!AppendEntries(i, j, <<nextIndex[i][j], e>>)
       /\ UNCHANGED faultVars
    \/ /\ etcd!AppendEntriesToSelf(i)
       /\ UNCHANGED faultVars
    \/ /\ \E j \in Server : MCHeartbeat(i, j)
    \/ /\ \E j \in Server : MCSendSnapshot(i, j)
    \/ /\ \E j \in Server : MCManualSendSnapshot(i, j)
    \/ /\ \E j \in Server : \E idx \in 1..commitIndex[i] :
           /\ idx > 0
           /\ MCSendSnapshotWithCompaction(i, j, idx)
    \/ /\ \E j \in Server : MCReportUnreachable(i, j)
    \/ /\ etcd!ReplicateImplicitEntry(i)
       /\ UNCHANGED faultVars
    \* Only compact to commitIndex for most aggressive compaction scenario
    \/ /\ log[i].offset < commitIndex[i]
       /\ MCCompactLog(i, commitIndex[i])
    \/ MCTimeout(i)
    \* Ready handled via MCReady composition
    \* \/ /\ etcd!Ready(i)
    \*    /\ UNCHANGED faultVars
    \/ MCStepDown(i)
    \/ /\ \E m \in DOMAIN messages : 
           /\ m.mdest = i
           /\ etcd!Receive(m)
       /\ UNCHANGED faultVars

\* Ready action: applies if enabled, otherwise no-op
MCReady(i) ==
    IF ENABLED etcd!Ready(i)
    THEN /\ etcd!Ready(i)
         /\ UNCHANGED faultVars
    ELSE UNCHANGED <<vars, faultVars>>

\* Async action composed with Ready to reduce state space
MCNextAsyncWithReady(i) ==
    MCNextAsync(i) \cdot MCReady(i)

MCNextAsyncAll ==
    \/ \E i \in Server : MCNextAsync(i)
    \/ \E i \in Server : 
       /\ etcd!Ready(i)
       /\ UNCHANGED faultVars

MCNextAsyncWithReadyAll ==
    \E i \in Server : MCNextAsyncWithReady(i)

MCNextCrash == \E i \in Server : MCRestart(i)

MCNextUnreliable ==
    \* Only duplicate once
    \/ \E m \in DOMAIN messages :
        /\ messages[m] = 1
        /\ MCDuplicateMessage(m)
    \* Only drop if it makes a difference
    \/ \E m \in DOMAIN messages :
        /\ messages[m] = 1
        /\ MCDropMessage(m)

MCNextPartition ==
    \/ \E group1 \in SUBSET(Server) :
        /\ group1 /= {}
        /\ group1 /= Server
        /\ MCCreatePartition(group1)
    \/ MCHealPartition

MCNext ==
    \/ MCNextAsyncAll
    \/ MCNextCrash
    \/ MCNextUnreliable
    \/ MCNextPartition

MCNextWithReady ==
    \/ MCNextAsyncWithReadyAll
    \/ MCNextCrash
    \/ MCNextUnreliable
    \/ MCNextPartition

\* Config changes go through ChangeConf, not direct Add/Delete
MCDynamicConfigActions ==
    \/ /\ \E i \in Server : MCChangeConf(i)
    \* \/ /\ \E i \in Server : MCChangeConfAndSend(i)
    \/ /\ \E i \in Server : etcd!ApplySimpleConfChange(i)
       /\ UNCHANGED faultVars
    \* ProposeLeaveJoint: AutoLeave per raft.go:745-760
    \/ /\ \E i \in Server : etcd!ProposeLeaveJoint(i)
       /\ UNCHANGED faultVars

MCNextDynamic ==
    \/ MCNext
    \/ MCDynamicConfigActions

MCNextDynamicWithReady ==
    \/ MCNextWithReady
    \/ MCDynamicConfigActions

mc_vars == <<vars, faultVars>>

mc_etcdSpec ==
    /\ MCInit
    /\ [][MCNextDynamic]_mc_vars

mc_etcdSpecWithReady ==
    /\ MCInit
    /\ [][MCNextDynamicWithReady]_mc_vars

mc_etcdSpec_no_conf_change ==
    /\ MCInit
    /\ [][MCNext]_mc_vars

mc_etcdSpecWithReady_no_conf_change ==
    /\ MCInit
    /\ [][MCNextWithReady]_mc_vars

\* Symmetry and view

\* Enabled via cfg file; may be unsound
Symmetry == Permutations(Server) \union Permutations(Value)

\* Excludes constraintCounters from state identity
ModelView == << view_vars >>

\* State space pruning

\* Limit=0 means no limit
MsgBufferConstraint ==
    \/ MaxMsgBufferLimit = 0
    \/ BagCardinality(messages) <= MaxMsgBufferLimit

PendingMsgBufferConstraint ==
    \/ MaxPendingMsgLimit = 0
    \/ BagCardinality(pendingMessages) <= MaxPendingMsgLimit

AllMsgBufferConstraint ==
    /\ MsgBufferConstraint
    /\ PendingMsgBufferConstraint

\* Monotonicity properties

\* Excludes Restart (durableState may have older commitIndex)
MonotonicCommitIndexProp ==
    [][(~\E i \in Server: Restart(i)) =>
        \A i \in Server : commitIndex'[i] >= commitIndex[i]]_mc_vars

\* Excludes Restart (durableState may have older term)
MonotonicTermProp ==
    [][(~\E i \in Server: Restart(i)) =>
        \A i \in Server : currentTerm'[i] >= currentTerm[i]]_mc_vars

\* matchIndex is monotonic except during BecomeLeader, Restart, or config re-add.
\* Re-added nodes get matchIndex reset to 0 (confchange.go initProgress).
MonotonicMatchIndexProp ==
    [][(~ \E i \in Server: etcd!BecomeLeader(i) \/ etcd!Restart(i)) =>
            (\A i,j \in Server :
                LET
                    preConfig == config[i].jointConfig[1] \cup config[i].jointConfig[2] \cup config[i].learners
                    postConfig == config'[i].jointConfig[1] \cup config'[i].jointConfig[2] \cup config'[i].learners
                IN
                (j \in preConfig /\ j \in postConfig) => matchIndex'[i][j] >= matchIndex[i][j])]_mc_vars

\* Leader only commits entries from its current term (Raft paper Figure 8 safety)
LeaderCommitCurrentTermLogsProp ==
    [][
        \A i \in Server :
            (state'[i] = Leader /\ commitIndex[i] /= commitIndex'[i]) =>
                historyLog'[i][commitIndex'[i]].term = currentTerm'[i]
    ]_mc_vars

\* Raft paper core properties

\* Leader Append-Only (Raft Figure 3, Property 2): log only grows, existing entries unchanged
LeaderAppendOnlyProp ==
    [][
        \A i \in Server :
            (state[i] = Leader /\ state'[i] = Leader) =>
                /\ LastIndex(log'[i]) >= LastIndex(log[i])
                /\ SubSeq(historyLog'[i], 1, LastIndex(log[i])) =
                   SubSeq(historyLog[i], 1, LastIndex(log[i]))
    ]_mc_vars

\* Committed entries persist across all transitions (excludes Restart)
CommittedEntriesPersistProp ==
    [][(~\E i \in Server: etcd!Restart(i)) =>
        \A i \in Server :
            SubSeq(historyLog'[i], 1, commitIndex[i]) =
            SubSeq(historyLog[i], 1, commitIndex[i])
    ]_mc_vars

=============================================================================
