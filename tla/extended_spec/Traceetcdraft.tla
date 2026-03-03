---------------------------- MODULE Traceetcdraft --------------------------

EXTENDS etcdraft, Json, IOUtils, Sequences, TLC

\* raft.pb.go enum MessageType
RaftMsgType ==
    "MsgApp" :> AppendEntriesRequest @@ "MsgAppResp" :> AppendEntriesResponse @@
    "MsgVote" :> RequestVoteRequest @@ "MsgVoteResp" :> RequestVoteResponse @@
    "MsgHeartbeat" :> AppendEntriesRequest @@ "MsgHeartbeatResp" :> AppendEntriesResponse @@
    "MsgSnap" :> SnapshotRequest

RaftMsgSubtype ==
    "MsgHeartbeat" :> "heartbeat" @@ "MsgHeartbeatResp" :> "heartbeat" @@
    "MsgApp" :> "app" @@ "MsgAppResp" :> "app" @@
    "MsgSnap" :> "snapshot"

RaftRole ==
    "StateFollower" :> Follower @@ "StateCandidate" :> Candidate @@ "StatePreCandidate" :> Candidate @@ "StateLeader" :> Leader

-------------------------------------------------------------------------------------

\* Trace validation has been designed for TLC running in default model-checking
\* mode, i.e., breadth-first search.
ASSUME TLCGet("config").mode = "bfs"

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./example.ndjson"

OriginTraceLog ==
    \* Deserialize the System log as a sequence of records from the log file.
    SelectSeq(ndJsonDeserialize(JsonFile), LAMBDA l: "event" \in DOMAIN l)

TraceLog ==
    TLCEval(IF "MAX_TRACE" \in DOMAIN IOEnv THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE)) ELSE OriginTraceLog)

TraceServer == TLCEval(FoldSeq(
    LAMBDA x, y: y \cup
        {x.event.nid} \cup
        (IF "msg" \in DOMAIN x.event /\ "to" \in DOMAIN x.event.msg THEN {x.event.msg.to} ELSE {}) \cup
        (IF x.event.name = "ChangeConf" /\ "changes" \in DOMAIN x.event.prop.cc
         THEN { x.event.prop.cc.changes[k].nid : k \in 1..Len(x.event.prop.cc.changes) }
         ELSE {}) \cup
        (IF x.event.name = "ApplyConfChange" /\ "newconf" \in DOMAIN x.event.prop.cc THEN ToSet(x.event.prop.cc.newconf) ELSE {}),
    {}, TraceLog))

AllChangeConfNids == TLCEval(FoldSeq(
    LAMBDA x, y: y \cup
        IF x.event.name = "ChangeConf" /\ "changes" \in DOMAIN x.event.prop.cc
        THEN { x.event.prop.cc.changes[k].nid : k \in 1..Len(x.event.prop.cc.changes) }
        ELSE {},
    {}, TraceLog))

BootstrapLogIndicesForServer(i) ==
    LET
        FirstBootstrapLogIndex == SelectInSeq(TraceLog, LAMBDA x: x.event.nid = i /\ x.event.name \in {"InitState", "BecomeFollower", "ApplyConfChange"})
        FirstNonBootstrapLogIndex == SelectInSeq(TraceLog, LAMBDA x: x.event.nid = i /\ x.event.name \notin {"InitState", "BecomeFollower", "ApplyConfChange"})
        LastBootstrapLogIndexUpperBound == IF FirstNonBootstrapLogIndex = 0 THEN Len(TraceLog) ELSE FirstNonBootstrapLogIndex-1
    IN
        IF FirstBootstrapLogIndex = 0 THEN {}
        ELSE
        { k \in FirstBootstrapLogIndex..LastBootstrapLogIndexUpperBound: TraceLog[k].event.nid = i }

BootstrapLogIndices == UNION { BootstrapLogIndicesForServer(i): i \in Server }

LastBootstrapLog == [ i \in Server |-> IF BootstrapLogIndicesForServer(i) = {} THEN TraceLog[1] ELSE TraceLog[Max(BootstrapLogIndicesForServer(i))] ]

BootstrappedConfig(i) ==
    IF LastBootstrapLog[i].event.name = "ApplyConfChange" THEN
        ToSet(LastBootstrapLog[i].event.prop.cc.newconf)
    ELSE
        ToSet(LastBootstrapLog[i].event.conf[1])

TraceInitServer == BootstrappedConfig(TraceLog[1].event.nid)
ASSUME TraceInitServer \subseteq TraceServer

\* Extract learners from bootstrap events (InitState, BecomeFollower, BecomeCandidate)
TraceLearners == TLCEval(
    LET bootstrapEvents == SelectSeq(TraceLog, LAMBDA x:
            x.event.name \in {"InitState", "BecomeFollower", "BecomeCandidate"} /\
            "learners" \in DOMAIN x.event /\ x.event.learners /= <<>>)
    IN IF Len(bootstrapEvents) > 0
       THEN ToSet(bootstrapEvents[1].event.learners)
       ELSE {})

\* Use bootstrap learners if available, otherwise infer from TraceServer
ImplicitLearners ==
    IF TraceLearners /= {}
    THEN TraceLearners
    ELSE TraceServer \ (TraceInitServer \cup AllChangeConfNids)

\* Extract MaxInflightMsgs from trace config (default: 256)
TraceMaxInflightMsgs == TLCEval(
    LET configLines == SelectSeq(ndJsonDeserialize(JsonFile), LAMBDA x: "tag" \in DOMAIN x /\ x.tag = "config")
    IN IF Len(configLines) > 0 /\ "config" \in DOMAIN configLines[1] /\ "MaxInflightMsgs" \in DOMAIN configLines[1].config
       THEN configLines[1].config.MaxInflightMsgs
       ELSE 256
)

\* Extract DisableConfChangeValidation from trace config (default: FALSE)
TraceDisableConfChangeValidation == TLCEval(
    LET configLines == SelectSeq(ndJsonDeserialize(JsonFile), LAMBDA x: "tag" \in DOMAIN x /\ x.tag = "config")
    IN IF Len(configLines) > 0 /\ "config" \in DOMAIN configLines[1] /\ "DisableConfChangeValidation" \in DOMAIN configLines[1].config
       THEN configLines[1].config.DisableConfChangeValidation
       ELSE FALSE
)

TraceInitServerVars == 
    /\ currentTerm = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN 0 ELSE LastBootstrapLog[i].event.state.term]
    /\ state = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN Follower ELSE LastBootstrapLog[i].event.role]
    /\ votedFor = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN "0" ELSE LastBootstrapLog[i].event.state.vote]

TraceInitLogVars    ==
    /\ log          = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={}
                       THEN [offset |-> 1, entries |-> <<>>, snapshotIndex |-> 0, snapshotTerm |-> 0]
                       ELSE [offset |-> 1,
                             entries |-> [j \in 1..LastBootstrapLog[i].event.log |-> [ term |-> 1, type |-> "ConfigEntry", value |-> [newconf |-> BootstrappedConfig(i), learners |-> ImplicitLearners]]],
                             snapshotIndex |-> LastBootstrapLog[i].event.state.snapshotIndex,
                             snapshotTerm |-> LastBootstrapLog[i].event.state.snapshotTerm
                            ]
                      ]
    /\ historyLog   = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN <<>> ELSE [j \in 1..LastBootstrapLog[i].event.log |-> [ term |-> 1, type |-> "ConfigEntry", value |-> [newconf |-> BootstrappedConfig(i), learners |-> ImplicitLearners]]]]
    /\ commitIndex  = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN 0 ELSE LastBootstrapLog[i].event.state.commit]
    /\ applied      = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN 0 ELSE LastBootstrapLog[i].event.state.applied]

TraceInitConfigVars ==
    /\ config = [i \in Server |-> [ jointConfig |-> <<BootstrappedConfig(i), {}>>, learners |-> ImplicitLearners, autoLeave |-> FALSE] ]
    /\ reconfigCount = 0
    \* Bootstrap config entries are already applied (committed at initial commitIndex)
    /\ appliedConfigIndex = [i \in Server |-> IF BootstrapLogIndicesForServer(i)={} THEN 0 ELSE LastBootstrapLog[i].event.state.commit]

TraceInitLeaderVars ==
    /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
    \* Initialize pendingConfChangeIndex from trace if available
    /\ pendingConfChangeIndex = [i \in Server |->
        IF BootstrapLogIndicesForServer(i) = {}
        THEN 0
        ELSE IF "pendingConfIndex" \in DOMAIN LastBootstrapLog[i].event.state
             THEN LastBootstrapLog[i].event.state.pendingConfIndex
             ELSE 0]

-------------------------------------------------------------------------------------
ConfFromLog(l) == << ToSet(l.event.conf[1]), ToSet(l.event.conf[2]) >>

OneMoreMessage(msg) ==
    \/ msg \notin DOMAIN pendingMessages /\ msg \in DOMAIN pendingMessages' /\ pendingMessages'[msg] = 1
    \/ msg \in DOMAIN pendingMessages /\ pendingMessages'[msg] = pendingMessages[msg] + 1

OneLessMessage(msg) ==
    \/ msg \in DOMAIN messages /\ messages[msg] = 1 /\ msg \notin DOMAIN messages'
    \/ msg \in DOMAIN messages /\ messages'[msg] = messages[msg] - 1

-------------------------------------------------------------------------------------

VARIABLE l
logline == TraceLog[l]
VARIABLE pl


TraceInit ==
    /\ l = 1
    /\ pl = 0
    /\ logline = TraceLog[l]
    /\ Init

StepToNextTrace ==
    /\ l' = l+1
    /\ pl' = l
    /\ l % Max({1, Len(TraceLog) \div 100}) = 0 => PrintT(<< "Progress %:", (l * 100) \div Len(TraceLog)>>)
    /\ l' > Len(TraceLog) => PrintT(<< "Progress %:", 100>>)

StepToNextTraceIfMessageIsProcessed(msg) ==
    IF OneLessMessage(msg)
        THEN StepToNextTrace
        ELSE
            /\ pl' = l
            /\ UNCHANGED <<l>>

-------------------------------------------------------------------------------------

LoglineIsEvent(e) ==
    /\ l <= Len(TraceLog)
    /\ logline.event.name = e

LoglineIsEvents(e) ==
    /\ l <= Len(TraceLog)
    /\ logline.event.name \in e

LoglineIsMessageEvent(e, i, j) ==
    /\ LoglineIsEvent(e)
    /\ logline.event.msg.from = i
    /\ logline.event.msg.to = j

LoglineIsNodeEvent(e, i) ==
    /\ LoglineIsEvent(e)
    /\ logline.event.nid = i

LoglineIsAppendEntriesRequest(m) ==
    /\ "msg" \in DOMAIN logline.event
    /\ m.mtype = AppendEntriesRequest
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.msubtype = RaftMsgSubtype[logline.event.msg.type]
    /\ m.mdest   = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.msubtype /= "snapshot" => m.mcommitIndex = logline.event.msg.commit
    /\ m.msubtype /= "heartbeat" => /\ m.mprevLogTerm = logline.event.msg.logTerm
                                   /\ m.mprevLogIndex = logline.event.msg.index
    /\ Len(m.mentries) = logline.event.msg.entries

LoglineIsSnapshotRequest(m) ==
    /\ "msg" \in DOMAIN logline.event
    /\ m.mtype = SnapshotRequest
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.mdest   = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.msnapshotIndex = logline.event.msg.index
    /\ m.msnapshotTerm = logline.event.msg.logTerm

LoglineIsAppendEntriesResponse(m) ==
    /\ "msg" \in DOMAIN logline.event
    /\ m.mtype = AppendEntriesResponse
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ \/ m.msubtype = RaftMsgSubtype[logline.event.msg.type]
       \/ /\ logline.event.msg.type = "MsgAppResp"
          /\ m.msubtype = "snapshot"
    /\ m.mdest   = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.msuccess = ~logline.event.msg.reject
    \* For success: mmatchIndex = index, mrejectHint = 0, mlogTerm = 0
    \* For reject: mmatchIndex = rejected index, mrejectHint/mlogTerm from trace
    /\ (~logline.event.msg.reject /\ m.msubtype /= "heartbeat") => m.mmatchIndex = logline.event.msg.index
    /\ logline.event.msg.reject => /\ m.mmatchIndex = logline.event.msg.index
                                   /\ m.mrejectHint = logline.event.msg.rejectHint
                                   /\ m.mlogTerm = logline.event.msg.logTerm

LoglineIsRequestVoteRequest(m) ==
    /\ "msg" \in DOMAIN logline.event
    /\ m.mtype = RequestVoteRequest
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.mdest = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.mlastLogIndex = logline.event.msg.index
    /\ m.mlastLogTerm = logline.event.msg.logTerm

LoglineIsRequestVoteResponse(m) ==
    /\ "msg" \in DOMAIN logline.event
    /\ m.mtype = RequestVoteResponse
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.mdest = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.mvoteGranted = ~logline.event.msg.reject

\* Helper to access log entry at specific logical index from a log record
LogEntryAt(xlog, index) ==
    xlog.entries[index - xlog.offset + 1]

ValidatePreStates(i) ==
    pl = l - 1 =>
        /\ currentTerm[i] = logline.event.state.term
        /\ state[i] = RaftRole[logline.event.role]
        /\ commitIndex[i] = logline.event.state.commit
        \* Note: applied not validated - updated asynchronously by application layer
        /\ votedFor[i] = logline.event.state.vote

ValidatePostStates(i) ==
    /\ currentTerm'[i] = logline.event.state.term
    /\ state'[i] = logline.event.role
    /\ votedFor'[i] = logline.event.state.vote
    /\ LastIndex(log'[i]) = logline.event.log
    /\ commitIndex'[i] = logline.event.state.commit
    /\ config'[i].jointConfig = ConfFromLog(logline)
    /\ log'[i].snapshotIndex = logline.event.state.snapshotIndex
    /\ log'[i].snapshotTerm = logline.event.state.snapshotTerm
    \* Validate applied if present in trace
    /\ "applied" \in DOMAIN logline.event.state =>
        applied'[i] = logline.event.state.applied
    \* Validate config.learners if present in trace
    /\ "learners" \in DOMAIN logline.event =>
        config'[i].learners = ToSet(logline.event.learners)

-------------------------------------------------------------------------------------
\* Progress-specific validation helpers

ValidateProgressState(i, j) ==
    \/ /\ "prop" \notin DOMAIN logline.event
       /\ TRUE
    \/ /\ "prop" \in DOMAIN logline.event
       /\ "state" \in DOMAIN logline.event.prop
       /\ progressState[i][j] = logline.event.prop.state
       /\ "match" \in DOMAIN logline.event.prop =>
           matchIndex[i][j] = logline.event.prop.match
       /\ "next" \in DOMAIN logline.event.prop =>
           nextIndex[i][j] = logline.event.prop.next
       /\ "paused" \in DOMAIN logline.event.prop =>
           msgAppFlowPaused[i][j] = logline.event.prop.paused
       /\ "inflights_count" \in DOMAIN logline.event.prop =>
           Cardinality(inflights[i][j]) = logline.event.prop.inflights_count
       /\ (progressState[i][j] = StateSnapshot /\ "pending_snapshot" \in DOMAIN logline.event.prop) =>
           pendingSnapshot[i][j] = logline.event.prop.pending_snapshot

-------------------------------------------------------------------------------------

ValidateAfterRequestVote(i, j) ==
    /\ ValidatePostStates(i)
    /\ \E m \in DOMAIN pendingMessages':
       /\ \/ LoglineIsRequestVoteRequest(m)
          \/ /\ LoglineIsRequestVoteResponse(m)
             /\ m.msource = m.mdest
       /\ OneMoreMessage(m)

RequestVoteIfLogged(i, j) ==
    /\ \/ LoglineIsMessageEvent("SendRequestVoteRequest", i, j)
       \/ /\ LoglineIsMessageEvent("SendRequestVoteResponse", i, j)
          /\ i = j
    /\ RequestVote(i, j)
    /\ ValidateAfterRequestVote(i, j)

ValidateAfterBecomeLeader(i) ==
    /\ ValidatePostStates(i)
    /\ logline.event.role = "StateLeader"
    /\ state'[i] = Leader
    /\ \A j \in Server: j /= i => progressState'[i][j] = StateProbe

BecomeLeaderIfLogged(i) ==
    /\ LoglineIsNodeEvent("BecomeLeader", i)
    /\ BecomeLeader(i)
    /\ ValidateAfterBecomeLeader(i)

ClientRequestIfLogged(i, v) ==
    /\ LoglineIsNodeEvent("Replicate", i)
    /\ ClientRequest(i, v)

ValidateAfterAdvanceCommitIndex(i) ==
    /\ ValidatePostStates(i)
    /\ logline.event.role = "StateLeader"
    /\ state[i] = Leader

AdvanceCommitIndexIfLogged(i) ==
    /\ LoglineIsNodeEvent("Commit", i)
    /\ state[i] = Leader  \* All Commit events in trace are from Leader
    /\ AdvanceCommitIndex(i)
    /\ ValidateAfterAdvanceCommitIndex(i)

ValidateProgressStatePrimed(i, j) ==
    \/ /\ "prop" \notin DOMAIN logline.event
       /\ TRUE
    \/ /\ "prop" \in DOMAIN logline.event
       /\ "state" \in DOMAIN logline.event.prop
       /\ progressState'[i][j] = logline.event.prop.state
       /\ "match" \in DOMAIN logline.event.prop =>
           matchIndex'[i][j] = logline.event.prop.match
       /\ "next" \in DOMAIN logline.event.prop =>
           nextIndex'[i][j] = logline.event.prop.next
       /\ "paused" \in DOMAIN logline.event.prop =>
           msgAppFlowPaused'[i][j] = logline.event.prop.paused
       /\ "inflights_count" \in DOMAIN logline.event.prop =>
           Cardinality(inflights'[i][j]) = logline.event.prop.inflights_count
       /\ (progressState'[i][j] = StateSnapshot /\ "pending_snapshot" \in DOMAIN logline.event.prop) =>
           pendingSnapshot'[i][j] = logline.event.prop.pending_snapshot

ValidateAfterAppendEntries(i, j) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsAppendEntriesRequest(msg)
        /\ OneMoreMessage(msg)
        /\ ValidateProgressState(i, j)

ValidateAfterHeartbeat(i, j) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsAppendEntriesRequest(msg)
        /\ OneMoreMessage(msg)
        /\ ValidateProgressState(i, j)

ValidateAfterAppendEntriesToSelf(i) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsAppendEntriesResponse(msg)
        /\ msg.msource = msg.mdest
        /\ OneMoreMessage(msg)

ValidateAfterSnapshot(i, j) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsSnapshotRequest(msg)
        /\ OneMoreMessage(msg)
        /\ ValidateProgressStatePrimed(i, j)

AppendEntriesIfLogged(i, j, range) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j)
    /\ logline.event.msg.type = "MsgApp"
    /\ range[1] = logline.event.msg.index + 1
    /\ range[2] = range[1] + logline.event.msg.entries
    /\ AppendEntries(i, j, range)
    /\ ValidateAfterAppendEntries(i, j)

HeartbeatIfLogged(i, j) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j)
    /\ logline.event.msg.type = "MsgHeartbeat"
    /\ Heartbeat(i, j)
    /\ ValidateAfterAppendEntries(i, j)

\* SendSnapshot - uses SendSnapshotWithCompaction
SendSnapshotIfLogged(i, j, index) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j)
    /\ logline.event.msg.type = "MsgSnap"
    /\ index = logline.event.msg.index
    /\ SendSnapshotWithCompaction(i, j, index)  \* Call action from etcdraft.tla
    /\ ValidateAfterSnapshot(i, j)
    /\ progressState'[i][j] = StateSnapshot

\* ManualSendSnapshot bypasses normal raft send path, does NOT modify progress state
ManualSendSnapshotIfLogged(i, j) ==
    /\ LoglineIsMessageEvent("ManualSendSnapshot", i, j)
    /\ logline.event.msg.type = "MsgSnap"
    /\ ManualSendSnapshot(i, j)  \* Call action from etcdraft.tla


\* Implicit replication with self-directed MsgAppResp
ImplicitReplicateAndSend(i) ==
    ReplicateImplicitEntry(i)  \* Call action from etcdraft.tla

AppendEntriesToSelfIfLogged(i) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesResponse", i, i)
    /\ IF LastIndex(log[i]) < logline.event.log
       THEN ImplicitReplicateAndSend(i) /\ ValidateAfterAppendEntriesToSelf(i)
       ELSE AppendEntriesToSelf(i) /\ ValidateAfterAppendEntriesToSelf(i)

ReceiveMessageTraceNames == { "ReceiveAppendEntriesRequest", "ReceiveAppendEntriesResponse", "ReceiveRequestVoteRequest", "ReceiveRequestVoteResponse", "ReceiveSnapshot" }
\* perform Receive transition if logline indicates so
LoglineIsReceivedMessage(m) ==
    \/ /\ LoglineIsEvent("ReceiveAppendEntriesRequest")
       /\ \/ LoglineIsAppendEntriesRequest(m)
          \/ LoglineIsSnapshotRequest(m)
    \/ /\ LoglineIsEvent("ReceiveAppendEntriesResponse")
       /\ LoglineIsAppendEntriesResponse(m)
    \/ /\ LoglineIsEvent("ReceiveRequestVoteRequest")
       /\ LoglineIsRequestVoteRequest(m)
    \/ /\ LoglineIsEvent("ReceiveRequestVoteResponse")
       /\ LoglineIsRequestVoteResponse(m)
    \* ReceiveSnapshot is actually covered by ReceiveAppendEntriesRequest in state_trace.go,
    \* but if trace uses ReceiveSnapshot explicit tag, we handle it here.
    \/ /\ LoglineIsEvent("ReceiveSnapshot")
       /\ LoglineIsSnapshotRequest(m)

ReceiveIfLogged(m) ==
    /\ LoglineIsReceivedMessage(m)
    /\ ValidatePreStates(m.mdest)
    /\ Receive(m)

\* perform Timeout transition if logline indicates so
ValidateAfterTimeout(i) ==
    /\ logline.event.role = "StateCandidate"
    /\ logline.event.nid = i
    /\ state'[i] = Candidate
    /\ currentTerm'[i] = logline.event.state.term

TimeoutIfLogged(i) ==
    /\ LoglineIsNodeEvent("BecomeCandidate", i)
    /\ Timeout(i)
    /\ ValidateAfterTimeout(i)


ChangeConfIfLogged(i) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ ValidatePreStates(i)
    /\ LET changes == logline.event.prop.cc.changes
           initialConf == [voters |-> GetConfig(i), learners |-> GetLearners(i)]
           finalConf == FoldSeq(ApplyChange, initialConf, changes)
           enterJoint == logline.event.prop.enterJoint
       IN
           \* Use ChangeConf's IF-branch directly: BecomeLeader sets pendingConfChangeIndex = lastIndex,
           \* so ChangeConf(i) would take the ELSE branch, but trace proves a real config change happened.
           /\ state[i] = Leader
           /\ ~IsJointConfig(i)
           /\ Replicate(i, [newconf |-> finalConf.voters, learners |-> finalConf.learners,
                           enterJoint |-> enterJoint, oldconf |-> GetConfig(i)], ConfigEntry)
           /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = LastIndex(log'[i])]
           \* Validate the created entry matches trace expectations
           /\ LogEntryAt(log'[i], LastIndex(log'[i])).value.newconf = finalConf.voters
           /\ LogEntryAt(log'[i], LastIndex(log'[i])).value.learners = finalConf.learners
           /\ LogEntryAt(log'[i], LastIndex(log'[i])).value.enterJoint = enterJoint
           /\ LogEntryAt(log'[i], LastIndex(log'[i])).value.oldconf = GetConfig(i)
           /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

ApplySimpleConfChangeIfLogged(i) ==
    /\ LoglineIsNodeEvent("ApplyConfChange", i)
    /\ ~IsJointConfig(i)  \* If we're in joint, we should be using LeaveJointIfLogged instead
    \* Exclude LeaveJoint events (they have leaveJoint flag)
    /\ ~("leaveJoint" \in DOMAIN logline.event.prop.cc /\ logline.event.prop.cc.leaveJoint = TRUE)
    /\ ApplySimpleConfChange(i)

\* Leave joint consensus via ApplySimpleConfChange
LeaveJointIfLogged(i) ==
    /\ LoglineIsNodeEvent("ApplyConfChange", i)
    /\ "newconf" \in DOMAIN logline.event.prop.cc
    /\ \/ IsJointConfig(i)  \* We're currently in joint consensus
       \/ ("leaveJoint" \in DOMAIN logline.event.prop.cc /\ logline.event.prop.cc.leaveJoint = TRUE)
    /\ ApplySimpleConfChange(i)  \* Use ApplySimpleConfChange to update appliedConfigIndex

\* Apply configuration from snapshot
ApplySnapshotConfChangeIfLogged(i) ==
    /\ LoglineIsNodeEvent("ApplyConfChange", i)
    /\ "newconf" \in DOMAIN logline.event.prop.cc
    /\ log[i].offset > commitIndex[i]  \* Indicates snapshot was applied
    /\ LET newVoters == ToSet(logline.event.prop.cc.newconf)
       IN ApplySnapshotConfChange(i, newVoters)  \* Call action from etcdraft.tla

ReadyIfLogged(i) ==
    /\ LoglineIsNodeEvent("Ready", i)
    /\ Ready(i)

RestartIfLogged(i) ==
    /\ LoglineIsNodeEvent("InitState", i)
    /\ Restart(i)
    /\ ValidatePostStates(i)

LoglineIsBecomeFollowerInUpdateTermOrReturnToFollower ==
    /\ LoglineIsEvent("BecomeFollower")
    /\ LET
            k == SelectLastInSubSeq(TraceLog, 1, l-1, LAMBDA x: x.event.nid = logline.event.nid)
       IN
            /\ k > 0
            /\ \/ /\ TraceLog[k].event.name \in ReceiveMessageTraceNames
                  /\ TraceLog[k].event.state.term < TraceLog[k].event.msg.term
                  /\ TraceLog[k].event.msg.term = logline.event.state.term
               \/ /\ TraceLog[k].event.name = "ReceiveAppendEntriesRequest"
                  /\ TraceLog[k].event.state.term = TraceLog[k].event.msg.term
                  /\ TraceLog[k].event.msg.term = logline.event.state.term
                  /\ TraceLog[k].event.role = Candidate

StepDownToFollowerIfLogged(i) ==
    /\ LoglineIsNodeEvent("BecomeFollower", i)
    /\ \lnot LoglineIsBecomeFollowerInUpdateTermOrReturnToFollower
    /\ StepDownToFollower(i)
    /\ ValidatePostStates(i)

\* Handle ReportUnreachable event
ReportUnreachableIfLogged(i, j) ==
    /\ LoglineIsNodeEvent("ReportUnreachable", i)
    /\ "prop" \in DOMAIN logline.event
    /\ "target" \in DOMAIN logline.event.prop
    /\ j = logline.event.prop.target
    /\ ReportUnreachable(i, j)
    \* Use Primed version since trace records state AFTER the transition
    /\ ValidateProgressStatePrimed(i, j)

\* Handle ReportSnapshotStatus event
ReportSnapshotStatusIfLogged(i, j) ==
    /\ LoglineIsNodeEvent("ReportSnapshotStatus", i)
    /\ "prop" \in DOMAIN logline.event
    /\ "target" \in DOMAIN logline.event.prop
    /\ "success" \in DOMAIN logline.event.prop
    /\ j = logline.event.prop.target
    /\ ReportSnapshotStatus(i, j, logline.event.prop.success)
    \* Use Primed version since trace records state AFTER the transition
    /\ ValidateProgressStatePrimed(i, j)

\* skip unused logs
SkipUnusedLogline ==
    /\ \/ /\ LoglineIsEvent("SendRequestVoteResponse")
          /\ logline.event.msg.from # logline.event.msg.to
       \/ /\ LoglineIsEvent("SendAppendEntriesResponse")
          /\ logline.event.msg.from # logline.event.msg.to  \* Non-self response already handled by HandleAppendEntriesRequest
       \/ LoglineIsBecomeFollowerInUpdateTermOrReturnToFollower
       \/ LoglineIsEvent("ReduceNextIndex") \* shall not be necessary when this is removed from raft
    /\ UNCHANGED <<vars>>
    /\ StepToNextTrace

\* Handle ApplyEntries event
ApplyEntriesIfLogged(i) ==
    /\ LoglineIsNodeEvent("ApplyEntries", i)
    /\ "prop" \in DOMAIN logline.event
    /\ "applied" \in DOMAIN logline.event.prop
    /\ LET newApplied == logline.event.prop.applied
       IN ApplyEntries(i, newApplied)
    /\ ValidatePostStates(i)

CompactLogIfLogged(i) ==
    /\ LoglineIsNodeEvent("CompactLog", i)
    /\ LET newStart == logline.event.prop.compactIndex + 1  \* compactIndex is snapshotIndex, newStart = snapshotIndex + 1
       IN CompactLog(i, newStart)
    /\ ValidatePostStates(i)

TraceNextNonReceiveActions ==
    /\ \/ /\ LoglineIsEvents({"SendRequestVoteRequest", "SendRequestVoteResponse"})
          /\ \E i,j \in Server : RequestVoteIfLogged(i, j)
       \/ /\ LoglineIsEvent("BecomeLeader")
          /\ \E i \in Server : BecomeLeaderIfLogged(i)
       \/ /\ LoglineIsEvent("Replicate")
          /\ \E i \in Server : ClientRequestIfLogged(i, 0)
       \/ /\ LoglineIsEvent("Commit")
          /\ \E i \in Server : AdvanceCommitIndexIfLogged(i)
       \/ /\ LoglineIsEvent("SendAppendEntriesRequest") /\ logline.event.msg.type = "MsgApp"
          /\ \E i,j \in Server : \E b,e \in matchIndex[i][j]+1..LastIndex(log[i])+1 : AppendEntriesIfLogged(i, j, <<b,e>>)
       \/ /\ LoglineIsEvent("SendAppendEntriesResponse")
          /\ \E i \in Server : AppendEntriesToSelfIfLogged(i)
       \/ /\ LoglineIsEvent("SendAppendEntriesRequest")
          /\ \E i,j \in Server : HeartbeatIfLogged(i, j) /\ logline.event.msg.type = "MsgHeartbeat"
       \/ /\ LoglineIsEvent("SendAppendEntriesRequest") /\ logline.event.msg.type = "MsgSnap"
          \* Pass msg.index as snapshot index
          /\ \E i,j \in Server : SendSnapshotIfLogged(i, j, logline.event.msg.index)
       \/ /\ LoglineIsEvent("ManualSendSnapshot")
          /\ \E i,j \in Server : ManualSendSnapshotIfLogged(i, j)
       \/ /\ LoglineIsEvent("BecomeCandidate")
          /\ \E i \in Server : TimeoutIfLogged(i)
       \/ /\ LoglineIsEvent("ChangeConf")
          /\ \E i \in Server: ChangeConfIfLogged(i)
       \/ /\ LoglineIsEvent("ApplyConfChange")
          /\ \E i \in Server: \/ ApplySimpleConfChangeIfLogged(i)
                              \/ ApplySnapshotConfChangeIfLogged(i)
                              \/ LeaveJointIfLogged(i)
       \/ /\ LoglineIsEvent("Ready")
          /\ \E i \in Server: ReadyIfLogged(i)
       \/ /\ LoglineIsEvent("InitState")
          /\ \E i \in Server: RestartIfLogged(i)
       \/ /\ LoglineIsEvent("BecomeFollower")
          /\ \E i \in Server: StepDownToFollowerIfLogged(i)
       \/ /\ LoglineIsEvent("ReportUnreachable")
          /\ \E i,j \in Server: ReportUnreachableIfLogged(i, j)
       \/ /\ LoglineIsEvent("ReportSnapshotStatus")
          /\ \E i,j \in Server: ReportSnapshotStatusIfLogged(i, j)
       \/ /\ LoglineIsEvent("ApplyEntries")
          /\ \E i \in Server: ApplyEntriesIfLogged(i)
       \/ /\ LoglineIsEvent("CompactLog")
          /\ \E i \in Server: CompactLogIfLogged(i)
       \/ SkipUnusedLogline
    /\ StepToNextTrace

TraceNextReceiveActions ==
    /\ LoglineIsEvents(ReceiveMessageTraceNames)
    /\ \E m \in DOMAIN messages :
        /\ ReceiveIfLogged(m)
        /\ StepToNextTraceIfMessageIsProcessed(m)

TraceNext ==
    \/ /\ l \in BootstrapLogIndices
       /\ UNCHANGED <<vars>>
       /\ StepToNextTrace
    \/ /\ l \notin BootstrapLogIndices
       /\ \/ TraceNextNonReceiveActions
          \/ TraceNextReceiveActions

TraceSpec ==
    TraceInit /\ [][TraceNext]_<<l, pl, vars>>

-------------------------------------------------------------------------------------

TraceView ==
    <<vars, l>>

-------------------------------------------------------------------------------------

\* The property TraceMatched below will be violated if TLC runs with more than a single worker.
ASSUME TLCGet("config").worker = 1

TraceMatched ==
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

etcd == INSTANCE etcdraft
etcdSpec == etcd!Init /\ [][etcd!NextDynamic]_etcd!vars

==================================================================================
