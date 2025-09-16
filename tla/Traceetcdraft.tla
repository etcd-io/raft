-------------------------------- MODULE Traceetcdraft -------------------------------
\* Copyright 2024 The etcd Authors
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.
\*

EXTENDS etcdraft_control, Json, IOUtils, Sequences, TLC

\* raft.pb.go enum MessageType
RaftMsgType ==
    "MsgApp" :> AppendEntriesRequest @@ "MsgAppResp" :> AppendEntriesResponse @@
    "MsgVote" :> RequestVoteRequest @@ "MsgVoteResp" :> RequestVoteResponse @@
    "MsgHeartbeat" :> AppendEntriesRequest @@ "MsgHeartbeatResp" :> AppendEntriesResponse @@
    "MsgSnap" :> AppendEntriesRequest

RaftMsgSubtype ==
    "MsgHeartbeat" :> "heartbeat" @@ "MsgHeartbeatResp" :> "heartbeat" @@
    "MsgApp" :> "app" @@ "MsgAppResp" :> "app" @@
    "MsgSnap" :> "snapshot"

-------------------------------------------------------------------------------------

\* Trace validation has been designed for TLC running in default model-checking
 \* mode, i.e., breadth-first search.
ASSUME TLCGet("config").mode = "bfs"

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./example.ndjson"

OriginTraceLog ==
    \* Deserialize the System log as a sequence of records from the log file.
    \* Run TLC with (assuming a suitable "tlc" shell alias):
    \* $ JSON=../tests/raft_scenarios/4582.ndjson tlc -note Traceccfraft
    \* Fall back to trace.ndjson if the JSON environment variable is not set.
    SelectSeq(ndJsonDeserialize(JsonFile), LAMBDA l: "tag" \in DOMAIN l /\ l.tag = "trace")

TraceLog ==
    TLCEval(IF "MAX_TRACE" \in DOMAIN IOEnv THEN SubSeq(OriginTraceLog, 1, Min({Len(OriginTraceLog), atoi(IOEnv.MAX_TRACE)})) ELSE OriginTraceLog)

TraceServer == TLCEval(FoldSeq(
    LAMBDA x, y: y \cup IF  /\ x.event.name = "ChangeConf" 
                            /\ "changes" \in DOMAIN x.event.prop.cc
                            /\ x.event.prop.cc.changes[1].action \in {"AddNewServer", "AddLearner"}
                            THEN {x.event.nid, x.event.prop.cc.changes[1].nid} 
                            ELSE {x.event.nid},
    {}, TraceLog))

BootstrapIndex == [ i \in Server |-> SelectInSeq(TraceLog, LAMBDA  x: x.event.nid = i /\ x.event.name = "Bootstrap") ]
TraceInitServer == { i \in Server : BootstrapIndex[i] > 0 }
ASSUME TraceInitServer \subseteq TraceServer   
BootstrapEntries(i) == FoldSeq(
    LAMBDA x, y: Append(y, [ term |-> 1, 
                             type |-> "ConfigEntry", 
                             value |-> [ newconf |-> ToSet(TraceLog[x].event.prop.cc.newconf), learners |-> {}] ]),
    <<>>, 
    SetToSortSeq({j \in DOMAIN TraceLog : /\ j < BootstrapIndex[i] 
                                          /\ TraceLog[j].event.nid = i 
                                          /\ TraceLog[j].event.name = "ApplyConfChange"}, <) ) 

TraceInitServerVars == /\ currentTerm = [i \in Server |-> IF i \in InitServer THEN 1 ELSE 0]
                       /\ state = [i \in Server |-> Follower]
                       /\ votedFor = [i \in Server |-> Nil]
TraceInitLogVars    == /\ log          = [i \in Server |-> BootstrapEntries(i)]
                       /\ commitIndex  = [i \in Server |-> Len(log[i])]
                       /\ applied      = [i \in Server |-> 0]
TraceInitConfigVars == 
    /\ config = [i \in Server |-> [ jointConfig |-> <<IF i \in InitServer THEN ToSet(TraceLog[BootstrapIndex[i]].event.conf[1]) ELSE {}, {}>>, learners |-> {}] ]
    /\ appliedConfChange = [i \in Server |-> Len(log[i])]
                        

-------------------------------------------------------------------------------------
ConfFromTrace(l) == << ToSet(l.event.conf[1]), ToSet(l.event.conf[2]) >>
NewConfFromTrace(l) == << ToSet(l.event.prop.cc.newconf), {} >>

OneMoreMessage(msg) ==
    \/ msg \notin DOMAIN pendingMessages /\ msg \in DOMAIN pendingMessages' /\ pendingMessages'[msg] = 1
    \/ msg \in DOMAIN pendingMessages /\ pendingMessages'[msg] = pendingMessages[msg] + 1

OneLessMessage(msg) ==
    \/ msg \in DOMAIN messages /\ messages[msg] = 1 /\ msg \notin DOMAIN messages'
    \/ msg \in DOMAIN messages /\ messages'[msg] = messages[msg] - 1

-------------------------------------------------------------------------------------

\* In some state, we will restrict the state exploration to only follow the actions in
\* constrainedBehavior. This is to reduce state space to be explored.
VARIABLE constrainedBehavior

Ready_PersistState_SendMessages_Behavior(i) == 
    << 
        << "Ready",         <<i>> >>, 
        << "PersistState",  <<i>> >>, 
        << "SendMessages",  <<i>> >> 
    >>

VARIABLE l

mcVars == <<vars, l, constrainedBehavior>>

logline == TraceLog[l]

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
    /\ m.mtype = AppendEntriesRequest
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.msubtype = RaftMsgSubtype[logline.event.msg.type]
    /\ m.mdest   = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    \* MsgSnap is equivalent to MsgApp except that it does not
    \* have commit index. Snapshot message contains leader log prefix
    \* up to a committed entry. That means the receiver can safely advance
    \* its commit index at least to the last log entry in snapshot message.
    \* Setting commit index in the MsgSnap message would become unnecessary.
    \* So we can safely ignore checking this against the model.
    /\ m.msubtype /= "snapshot" => m.mcommitIndex = logline.event.msg.commit
    /\ m.msubtype /= "heartbeat" => /\ m.mprevLogTerm = logline.event.msg.logTerm
                                   /\ m.mprevLogIndex = logline.event.msg.index
    /\ Len(m.mentries) = logline.event.msg.entries

LoglineIsAppendEntriesResponse(m) ==
    /\ m.mtype = AppendEntriesResponse
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.msubtype = RaftMsgSubtype[logline.event.msg.type]
    /\ m.mdest   = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.msuccess = ~logline.event.msg.reject
    /\ (\lnot logline.event.msg.reject /\ m.msubtype /= "heartbeat") => m.mmatchIndex = logline.event.msg.index

LoglineIsRequestVoteRequest(m) ==  
    /\ m.mtype = RequestVoteRequest
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.mdest = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.mlastLogIndex = logline.event.msg.index
    /\ m.mlastLogTerm = logline.event.msg.logTerm

LoglineIsRequestVoteResponse(m) ==  
    /\ m.mtype = RequestVoteResponse
    /\ m.mtype = RaftMsgType[logline.event.msg.type]
    /\ m.mdest = logline.event.msg.to
    /\ m.msource = logline.event.msg.from
    /\ m.mterm = logline.event.msg.term
    /\ m.mvoteGranted = ~logline.event.msg.reject

ReceiveMessageTraceNames == { "ReceiveAppendEntriesRequest", "ReceiveAppendEntriesResponse", "ReceiveRequestVoteRequest", "ReceiveRequestVoteResponse", "ReceiveSnapshot" }
LoglineIsReceivedMessage(m) ==
    \/ /\ LoglineIsEvent("ReceiveAppendEntriesRequest")
       /\ LoglineIsAppendEntriesRequest(m)
    \/ /\ LoglineIsEvent("ReceiveAppendEntriesResponse")
       /\ LoglineIsAppendEntriesResponse(m)
    \/ /\ LoglineIsEvent("ReceiveRequestVoteRequest")
       /\ LoglineIsRequestVoteRequest(m) 
    \/ /\ LoglineIsEvent("ReceiveRequestVoteResponse")
       /\ LoglineIsRequestVoteResponse(m)
    \/ /\ LoglineIsEvent("ReceiveSnapshot")
       /\ LoglineIsAppendEntriesRequest(m)

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

TraceIsRequestVote(i, j) ==
    \/ LoglineIsMessageEvent("SendRequestVoteRequest", i, j)
    \* etcd candidate sends MsgVoteResp to itself upon compain starting
    \/ /\ LoglineIsMessageEvent("SendRequestVoteResponse", i, j)
       /\ i = j 
TraceIsBecomeLeader(i) == LoglineIsNodeEvent("BecomeLeader", i)
TraceIsClientRequest(i, v) == LoglineIsNodeEvent("Replicate", i)
TraceIsAdvanceCommitIndex(i) == LoglineIsNodeEvent("Commit", i)
TraceIsAppendEntries(i, j, range) == 
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j) 
    /\ logline.event.msg.type = "MsgApp"
    /\ range[1] = logline.event.msg.index + 1
    /\ range[2] = range[1] + logline.event.msg.entries
TraceIsHeartbeat(i, j) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j)
    /\ logline.event.msg.type = "MsgHeartbeat"
TraceIsSendSnapshot(i, j, index) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j)
    /\ logline.event.msg.type = "MsgSnap"
    /\ index = logline.event.msg.entries
TraceIsAppendEntriesToSelf(i) == LoglineIsMessageEvent("SendAppendEntriesResponse", i, i)
TraceIsTimeout(i) == LoglineIsNodeEvent("BecomeCandidate", i)
TraceIsAddNewServer(i, j) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ Len(logline.event.prop.cc.changes) = 1
    /\ logline.event.prop.cc.changes[1].action = "AddNewServer"
    /\ logline.event.prop.cc.changes[1].nid = j
TraceIsAddLearner(i, j) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ Len(logline.event.prop.cc.changes) = 1
    /\ logline.event.prop.cc.changes[1].action = "AddLearner"
    /\ logline.event.prop.cc.changes[1].nid = j
TraceIsDeleteServer(i, j) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ Len(logline.event.prop.cc.changes) = 1
    /\ logline.event.prop.cc.changes[1].action = "RemoveServer"
    /\ logline.event.prop.cc.changes[1].nid = j
TraceIsApplyConfChange(i) == LoglineIsNodeEvent("ApplyConfChange", i)
TraceIsRestart(i) == LoglineIsNodeEvent("InitState", i)
TraceIsStepDownToFollower(i) == 
    /\ LoglineIsNodeEvent("BecomeFollower", i)
    /\ \lnot LoglineIsBecomeFollowerInUpdateTermOrReturnToFollower
TraceIsReceive(m) == LoglineIsReceivedMessage(m)

\* We don't trace for PersistState and SendMessages. Though they can be in any place
\* in ready phase, we can eliminate the nondeterministic by forcing the behavior sequence 
\* as Ready->PersistState->SendMessages->...->Advance. Note that the sequence
\* is not necessary same as the implementation because we always end up with same state after 
\* Advance no matter when PersistState and SendMessages are performed. 
\*
\* For each Ready trace, we will walk 3 steps in the state machine: 
\* Ready -> PersistState -> SendMessages
\* Trace location variable l will be unchanged until SendMessages action is done.
TraceIsReady(i) == LoglineIsNodeEvent("Ready", i)
TraceIsAdvance(i) == LoglineIsNodeEvent("Advance", i)
TraceIsUnused ==
    \/ /\ l < Len(TraceLog) 
       /\ l <= BootstrapIndex[logline.event.nid] 
    \/ /\ LoglineIsEvent("SendAppendEntriesResponse")
       /\ logline.event.msg.from # logline.event.msg.to
    \/ /\ LoglineIsEvent("SendRequestVoteResponse")
       /\ logline.event.msg.from # logline.event.msg.to
    \/ LoglineIsBecomeFollowerInUpdateTermOrReturnToFollower
    \/ LoglineIsEvent("ReduceNextIndex") \* shall not be necessary when this is removed from raft

TraceIsEnabled(action, args) ==
    IF TraceIsUnused THEN 
        action = "Unused"
    ELSE IF constrainedBehavior /= <<>> THEN
        <<action, args>> = constrainedBehavior[1]
    ELSE
       CASE action = "RequestVote"          -> TraceIsRequestVote(args[1], args[2])
         [] action = "BecomeLeader"         -> TraceIsBecomeLeader(args[1])
         [] action = "ClientRequest"        -> TraceIsClientRequest(args[1], args[2])
         [] action = "AdvanceCommitIndex"   -> TraceIsAdvanceCommitIndex(args[1])
         [] action = "AppendEntries"        -> TraceIsAppendEntries(args[1], args[2], args[3])
         [] action = "Heartbeat"            -> TraceIsHeartbeat(args[1], args[2])
         [] action = "SendSnapshot"         -> TraceIsSendSnapshot(args[1], args[2], args[3])
         [] action = "AppendEntriesToSelf"  -> TraceIsAppendEntriesToSelf(args[1])
         [] action = "Timeout"              -> TraceIsTimeout(args[1])
         [] action = "AddNewServer"         -> TraceIsAddNewServer(args[1], args[2])
         [] action = "AddLearner"           -> TraceIsAddLearner(args[1], args[2])
         [] action = "DeleteServer"         -> TraceIsDeleteServer(args[1], args[2])
         [] action = "ApplyConfChange"      -> TraceIsApplyConfChange(args[1])
         [] action = "Ready"                -> TraceIsReady(args[1])
         [] action = "PersistState"         -> TRUE
         [] action = "SendMessages"         -> TRUE
         [] action = "Advance"              -> TraceIsAdvance(args[1])
         [] action = "Restart"              -> TraceIsRestart(args[1])
         [] action = "StepDownToFollower"   -> TraceIsStepDownToFollower(args[1])
         [] action = "Receive"              -> TraceIsReceive(args[1])
         [] action = "DuplicateMessage"     -> FALSE
         [] action = "DropMessage"          -> FALSE
         [] action = "Unused"               -> TRUE

SpecActionsToInspectState == {
    "RequestVote",
    "BecomeLeader",
    "AdvanceCommitIndex",
    "AppendEntries",
    "Heartbeat",
    "SendSnapshot",
    "AppendEntriesToSelf",
    "Timeout",
    "Restart",
    "StepDownToFollower",
    "Ready",
    "Advance",
    "ApplyConfChange"
}
ValidateTraceState(action, args) ==
    LET i == logline.event.nid
    IN  action \in SpecActionsToInspectState => 
            /\ currentTerm'[i] = logline.event.state.term
            /\ votedFor'[i] = logline.event.state.vote
            /\ commitIndex'[i] = logline.event.state.commit
            /\ state'[i] = logline.event.role    
            /\ Len(log'[i]) = logline.event.log
            /\ applied'[i] = logline.event.applied
            /\ IF action = "ApplyConfChange" THEN 
                    /\ config[i].jointConfig = ConfFromTrace(logline)
                    /\ config'[i].jointConfig = NewConfFromTrace(logline)
               ELSE 
                    config'[i].jointConfig = ConfFromTrace(logline)

NextConstrainedBehavior(action, args) ==
    IF constrainedBehavior /= <<>> /\ <<action, args>> = constrainedBehavior[1] THEN
        constrainedBehavior' = Tail(constrainedBehavior)
    ELSE IF constrainedBehavior = <<>> /\ action = "Ready" THEN 
        constrainedBehavior' = Ready_PersistState_SendMessages_Behavior(args[1])
    ELSE 
        UNCHANGED <<constrainedBehavior>>

AdvanceTrace == 
    /\ l' = l+1
    /\ l % (Len(TraceLog) \div 100) = 0 => PrintT(<< "Progress %:", (l * 100) \div Len(TraceLog) >>) 
    /\ l' > Len(TraceLog) => PrintT(<< "Progress %:", 100>>)

StepTrace(action, args) ==
    CASE action = "Receive" -> 
        IF OneLessMessage(args[1]) THEN 
            AdvanceTrace
        ELSE 
            UNCHANGED <<l>>
      \* Trace log shall keep logline unchanged in Ready and PersistState, and step
      \* to the next line after SendMessages action is done.
      [] action \in {"Ready", "PersistState"} -> UNCHANGED <<l>>
      [] OTHER -> AdvanceTrace

TracePostAction(action, args) ==
    /\ ValidateTraceState(action, args)
    /\ NextConstrainedBehavior(action, args)
    /\ StepTrace(action, args)

-------------------------------------------------

TraceInit ==
    /\ l = 1
    /\ constrainedBehavior = <<>>
    /\ Init

SkipUnusedAction == SpecAction("Unused", <<>>, LAMBDA act: 
    /\ l <= Len(TraceLog)
    /\ UNCHANGED <<vars>>
)

TraceNext ==
    \/ Controlled_Next
    \/ SkipUnusedAction      

TraceSpec == TraceInit /\ [][TraceNext]_mcVars

-------------------------------------------------------------------------------------

TraceView ==
    \* A high-level state  s  can appear multiple times in a system trace.  Including the
     \* current level in TLC's view ensures that TLC will not stop model checking when  s
     \* appears the second time in the trace.  Put differently,  TraceView  causes TLC to
     \* consider  s_i  and s_j  , where  i  and  j  are the positions of  s  in the trace,
     \* to be different states.
    mcVars

-------------------------------------------------------------------------------------

\* The property TraceMatched below will be violated if TLC runs with more than a single worker.
ASSUME TLCGet("config").worker = 1

TraceMatched ==
    \* We force TLC to check TraceMatched as a temporal property because TLC checks temporal
    \* properties after generating all successor states of the current state, unlike
    \* invariants that are checked after generating a successor state.
    \* If the queue is empty after generating all successors of the current state,
    \* and l is less than the length of the trace, then TLC failed to validate the trace.
    \*
    [](l <= Len(TraceLog) => [](TLCGet("queue") > 0 \/ l > Len(TraceLog)))

==================================================================================


