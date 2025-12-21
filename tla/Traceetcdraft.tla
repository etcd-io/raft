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

EXTENDS etcdraft, Json, IOUtils, Sequences, TLC

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
    TLCEval(IF "MAX_TRACE" \in DOMAIN IOEnv THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE)) ELSE OriginTraceLog)

TraceServer == TLCEval(FoldSeq(
    LAMBDA x, y: y \cup IF  /\ x.event.name = "ChangeConf" 
                            /\ "changes" \in DOMAIN x.event.prop.cc
                            /\ x.event.prop.cc.changes[1].action \in {"AddNewServer", "AddLearner"}
                            THEN {x.event.nid, x.event.prop.cc.changes[1].nid} 
                            ELSE {x.event.nid},
    {}, TraceLog))
        
BootstrapLogIndicesForServer(i) ==
    LET 
        FirstBootstrapLogIndex == SelectInSeq(TraceLog, LAMBDA x: x.event.nid = i /\ x.event.name \in {"InitState", "BecomeFollower", "ApplyConfChange"})
        FirstNonBootstrapLogIndex == SelectInSeq(TraceLog, LAMBDA x: x.event.nid = i /\ x.event.name \notin {"InitState", "BecomeFollower", "ApplyConfChange"})
        LastBootstrapLogIndexUpperBound == IF FirstNonBootstrapLogIndex = 0 THEN Len(TraceLog) ELSE FirstNonBootstrapLogIndex-1
    IN 
        { k \in FirstBootstrapLogIndex..LastBootstrapLogIndexUpperBound: TraceLog[k].event.nid = i }
            
BootstrapLogIndices == UNION { BootstrapLogIndicesForServer(i): i \in Server }

LastBootstrapLog == [ i \in Server |-> TraceLog[Max(BootstrapLogIndicesForServer(i))] ]

BootstrappedConfig(i) == 
    IF LastBootstrapLog[i].event.name = "ApplyConfChange" THEN 
        ToSet(LastBootstrapLog[i].event.prop.cc.newconf)
    ELSE 
        ToSet(LastBootstrapLog[i].event.conf[1])

TraceInitServer == BootstrappedConfig(TraceLog[1].event.nid)
ASSUME TraceInitServer \subseteq TraceServer   

TraceInitServerVars == /\ currentTerm = [i \in Server |-> LastBootstrapLog[i].event.state.term]
                       /\ state = [i \in Server |-> LastBootstrapLog[i].event.role]
                       /\ votedFor = [i \in Server |-> LastBootstrapLog[i].event.state.vote]
TraceInitLogVars    == /\ log          = [i \in Server |-> [j \in 1..LastBootstrapLog[i].event.log |-> [ term |-> 1, type |-> "ConfigEntry", value |-> [newconf |-> BootstrappedConfig(i), learners |-> {}]]]]
                       /\ commitIndex  = [i \in Server |-> LastBootstrapLog[i].event.state.commit]
TraceInitConfigVars == 
    /\ config = [i \in Server |-> [ jointConfig |-> <<BootstrappedConfig(i), {}>>, learners |-> {}] ]
    /\ reconfigCount = 0 
                        

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

ValidatePreStates(i) ==
    \* only validate upon first visit of the logline
    pl = l - 1 =>   /\ currentTerm[i] = logline.event.state.term
                    /\ state[i] = logline.event.role
                    /\ votedFor[i] = logline.event.state.vote
                    /\ Len(log[i]) = logline.event.log
                    /\ commitIndex[i] = logline.event.state.commit
                    /\ config[i].jointConfig = ConfFromLog(logline)

ValidatePostStates(i) ==
    /\ currentTerm'[i] = logline.event.state.term
    /\ state'[i] = logline.event.role
    /\ votedFor'[i] = logline.event.state.vote
    /\ Len(log'[i]) = logline.event.log
    /\ commitIndex'[i] = logline.event.state.commit
    /\ config'[i].jointConfig = ConfFromLog(logline)

\* perform RequestVote transition if logline indicates so
ValidateAfterRequestVote(i, j) == 
    /\ ValidatePostStates(i)
    /\ \E m \in DOMAIN pendingMessages':
       /\ \/ LoglineIsRequestVoteRequest(m)
          \/ /\ LoglineIsRequestVoteResponse(m)
             /\ m.msource = m.mdest
       /\ OneMoreMessage(m)

RequestVoteIfLogged(i, j) ==
    /\ \/ LoglineIsMessageEvent("SendRequestVoteRequest", i, j)
       \* etcd candidate sends MsgVoteResp to itself upon compain starting
       \/ /\ LoglineIsMessageEvent("SendRequestVoteResponse", i, j)
          /\ i = j 
    /\ RequestVote(i, j)
    /\ ValidateAfterRequestVote(i, j)   

\* perform BecomeLeader transition if logline indicates so
ValidateAfterBecomeLeader(i) == 
    /\ ValidatePostStates(i)
    /\ logline.event.role = "StateLeader"
    /\ state'[i] = Leader
    
BecomeLeaderIfLogged(i) ==
    /\ LoglineIsNodeEvent("BecomeLeader", i)
    /\ BecomeLeader(i)
    /\ ValidateAfterBecomeLeader(i)

\* perform ClientRequest transition if logline indicates so
ClientRequestIfLogged(i, v) == 
    /\ LoglineIsNodeEvent("Replicate", i)
    /\ ClientRequest(i, v)

\* perform AdvanceCommitIndex transition if logline indicates so
ValidateAfterAdvanceCommitIndex(i) ==
    /\ ValidatePostStates(i)
    /\ logline.event.role = "StateLeader"
    /\ state[i] = Leader

AdvanceCommitIndexIfLogged(i) ==
    /\ LoglineIsNodeEvent("Commit", i)
    /\ AdvanceCommitIndex(i)
    /\ ValidateAfterAdvanceCommitIndex(i)    

\* perform AppendEntries transition if logline indicates so
ValidateAfterAppendEntries(i, j) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsAppendEntriesRequest(msg)
        /\ OneMoreMessage(msg)

ValidateAfterHeartbeat(i, j) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsAppendEntriesRequest(msg)
        /\ OneMoreMessage(msg)

ValidateAfterAppendEntriesToSelf(i) ==
    /\ ValidatePostStates(i)
    /\ \E msg \in DOMAIN pendingMessages':
        /\ LoglineIsAppendEntriesResponse(msg)
        /\ msg.msource = msg.mdest
        \* There is now one more message of this type.
        /\ OneMoreMessage(msg)

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

SendSnapshotIfLogged(i, j, index) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesRequest", i, j)
    /\ logline.event.msg.type = "MsgSnap"
    /\ index = logline.event.msg.entries
    /\ SendSnapshot(i, j, index)
    /\ ValidateAfterAppendEntries(i, j)

AppendEntriesToSelfIfLogged(i) ==
    /\ LoglineIsMessageEvent("SendAppendEntriesResponse", i, i)
    /\ AppendEntriesToSelf(i)
    /\ ValidateAfterAppendEntriesToSelf(i)

ReceiveMessageTraceNames == { "ReceiveAppendEntriesRequest", "ReceiveAppendEntriesResponse", "ReceiveRequestVoteRequest", "ReceiveRequestVoteResponse", "ReceiveSnapshot" }
\* perform Receive transition if logline indicates so
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

\* perform AddNewServer transition if logline indicates so
AddNewServerIfLogged(i, j) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ Len(logline.event.prop.cc.changes) = 1
    /\ logline.event.prop.cc.changes[1].action = "AddNewServer"
    /\ logline.event.prop.cc.changes[1].nid = j
    /\ AddNewServer(i, j)

\* perform AddLearner transition if logline indicates so
AddLearnerIfLogged(i, j) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ Len(logline.event.prop.cc.changes) = 1
    /\ logline.event.prop.cc.changes[1].action = "AddLearner"
    /\ logline.event.prop.cc.changes[1].nid = j
    /\ AddLearner(i, j)

\* perform DeleteServer transition if logline indicates so
DeleteServerIfLogged(i, j) ==
    /\ LoglineIsNodeEvent("ChangeConf", i)
    /\ Len(logline.event.prop.cc.changes) = 1
    /\ logline.event.prop.cc.changes[1].action = "RemoveServer"
    /\ logline.event.prop.cc.changes[1].nid = j
    /\ DeleteServer(i, j)
    
ApplySimpleConfChangeIfLogged(i) ==
    /\ LoglineIsNodeEvent("ApplyConfChange", i)
    /\ ApplySimpleConfChange(i)

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

\* skip unused logs
SkipUnusedLogline ==
    /\ \/ /\ LoglineIsEvent("SendAppendEntriesResponse")
          /\ logline.event.msg.from # logline.event.msg.to
       \/ /\ LoglineIsEvent("SendRequestVoteResponse")
          /\ logline.event.msg.from # logline.event.msg.to
       \/ LoglineIsBecomeFollowerInUpdateTermOrReturnToFollower
       \/ LoglineIsEvent("ReduceNextIndex") \* shall not be necessary when this is removed from raft
    /\ UNCHANGED <<vars>>

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
          /\ \E i,j \in Server : \E b,e \in matchIndex[i][j]+1..Len(log[i])+1 : AppendEntriesIfLogged(i, j, <<b,e>>)
       \/ /\ LoglineIsEvent("SendAppendEntriesResponse") 
          /\ \E i \in Server : AppendEntriesToSelfIfLogged(i)
       \/ /\ LoglineIsEvent("SendAppendEntriesRequest")
          /\ \E i,j \in Server : HeartbeatIfLogged(i, j) /\ logline.event.msg.type = "MsgHeartbeat"
       \/ /\ LoglineIsEvent("SendAppendEntriesRequest") /\ logline.event.msg.type = "MsgSnap"
          /\ \E i,j \in Server : \E index \in 1..commitIndex[i] : SendSnapshotIfLogged(i, j, index)
       \/ /\ LoglineIsEvent("BecomeCandidate")
          /\ \E i \in Server : TimeoutIfLogged(i)
       \/ /\ LoglineIsEvent("ChangeConf") 
          /\ \E i,j \in Server: AddNewServerIfLogged(i, j)
       \/ /\ LoglineIsEvent("ChangeConf")
          /\ \E i,j \in Server: AddLearnerIfLogged(i, j)
       \/ /\ LoglineIsEvent("ChangeConf")
          /\ \E i,j \in Server: DeleteServerIfLogged(i, j)
       \/ /\ LoglineIsEvent("ApplyConfChange")
          /\ \E i \in Server: ApplySimpleConfChangeIfLogged(i)
       \/ /\ LoglineIsEvent("Ready")
          /\ \E i \in Server: ReadyIfLogged(i)
       \/ /\ LoglineIsEvent("InitState")
          /\ \E i \in Server: RestartIfLogged(i)
       \/ /\ LoglineIsEvent("BecomeFollower")
          /\ \E i \in Server: StepDownToFollowerIfLogged(i)
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
    \* A high-level state  s  can appear multiple times in a system trace.  Including the
     \* current level in TLC's view ensures that TLC will not stop model checking when  s
     \* appears the second time in the trace.  Put differently,  TraceView  causes TLC to
     \* consider  s_i  and s_j  , where  i  and  j  are the positions of  s  in the trace,
     \* to be different states.
    <<vars, l>>

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
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

etcd == INSTANCE etcdraft
etcdSpec == etcd!Init /\ [][etcd!NextDynamic]_etcd!vars

==================================================================================


