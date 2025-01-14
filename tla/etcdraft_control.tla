--------------------------------- MODULE etcdraft_control ---------------------------------
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
\*
\* This is the extended specification for the Raft consensus algorithm to add
\* additional controls for state exploration.
\*
\* Copyright 2014 Diego Ongaro, 2015 Brandon Amos and Huanchen Zhang,
\* 2016 Daniel Ricketts, 2021 George Pîrlea and Darius Foo.
\*
\* This work is licensed under the Creative Commons Attribution-4.0
\* International License https://creativecommons.org/licenses/by/4.0/

EXTENDS etcdraft

----
\* Define controls for state transitions

SpecActions == {
    "RequestVote",
    "BecomeLeader",
    "ClientRequest",
    "AdvanceCommitIndex",
    "AppendEntries",
    "AppendEntriesToSelf",
    "Heartbeat",
    "SendSnapshot",
    "Receive",
    "Timeout",
    "StepDownToFollower",
    "Ready",
    "PersistState",
    "ApplyConfChange",
    "SendMessages",
    "Advance",
    "Restart",
    "DuplicateMessage",
    "DropMessage",
    "AddNewServer",
    "AddLearner",
    "DeleteServer"
}

\* Default implementation of IsEnabled which controls if the action is enabled or not.
IsEnabled(action, args) == action \in SpecActions
\* Default implementation of PostAction which is called after the action.
PostAction(action, args) == TRUE

\* This is a wrapper of top level actions in the spec which allows injecting of custom
\* IsEnabled to constrain the action and custom PostAction to process necessary operations
\* after the action (validation for example).
SpecAction(action, args, op(_)) == 
    /\ IsEnabled(action, args)
    /\ op(action)
    /\ PostAction(action, args)

etcd == INSTANCE etcdraft

Controlled_RequestVote(i, j) == SpecAction("RequestVote", <<i, j>>, LAMBDA act: etcd!RequestVote(i, j))
Controlled_BecomeLeader(i) == SpecAction("BecomeLeader", <<i>>, LAMBDA act: etcd!BecomeLeader(i))
Controlled_ClientRequest(i, v) == SpecAction("ClientRequest", <<i, v>>, LAMBDA act: etcd!ClientRequest(i, v))
Controlled_AdvanceCommitIndex(i) == SpecAction("AdvanceCommitIndex", <<i>>, LAMBDA act: etcd!AdvanceCommitIndex(i))
Controlled_AppendEntriesToSelf(i) == SpecAction("AppendEntriesToSelf", <<i>>, LAMBDA act: etcd!AppendEntriesToSelf(i))
Controlled_Receive(m) == SpecAction("Receive", <<m>>, LAMBDA act: etcd!Receive(m))
Controlled_Timeout(i) == SpecAction("Timeout", <<i>>, LAMBDA act: etcd!Timeout(i))
Controlled_StepDownToFollower(i) == SpecAction("StepDownToFollower", <<i>>, LAMBDA act: etcd!StepDownToFollower(i))
Controlled_Ready(i) == SpecAction("Ready", <<i>>, LAMBDA  act: etcd!Ready(i))
Controlled_PersistState(i) == SpecAction("PersistState", <<i>>, LAMBDA act: etcd!PersistState(i))
Controlled_ApplyConfChange(i) == SpecAction("ApplyConfChange", <<i>>, LAMBDA act: etcd!ApplyConfChange(i))
Controlled_SendMessages(i) == SpecAction("SendMessages", <<i>>, LAMBDA act: etcd!SendMessages(i))
Controlled_Advance(i) == SpecAction("Advance", <<i>>, LAMBDA act: etcd!Advance(i))
Controlled_Restart(i) == SpecAction("Restart", <<i>>, LAMBDA act: etcd!Restart(i))
Controlled_DuplicateMessage(m) == SpecAction("DuplicateMessage", <<m>>, LAMBDA act: etcd!DuplicateMessage(m))
Controlled_DropMessage(m) == SpecAction("DropMessage", <<m>>, LAMBDA act: etcd!DropMessage(m))
Controlled_AddNewServer(i, j) == SpecAction("AddNewServer", <<i, j>>, LAMBDA act: etcd!AddNewServer(i, j))
Controlled_AddLearner(i, j) == SpecAction("AddLearner", <<i, j>>, LAMBDA act: etcd!AddLearner(i, j))
Controlled_DeleteServer(i, j) == SpecAction("DeleteServer", <<i, j>>, LAMBDA act: etcd!DeleteServer(i, j))

Controlled_AppendEntries(i, j) ==
    \E b \in matchIndex[i][j]+1..Len(log[i])+1 : 
            \E e \in b..Len(log[i])+1 : 
                SpecAction("AppendEntries", << i, j, <<b,e>> >>, LAMBDA act: etcd!AppendEntriesInRangeToPeer("app", i, j, <<b,e>>))
Controlled_Heartbeat(i, j) ==
    SpecAction("Heartbeat", <<i, j>>, LAMBDA act: etcd!AppendEntriesInRangeToPeer("heartbeat", i, j, <<1,1>>))
Controlled_SendSnapshot(i, j) ==
    \E index \in 1..commitIndex[i] : 
        SpecAction("SendSnapshot", <<i, j>>, LAMBDA act: etcd!AppendEntriesInRangeToPeer("snapshot", i, j, <<1,index+1>>))    
    
-----

Controlled_NextRequestVoteRequest == \E m \in DOMAIN messages : m.mtype = RequestVoteRequest /\ Controlled_Receive(m)
Controlled_NextRequestVoteResponse == \E m \in DOMAIN messages : m.mtype = RequestVoteResponse /\ Controlled_Receive(m)
Controlled_NextAppendEntriesRequest == \E m \in DOMAIN messages : m.mtype = AppendEntriesRequest /\ Controlled_Receive(m)
Controlled_NextAppendEntriesResponse == \E m \in DOMAIN messages : m.mtype = AppendEntriesResponse /\ Controlled_Receive(m)

\* Defines how the variables may transition.
Controlled_Next == 
    \/ \E i,j \in Server : Controlled_RequestVote(i, j)
    \/ \E i \in Server : Controlled_BecomeLeader(i)
    \/ \E i \in Server: Controlled_ClientRequest(i, 0)
    \/ \E i \in Server : Controlled_AdvanceCommitIndex(i)
    \/ \E i,j \in Server : Controlled_AppendEntries(i, j)
    \/ \E i \in Server : Controlled_AppendEntriesToSelf(i)
    \/ \E i,j \in Server : Controlled_Heartbeat(i, j)
    \/ \E i,j \in Server : Controlled_SendSnapshot(i, j)
    \*\/ \E m \in DOMAIN messages : Receive(m)
    \/ Controlled_NextRequestVoteRequest
    \/ Controlled_NextRequestVoteResponse
    \/ Controlled_NextAppendEntriesRequest
    \/ Controlled_NextAppendEntriesResponse
    \/ \E i \in Server : Controlled_Timeout(i)
    \/ \E i \in Server : Controlled_StepDownToFollower(i)
    \/ \E i \in Server : Controlled_Ready(i)
    \/ \E i \in Server : Controlled_PersistState(i)
    \/ \E i \in Server : Controlled_ApplyConfChange(i)
    \/ \E i \in Server : Controlled_SendMessages(i)
    \/ \E i \in Server : Controlled_Advance(i)
    \/ \E i \in Server : Controlled_Restart(i)
    \* Only duplicate once
    \/ \E m \in DOMAIN messages : 
        /\ messages[m] = 1
        /\ Controlled_DuplicateMessage(m)
    \* Only drop if it makes a difference            
    \/ \E m \in DOMAIN messages : 
        /\ messages[m] = 1
        /\ Controlled_DropMessage(m)
    \/ \E i, j \in Server : Controlled_AddNewServer(i, j)
    \/ \E i, j \in Server : Controlled_AddLearner(i, j)
    \/ \E i, j \in Server : Controlled_DeleteServer(i, j)

===============================================================================
