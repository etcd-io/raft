---------- MODULE MCetcdraft ----------
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

EXTENDS etcdraft

CONSTANT ReconfigurationLimit
ASSUME ReconfigurationLimit \in Nat

CONSTANT MaxTermLimit
ASSUME MaxTermLimit \in Nat

\* Limit on client requests
CONSTANT RequestLimit
ASSUME RequestLimit \in Nat

etcd == INSTANCE etcdraft

\* Application uses Node (instead of RawNode) will have multiple ConfigEntry entries appended to log in bootstrapping.
BootstrapLog ==
    LET prevConf(y) == IF Len(y) = 0 THEN {} ELSE y[Len(y)].value.newconf
    IN FoldSeq(LAMBDA x, y: Append(y, [ term  |-> 1, type |-> ConfigEntry, value |-> [ newconf |-> prevConf(y) \union {x}, learners |-> {} ] ]), <<>>, SetToSeq(InitServer))

\* etcd is bootstrapped in two ways.
\* 1. bootstrap a cluster for the first time: server vars are initialized with term 1 and pre-inserted log entries for initial configuration.
\* 2. adding a new member: server vars are initialized with all state 0
\* 3. restarting an existing member: all states are loaded from durable storage
etcdInitServerVars  == /\ currentTerm = [i \in Server |-> IF i \in InitServer THEN 1 ELSE 0]
                       /\ state       = [i \in Server |-> Follower]
                       /\ votedFor    = [i \in Server |-> Nil]
etcdInitLogVars     == /\ log          = [i \in Server |-> IF i \in InitServer THEN BootstrapLog ELSE <<>>]
                       /\ commitIndex  = [i \in Server |-> IF i \in InitServer THEN Cardinality(InitServer) ELSE 0]
etcdInitConfigVars  == /\ config = [i \in Server |-> [ jointConfig |-> IF i \in InitServer THEN <<InitServer, {}>> ELSE <<{}, {}>>, learners |-> {}]]
                       /\ reconfigCount = 0 \* the bootstrap configuraitons are not counted

\* This file controls the constants as seen below.
\* In addition to basic settings of how many nodes are to be model checked,
\* the model allows to place additional limitations on the state space of the program.

\* Limit the # of reconfigurations to ReconfigurationLimit
MCAddNewServer(i, j) ==
    /\ reconfigCount < ReconfigurationLimit
    /\ etcd!AddNewServer(i, j)
MCDeleteServer(i, j) ==
    /\ reconfigCount < ReconfigurationLimit
    /\ etcd!DeleteServer(i, j)
MCAddLearner(i, j) ==
    /\ reconfigCount < ReconfigurationLimit
    /\ etcd!AddLearner(i, j)

\* Limit the terms that can be reached. Needs to be set to at least 3 to
\* evaluate all relevant states. If set to only 2, the candidate_quorum
\* constraint below is too restrictive.
MCTimeout(i) ==
    \* Limit the term of each server to reduce state space
    /\ currentTerm[i] < MaxTermLimit
    \* Limit max number of simultaneous candidates
    \* We made several restrictions to the state space of Raft. However since we
    \* made these restrictions, Deadlocks can occur at places that Raft would in
    \* real-world deployments handle graciously.
    \* One example of this is if a Quorum of nodes becomes Candidate but can not
    \* timeout anymore since we constrained the terms. Then, an artificial Deadlock
    \* is reached. We solve this below. If TermLimit is set to any number >2, this is
    \* not an issue since breadth-first search will make sure that a similar
    \* situation is simulated at term==1 which results in a term increase to 2.
    /\ Cardinality({ s \in GetConfig(i) : state[s] = Candidate}) < 1
    /\ etcd!Timeout(i)

\* Limit number of requests (new entries) that can be made
MCClientRequest(i, v) ==
    \* Allocation-free variant of Len(SelectSeq(log[i], LAMBDA e: e.contentType = TypeEntry)) < RequestLimit
    /\ FoldSeq(LAMBDA e, count: IF e.type = ValueEntry THEN count + 1 ELSE count, 0, log[i]) < RequestLimit
    /\ etcd!ClientRequest(i, v)

\* Limit how many identical append entries messages each node can send to another
\* Limit number of duplicate messages sent to the same server
MCSend(msg) ==
    \* One AppendEntriesRequest per node-pair at a time:
    \* a) No AppendEntries request from i to j.
    /\ ~ \E n \in DOMAIN messages \union DOMAIN pendingMessages:
        /\ n.mdest = msg.mdest
        /\ n.msource = msg.msource
        /\ n.mterm = msg.mterm
        /\ n.mtype = AppendEntriesRequest
        /\ msg.mtype = AppendEntriesRequest
    \* b) No (corresponding) AppendEntries response from j to i.
    /\ ~ \E n \in DOMAIN messages \union DOMAIN pendingMessages:
        /\ n.mdest = msg.msource
        /\ n.msource = msg.mdest
        /\ n.mterm = msg.mterm
        /\ n.mtype = AppendEntriesResponse
        /\ msg.mtype = AppendEntriesRequest
    /\ etcd!Send(msg)

mc_etcdSpec ==   
    /\ Init
    /\ [][NextDynamic]_vars

\* Symmetry set over possible servers. May dangerous and is only enabled
\* via the Symmetry option in cfg file.
Symmetry == Permutations(Server)

\* Include all variables in the view, which is similar to defining no view.
View == << vars >>

----

===================================