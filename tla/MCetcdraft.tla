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

CONSTANTS s1,s2,s3,s4,s5

CONSTANT ReconfigurationLimit
ASSUME ReconfigurationLimit \in Nat

CONSTANT MaxTermLimit
ASSUME MaxTermLimit \in Nat

CONSTANT  MaxRestart
ASSUME MaxRestart \in Nat

\* Limit on client requests
CONSTANT RequestLimit
ASSUME RequestLimit \in Nat

CONSTANT MaxStepDownToFollower
ASSUME MaxStepDownToFollower \in Nat

etcd == INSTANCE etcdraft

VARIABLE  restartCount
VARIABLE  stepDownCount
constaintVars == <<restartCount, stepDownCount>>

mcVars == << vars, constaintVars >>

ChangeOneVar(_vars, index, op(_)) ==
    IF op(index) THEN 
        UNCHANGED RemoveAt(_vars, index)
    ELSE 
        UNCHANGED _vars

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
                       /\ applied      = [i \in Server |-> 0]
etcdInitConfigVars  == /\ config = [i \in Server |-> [ jointConfig |-> IF i \in InitServer THEN <<InitServer, {}>> ELSE <<{}, {}>>, learners |-> {}]]
                       /\ appliedConfChange = [i \in Server |-> Len(BootstrapLog)]

\* This file controls the constants as seen below.
\* In addition to basic settings of how many nodes are to be model checked,
\* the model allows to place additional limitations on the state space of the program.

\* Check the # of reconfigurations and return true if it is lower than ReconfigurationLimit
UnderReconfigLimit ==
    LET leaders == {i \in Server : state[i] = Leader}
    IN  
        \E i \in leaders : 
            /\ \A j \in leaders \ {i} : Len(log[i]) >= Len(log[j])
            /\ ReconfigurationLimit > FoldSeq(LAMBDA x, y: IF x.type = ConfigEntry THEN 1 ELSE 0, -Cardinality(InitServer), log[i])

MCIsEnabled(action, args) ==
    CASE action \in { "AddNewServer", "DeleteServer", "AddLearner" } -> UnderReconfigLimit
      [] action = "Restart" -> restartCount < MaxRestart
      [] action = "Timeout" -> 
            \* Limit the term of each server to reduce state space
            /\ currentTerm[args[1]] < MaxTermLimit
            \* Limit max number of simultaneous candidates
            \* We made several restrictions to the state space of Raft. However since we
            \* made these restrictions, Deadlocks can occur at places that Raft would in
            \* real-world deployments handle graciously.
            \* One example of this is if a Quorum of nodes becomes Candidate but can not
            \* timeout anymore since we constrained the terms. Then, an artificial Deadlock
            \* is reached. We solve this below. If TermLimit is set to any number >2, this is
            \* not an issue since breadth-first search will make sure that a similar
            \* situation is simulated at term==1 which results in a term increase to 2.
            /\ Cardinality({ s \in GetConfig(args[1]) : state[s] = Candidate}) < 1
      [] action = "ClientRequest" ->
            \* Allocation-free variant of Len(SelectSeq(log[i], LAMBDA e: e.contentType = TypeEntry)) < RequestLimit
            FoldSeq(LAMBDA e, count: IF e.type = ValueEntry THEN count + 1 ELSE count, 0, log[args[1]]) < RequestLimit
      [] action = "Ready" ->
            \* If there is no new message and no state change, we can skip Advance to reduce state spece
            \* to be explored
            LET rd == ReadyData(args[1])
            IN 
                \/ rd.msgs /= EmptyBag
                \/ DurableStateFromReady(rd) /= durableState[args[1]]
      [] action = "StepDownToFollower" -> stepDownCount < MaxStepDownToFollower
      [] OTHER -> etcd!IsEnabled(action, args)

MCPostAction(action, args) ==
    CASE action = "Restart" -> 
            /\ restartCount' = restartCount + 1
            /\ UNCHANGED <<stepDownCount>>
      [] action = "StepDownToFollower" -> 
            /\ stepDownCount' = stepDownCount + 1
            /\ UNCHANGED <<restartCount>>
      [] OTHER -> UNCHANGED <<constaintVars>>
\* Symmetry set over possible servers. May dangerous and is only enabled
\* via the Symmetry option in cfg file.
Symmetry == Permutations(Server)

\* Include all variables in the view, which is similar to defining no view.
View == mcVars

InitConstraintVars ==
    /\ restartCount = 0
    /\ stepDownCount = 0

MCInit == 
    /\ Init
    /\ InitConstraintVars

MCSpec == MCInit /\ [][Next]_mcVars

----
===================================