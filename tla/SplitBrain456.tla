---------------------------- MODULE SplitBrain456 ----------------------------
\* Copyright 2026 The etcd Authors
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
\* Regression scenario for https://github.com/etcd-io/raft/issues/456
\* (scenario script adapted from the BugDriver spec attached to the issue).
\*
\* It scripts the following schedule against etcdraft.tla:
\*   1. s1 becomes leader of {s1,s2,s3} in term 1.
\*   2. s1 commits config change #1: {s1,s2,s3}      -> {s1,s2,s3,s4}.
\*   3. s1 commits config change #2: {s1,s2,s3,s4}   -> {s1,s2,s3,s4,s5}.
\*   4. s2 learns both entries are committed and applies config #2;
\*      s3 has both entries in its log but learned only entry #1 is
\*      committed and applied neither.
\*   5. s2 campaigns and wins term 2 with votes {s2,s4,s5} under config
\*      {s1,s2,s3,s4,s5}.
\*   6. s3 attempts to campaign under its stale active config {s1,s2,s3}.
\*
\* Without the HasUnappliedConfChange guard on Timeout, step 6 elects s3
\* with votes {s1,s3} in the same term 2 - two leaders in one term
\* (MoreThanOneLeaderInv is violated). With the guard, s3 must not campaign
\* while config change #1 is committed but unapplied, and the scenario
\* stops right before step 6, which is asserted by CampaignBlockedByGuard.
\*
\* Run:
\*   java -cp tla2tools.jar:CommunityModules-deps.jar tlc2.TLC \
\*     -config SplitBrain456.cfg SplitBrain456.tla
EXTENDS etcdraft

VARIABLE pc

DriverVars == <<vars, pc>>

\* Message-selection helpers used by the scripted schedule below.
RecvRVReq(dst, src) ==
    \E m \in DOMAIN messages :
        /\ m.mtype = RequestVoteRequest
        /\ m.msource = src
        /\ m.mdest = dst
        /\ Receive(m)

RecvRVResp(dst, src) ==
    \E m \in DOMAIN messages :
        /\ m.mtype = RequestVoteResponse
        /\ m.msource = src
        /\ m.mdest = dst
        /\ Receive(m)

RecvAEReq(dst, src, subtype) ==
    \E m \in DOMAIN messages :
        /\ m.mtype = AppendEntriesRequest
        /\ m.msubtype = subtype
        /\ m.msource = src
        /\ m.mdest = dst
        /\ Receive(m)

RecvAEReqAck(dst, src, subtype) ==
    \E m \in DOMAIN messages :
        /\ m.mtype = AppendEntriesRequest
        /\ m.msubtype = subtype
        /\ m.msource = src
        /\ m.mdest = dst
        /\ Receive(m)
        /\ m \notin DOMAIN messages'

RecvAEResp(dst, src, subtype) ==
    \E m \in DOMAIN messages :
        /\ m.mtype = AppendEntriesResponse
        /\ m.msubtype = subtype
        /\ m.msource = src
        /\ m.mdest = dst
        /\ Receive(m)

DriverInit == Init /\ pc = 0

DriverNext ==
    \/ \* Elect server 1 as leader in term 1 with votes from 1 and 2.
       /\ pc = 0
       /\ Timeout(1)
       /\ pc' = 1
    \/ /\ pc = 1
       /\ RequestVote(1, 1)
       /\ pc' = 2
    \/ /\ pc = 2
       /\ Ready(1)
       /\ pc' = 3
    \/ /\ pc = 3
       /\ RecvRVResp(1, 1)
       /\ pc' = 4
    \/ /\ pc = 4
       /\ RequestVote(1, 2)
       /\ pc' = 5
    \/ /\ pc = 5
       /\ Ready(1)
       /\ pc' = 6
    \/ /\ pc = 6
       /\ RecvRVReq(2, 1) \* server 2 first updates to term 1
       /\ pc' = 7
    \/ /\ pc = 7
       /\ RecvRVReq(2, 1) \* server 2 grants the vote
       /\ pc' = 8
    \/ /\ pc = 8
       /\ Ready(2)
       /\ pc' = 9
    \/ /\ pc = 9
       /\ RecvRVResp(1, 2)
       /\ pc' = 10
    \/ /\ pc = 10
       /\ BecomeLeader(1)
       /\ pc' = 11

    \/ \* Append and commit the first configuration entry: {1,2,3} -> {1,2,3,4}.
       /\ pc = 11
       /\ AddNewServer(1, 4)
       /\ pc' = 12
    \/ /\ pc = 12
       /\ AppendEntriesToSelf(1)
       /\ pc' = 13
    \/ /\ pc = 13
       /\ Ready(1)
       /\ pc' = 14
    \/ /\ pc = 14
       /\ RecvAEResp(1, 1, "app")
       /\ pc' = 15
    \/ /\ pc = 15
       /\ AppendEntries(1, 2, <<1, 2>>)
       /\ pc' = 16
    \/ /\ pc = 16
       /\ Ready(1)
       /\ pc' = 17
    \/ /\ pc = 17
       /\ RecvAEReq(2, 1, "app") \* append entry 1 on server 2
       /\ pc' = 18
    \/ /\ pc = 18
       /\ RecvAEReqAck(2, 1, "app") \* acknowledge entry 1 from server 2
       /\ pc' = 19
    \/ /\ pc = 19
       /\ Ready(2)
       /\ pc' = 20
    \/ /\ pc = 20
       /\ RecvAEResp(1, 2, "app")
       /\ pc' = 21
    \/ /\ pc = 21
       /\ AppendEntries(1, 3, <<1, 2>>)
       /\ pc' = 22
    \/ /\ pc = 22
       /\ Ready(1)
       /\ pc' = 23
    \/ /\ pc = 23
       /\ RecvAEReq(3, 1, "app") \* server 3 first updates to term 1
       /\ pc' = 24
    \/ /\ pc = 24
       /\ RecvAEReq(3, 1, "app") \* append entry 1 on server 3
       /\ pc' = 25
    \/ /\ pc = 25
       /\ RecvAEReqAck(3, 1, "app") \* acknowledge entry 1 from server 3
       /\ pc' = 26
    \/ /\ pc = 26
       /\ Ready(3)
       /\ pc' = 27
    \/ /\ pc = 27
       /\ RecvAEResp(1, 3, "app")
       /\ pc' = 28
    \/ /\ pc = 28
       /\ AdvanceCommitIndex(1)
       /\ pc' = 29
    \/ /\ pc = 29
       /\ ApplySimpleConfChange(1) \* only the leader applies entry 1
       /\ pc' = 30

    \/ \* Append and commit the second configuration entry: {1,2,3,4} -> {1,2,3,4,5}.
       /\ pc = 30
       /\ AddNewServer(1, 5)
       /\ pc' = 31
    \/ /\ pc = 31
       /\ AppendEntriesToSelf(1)
       /\ pc' = 32
    \/ /\ pc = 32
       /\ Ready(1)
       /\ pc' = 33
    \/ /\ pc = 33
       /\ RecvAEResp(1, 1, "app")
       /\ pc' = 34
    \/ /\ pc = 34
       /\ AppendEntries(1, 2, <<2, 3>>)
       /\ pc' = 35
    \/ /\ pc = 35
       /\ Ready(1)
       /\ pc' = 36
    \/ /\ pc = 36
       /\ RecvAEReq(2, 1, "app") \* append entry 2 on server 2
       /\ pc' = 37
    \/ /\ pc = 37
       /\ RecvAEReqAck(2, 1, "app") \* acknowledge entry 2 from server 2
       /\ pc' = 38
    \/ /\ pc = 38
       /\ Ready(2)
       /\ pc' = 39
    \/ /\ pc = 39
       /\ RecvAEResp(1, 2, "app")
       /\ pc' = 40
    \/ /\ pc = 40
       /\ AppendEntries(1, 3, <<2, 3>>)
       /\ pc' = 41
    \/ /\ pc = 41
       /\ Ready(1)
       /\ pc' = 42
    \/ /\ pc = 42
       /\ RecvAEReq(3, 1, "app") \* append entry 2 on server 3
       /\ pc' = 43
    \/ /\ pc = 43
       /\ RecvAEReqAck(3, 1, "app") \* acknowledge entry 2 from server 3
       /\ pc' = 44
    \/ /\ pc = 44
       /\ Ready(3)
       /\ pc' = 45
    \/ /\ pc = 45
       /\ RecvAEResp(1, 3, "app")
       /\ pc' = 46
    \/ /\ pc = 46
       /\ AdvanceCommitIndex(1)
       /\ pc' = 47
    \/ /\ pc = 47
       /\ Heartbeat(1, 2) \* tell server 2 that entry 2 is committed
       /\ pc' = 48
    \/ /\ pc = 48
       /\ Ready(1)
       /\ pc' = 49
    \/ /\ pc = 49
       /\ RecvAEReq(2, 1, "heartbeat")
       /\ pc' = 50
    \/ /\ pc = 50
       /\ Ready(2)
       /\ pc' = 51
    \/ /\ pc = 51
       /\ RecvAEResp(1, 2, "heartbeat")
       /\ pc' = 52
    \/ /\ pc = 52
       /\ ApplySimpleConfChange(2) \* server 2 applies entry 2; server 3 applies neither entry
       /\ pc' = 53

    \/ \* Server 2 campaigns using applied config {1,2,3,4,5} and wins with {2,4,5}.
       /\ pc = 53
       /\ Timeout(2)
       /\ pc' = 54
    \/ /\ pc = 54
       /\ RequestVote(2, 2)
       /\ pc' = 55
    \/ /\ pc = 55
       /\ Ready(2)
       /\ pc' = 56
    \/ /\ pc = 56
       /\ RecvRVResp(2, 2)
       /\ pc' = 57
    \/ /\ pc = 57
       /\ RequestVote(2, 4)
       /\ pc' = 58
    \/ /\ pc = 58
       /\ Ready(2)
       /\ pc' = 59
    \/ /\ pc = 59
       /\ RecvRVReq(4, 2) \* server 4 first updates to term 2
       /\ pc' = 60
    \/ /\ pc = 60
       /\ RecvRVReq(4, 2) \* server 4 grants the vote
       /\ pc' = 61
    \/ /\ pc = 61
       /\ Ready(4)
       /\ pc' = 62
    \/ /\ pc = 62
       /\ RecvRVResp(2, 4)
       /\ pc' = 63
    \/ /\ pc = 63
       /\ RequestVote(2, 5)
       /\ pc' = 64
    \/ /\ pc = 64
       /\ Ready(2)
       /\ pc' = 65
    \/ /\ pc = 65
       /\ RecvRVReq(5, 2) \* server 5 first updates to term 2
       /\ pc' = 66
    \/ /\ pc = 66
       /\ RecvRVReq(5, 2) \* server 5 grants the vote
       /\ pc' = 67
    \/ /\ pc = 67
       /\ Ready(5)
       /\ pc' = 68
    \/ /\ pc = 68
       /\ RecvRVResp(2, 5)
       /\ pc' = 69
    \/ /\ pc = 69
       /\ BecomeLeader(2)
       /\ pc' = 70

    \/ \* Server 3 attempts to campaign in the same term using its stale
       \* active config {1,2,3}. This must be blocked: server 3 still has
       \* the committed-but-unapplied config entries 1 and 2 in its log.
       \* If Timeout(3) were allowed here, server 3 would win with votes
       \* {1,3} under config {1,2,3} while server 2 is already leader of
       \* term 2 under config {1,2,3,4,5} - a split brain.
       /\ pc = 70
       /\ Timeout(3)
       /\ pc' = 71
    \/ /\ pc = 71
       /\ RequestVote(3, 3)
       /\ pc' = 72
    \/ /\ pc = 72
       /\ Ready(3)
       /\ pc' = 73
    \/ /\ pc = 73
       /\ RecvRVResp(3, 3)
       /\ pc' = 74
    \/ /\ pc = 74
       /\ RequestVote(3, 1)
       /\ pc' = 75
    \/ /\ pc = 75
       /\ Ready(3)
       /\ pc' = 76
    \/ /\ pc = 76
       /\ RecvRVReq(1, 3) \* server 1 first updates to term 2
       /\ pc' = 77
    \/ /\ pc = 77
       /\ RecvRVReq(1, 3) \* server 1 grants the vote
       /\ pc' = 78
    \/ /\ pc = 78
       /\ Ready(1)
       /\ pc' = 79
    \/ /\ pc = 79
       /\ RecvRVResp(3, 1)
       /\ pc' = 80
    \/ /\ pc = 80
       /\ BecomeLeader(3)
       /\ pc' = 81

DriverSpec == DriverInit /\ [][DriverNext]_DriverVars /\ WF_DriverVars(DriverNext)

\* Sanity check that the schedule is not vacuously safe: every step up to
\* and including the election of server 2 (pc = 70) must remain enabled,
\* and the run must stop exactly at server 3's blocked campaign. If an
\* earlier step becomes disabled by an unrelated spec change, this property
\* fails and the scenario needs to be updated.
CampaignBlockedByGuard == <>[](pc = 70)

===============================================================================
