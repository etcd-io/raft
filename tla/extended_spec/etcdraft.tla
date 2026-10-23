-------------------------- MODULE etcdraft --------------------------
EXTENDS Naturals, Integers, Bags, FiniteSets, Sequences, SequencesExt, FiniteSetsExt, BagsExt, TLC

\* The initial and global set of server IDs.
CONSTANTS InitServer, Server

\* Log metadata to distinguish values from configuration changes.
CONSTANT ValueEntry, ConfigEntry

\* Server states.
CONSTANTS 
    \* @type: Str;
    Follower,
    \* @type: Str;
    Candidate,
    \* @type: Str;
    Leader

\* A reserved value.
CONSTANTS 
    \* @type: Int;
    Nil

\* Message types:
CONSTANTS
    \* @type: Str;
    RequestVoteRequest,
    \* @type: Str;
    RequestVoteResponse,
    \* @type: Str;
    AppendEntriesRequest,
    \* @type: Str;
    AppendEntriesResponse,
    \* @type: Str;
    SnapshotRequest,
    \* @type: Str;
    SnapshotResponse

\* Progress state constants
\* Reference: tracker/state.go:20-33
CONSTANTS
    \* @type: Str;
    StateProbe,      \* Probe state: don't know follower's last index
    \* @type: Str;
    StateReplicate,  \* Replicate state: normal fast replication
    \* @type: Str;
    StateSnapshot    \* Snapshot state: need to send snapshot

\* Flow control configuration constants
\* Reference: raft.go:205-210, Config.MaxInflightMsgs
CONSTANT
    \* @type: Int;
    MaxInflightMsgs  \* Max inflight messages (per Progress)

ASSUME MaxInflightMsgs \in Nat /\ MaxInflightMsgs > 0

\* When TRUE, enforce FIFO message ordering via sequence numbers
CONSTANT MsgNoReorder
ASSUME MsgNoReorder \in BOOLEAN

\* Maximum number of network partitions (2 = split into 2 groups)
CONSTANT MaxPartitions
ASSUME MaxPartitions \in Nat /\ MaxPartitions >= 1

\* When TRUE, Restart drops all messages to/from the restarting node
CONSTANT RestartDropsMessages
ASSUME RestartDropsMessages \in BOOLEAN

\* Turns off propose-time config change verification.
\* Reference: raft.go:258-278
\* WARNING: can violate QuorumLogInv if new config members lack committed entries.
CONSTANT DisableConfChangeValidation
ASSUME DisableConfChangeValidation \in BOOLEAN

----
\* A bag of records representing requests and responses sent between servers.
VARIABLE
    \* @typeAlias: ENTRY = [term: Int, value: Int];
    \* @typeAlias: LOGT = Seq(ENTRY);
    \* @typeAlias: RVREQT = [mtype: Str, mterm: Int, mlastLogTerm: Int, mlastLogIndex: Int, msource: Int, mdest: Int];
    \* @typeAlias: RVRESPT = [mtype: Str, mterm: Int, mvoteGranted: Bool, msource: Int, mdest: Int ];
    \* @typeAlias: AEREQT = [mtype: Str, mterm: Int, mprevLogIndex: Int, mprevLogTerm: Int, mentries: LOGT, mcommitIndex: Int, msource: Int, mdest: Int ];
    \* @typeAlias: AERESPT = [mtype: Str, mterm: Int, msuccess: Bool, mmatchIndex: Int, mrejectHint: Int, mlogTerm: Int, msource: Int, mdest: Int ];
    \* @typeAlias: MSG = [ wrapped: Bool, mtype: Str, mterm: Int, msource: Int, mdest: Int, RVReq: RVREQT, RVResp: RVRESPT, AEReq: AEREQT, AEResp: AERESPT ];
    \* @type: MSG -> Int;
    messages
VARIABLE 
    pendingMessages

\* FIFO sequence counter (used when MsgNoReorder = TRUE)
VARIABLE
    msgSeqCounter

\* Partition assignment: 0 = no partition, 1..MaxPartitions = partition group
VARIABLE
    partitions

pendingMsgVars == <<pendingMessages, msgSeqCounter>>

messageVars == <<messages, pendingMessages, msgSeqCounter>>

partitionVars == <<partitions>>

----

\* The server's term number.
VARIABLE 
    \* @type: Int -> Int;
    currentTerm
\* The server's state (Follower, Candidate, or Leader).
VARIABLE 
    \* @type: Int -> Str;
    state
\* The candidate the server voted for in its current term, or
\* Nil if it hasn't voted for any.
VARIABLE 
    \* @type: Int -> Int;
    votedFor
serverVars == <<currentTerm, state, votedFor>>

\* Server's log with offset-based indexing.
VARIABLE 
    \* @type: Int -> [ offset: Int, entries: LOGT, snapshotIndex: Int, snapshotTerm: Int ];
    log
\* Ghost variable: full log history for verification
VARIABLE
    \* @type: Int -> LOGT;
    historyLog
\* The highest log index the state machine may apply.
VARIABLE
    \* @type: Int -> Int;
    commitIndex
\* The highest log index applied to the state machine.
VARIABLE
    \* @type: Int -> Int;
    applied
logVars == <<log, historyLog, commitIndex, applied>>

\* Set of servers that responded to RequestVote in currentTerm.
VARIABLE 
    \* @type: Int -> Set(Int);
    votesResponded
\* Set of servers that granted vote in currentTerm.
VARIABLE 
    \* @type: Int -> Set(Int);
    votesGranted
\* @type: Seq(Int -> Set(Int));
candidateVars == <<votesResponded, votesGranted>>

\* Highest log index each follower has acknowledged.
VARIABLE 
    \* @type: Int -> (Int -> Int);
    matchIndex
VARIABLE
    pendingConfChangeIndex
leaderVars == <<matchIndex, pendingConfChangeIndex>>

\* @type: Int -> [jointConfig: Seq(Set(int)), learners: Set(int)]
VARIABLE 
    config
VARIABLE
    reconfigCount

\* Index of the last applied config entry per server
VARIABLE
    \* @type: Int -> Int;
    appliedConfigIndex

configVars == <<config, reconfigCount, appliedConfigIndex>>

VARIABLE
    durableState

\* ============================================================================
\* Progress state machine variables
\* Reference: tracker/progress.go:30-117
\* ============================================================================

VARIABLE
    \* @type: Int -> (Int -> Str);
    progressState

VARIABLE
    \* @type: Int -> (Int -> Int);
    pendingSnapshot

VARIABLE
    \* @type: Int -> (Int -> Int);
    nextIndex

VARIABLE
    \* @type: Int -> (Int -> Bool);
    msgAppFlowPaused

\* ============================================================================
\* Inflights flow control variables
\* Reference: tracker/inflights.go:28-40
\* ============================================================================

VARIABLE
    \* @type: Int -> (Int -> Set(Int));
    inflights        \* Modeled as set (actual code uses ring buffer); sufficient for capacity checks

progressVars == <<progressState, pendingSnapshot, nextIndex, msgAppFlowPaused, inflights>>

----

vars == <<messageVars, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, partitionVars>>

\* View variables: excludes ghost/derived variables for state space reduction
view_vars == <<messages, pendingMessages, serverVars, candidateVars, leaderVars, log, commitIndex, config, progressVars, partitionVars>>

----
\* Helpers

\* The set of all quorums. This just calculates simple majorities, but the only
\* important property is that every quorum overlaps with every other.
Quorum(c) == {i \in SUBSET(c) : Cardinality(i) * 2 > Cardinality(c)}

\* ============================================================================
\* Network Partition Helpers
\* ============================================================================

\* Check if there is currently a network partition active
IsPartitioned == \E i \in Server : partitions[i] /= 0

\* Check if two servers can communicate (in the same partition or no partition)
CanCommunicate(i, j) ==
    \/ partitions[i] = 0  \* No partition active for i
    \/ partitions[j] = 0  \* No partition active for j
    \/ partitions[i] = partitions[j]  \* Same partition

\* Get all messages that cross partition boundaries (to be dropped)
CrossPartitionMessages ==
    {m \in DOMAIN messages : 
        /\ partitions[m.msource] /= 0 
        /\ partitions[m.mdest] /= 0
        /\ partitions[m.msource] /= partitions[m.mdest]}

\* Get all pending messages that cross partition boundaries
CrossPartitionPendingMessages ==
    {m \in DOMAIN pendingMessages : 
        /\ partitions[m.msource] /= 0 
        /\ partitions[m.mdest] /= 0
        /\ partitions[m.msource] /= partitions[m.mdest]}

\* Drop all messages between two servers
DropMessagesBetween(i, j) ==
    {m \in DOMAIN messages : (m.msource = i /\ m.mdest = j) \/ (m.msource = j /\ m.mdest = i)}

\* Drop all messages to/from a specific server
MessagesToOrFrom(i) ==
    {m \in DOMAIN messages : m.msource = i \/ m.mdest = i}

\* Drop all pending messages to/from a specific server
PendingMessagesToOrFrom(i) ==
    {m \in DOMAIN pendingMessages : m.msource = i \/ m.mdest = i}

\* ============================================================================
\* Virtual Log Helpers (Offset-aware)
\* ============================================================================

\* Get the logical index of the last entry in the log
\* @type: [ offset: Int, entries: LOGT, snapshotIndex: Int, snapshotTerm: Int ] => Int;
LastIndex(xlog) ==
    xlog.offset + Len(xlog.entries) - 1

\* Get the term of the last entry (or snapshot term if empty)
\* @type: [ offset: Int, entries: LOGT, snapshotIndex: Int, snapshotTerm: Int ] => Int;
LastTerm(xlog) ==
    IF Len(xlog.entries) > 0 THEN xlog.entries[Len(xlog.entries)].term
    ELSE xlog.snapshotTerm

\* Check if a logical index is available in the memory log (not compacted)
IsAvailable(i, index) ==
    /\ index >= log[i].offset
    /\ index <= LastIndex(log[i])

\* Get the log entry at a logical index
\* PRECONDITION: IsAvailable(i, index)
LogEntry(i, index) ==
    log[i].entries[index - log[i].offset + 1]

\* Get the term at a logical index; returns 0 for index 0 or unavailable indices.
LogTerm(i, index) ==
    IF index = 0 THEN 0
    ELSE IF index = log[i].snapshotIndex THEN log[i].snapshotTerm
    ELSE IF IsAvailable(i, index) THEN LogEntry(i, index).term
    ELSE 0

\* Reference: raft.go findConflictByTerm - largest index in [firstIndex-1, index] with term <= targetTerm
FindConflictByTerm(i, index, targetTerm) ==
    LET firstIdx == log[i].offset
        validRange == (firstIdx - 1)..index
        matchingIndices == {k \in validRange : LogTerm(i, k) <= targetTerm}
    IN IF index = 0 \/ matchingIndices = {} THEN 0 ELSE Max(matchingIndices)

\* Reference: raft.go handleAppendEntries - compute rejection hint via findConflictByTerm
ComputeRejectHint(i, rejectedIndex, leaderTerm) ==
    LET hintIndex == Min({rejectedIndex, LastIndex(log[i])})
    IN FindConflictByTerm(i, hintIndex, leaderTerm)

\* Add message m to bag of messages.
\* @type: (MSG, MSG -> Int) => MSG -> Int;
WithMessage(m, msgs) == msgs (+) SetToBag({m})

\* Remove message m from bag of messages.
\* @type: (MSG, MSG -> Int) => MSG -> Int;
WithoutMessage(m, msgs) == msgs (-) SetToBag({m})

\* Add message to pendingMessages; adds sequence number when MsgNoReorder is TRUE.
SendDirect(m) == 
    IF MsgNoReorder
    THEN /\ pendingMessages' = WithMessage(m @@ [mseq |-> msgSeqCounter], pendingMessages)
         /\ msgSeqCounter' = msgSeqCounter + 1
    ELSE /\ pendingMessages' = WithMessage(m, pendingMessages)
         /\ UNCHANGED msgSeqCounter

\* All pending messages sent from node i
PendingMessages(i) ==
    FoldBag(LAMBDA x, y: IF y.msource = i THEN BagAdd(x,y) ELSE x, EmptyBag, pendingMessages)

\* Remove all messages in pendingMessages that were sent from node i
ClearPendingMessages(i) ==
    pendingMessages (-) PendingMessages(i)

\* Move all messages which was sent from node i in pendingMessages to messages
SendPendingMessages(i) ==
    LET msgs == PendingMessages(i)
    IN /\ messages' = msgs (+) messages
       /\ pendingMessages' = pendingMessages (-) msgs

\* Remove a message from messages or pendingMessages.
DiscardDirect(m) ==
    IF m \in DOMAIN messages 
    THEN messages' = WithoutMessage(m, messages) /\ UNCHANGED <<pendingMessages, msgSeqCounter>>
    ELSE pendingMessages' = WithoutMessage(m, pendingMessages) /\ UNCHANGED <<messages, msgSeqCounter>>

\* Send response and discard request; adds sequence number when MsgNoReorder is TRUE.
ReplyDirect(response, request) ==
    LET resp == IF MsgNoReorder THEN response @@ [mseq |-> msgSeqCounter] ELSE response
    IN IF request \in DOMAIN messages
       THEN /\ messages' = WithoutMessage(request, messages)
            /\ pendingMessages' = WithMessage(resp, pendingMessages)
            /\ IF MsgNoReorder THEN msgSeqCounter' = msgSeqCounter + 1 ELSE UNCHANGED msgSeqCounter
       ELSE /\ pendingMessages' = WithMessage(resp, WithoutMessage(request, pendingMessages))
            /\ UNCHANGED messages
            /\ IF MsgNoReorder THEN msgSeqCounter' = msgSeqCounter + 1 ELSE UNCHANGED msgSeqCounter

\* Default: change when needed
 Send(m) == SendDirect(m)
 Reply(response, request) == ReplyDirect(response, request) 
 Discard(m) == DiscardDirect(m)

\* Check if m is the lowest-sequence message from source to dest (FIFO ordering).
IsFifoFirst(m) ==
    IF ~MsgNoReorder THEN TRUE
    ELSE ~\E other \in DOMAIN messages:
            /\ other.msource = m.msource
            /\ other.mdest = m.mdest
            /\ other.mseq < m.mseq
     
MaxOrZero(s) == IF s = {} THEN 0 ELSE Max(s)

GetJointConfig(i) == 
    config[i].jointConfig

GetConfig(i) == 
    GetJointConfig(i)[1]

GetOutgoingConfig(i) ==
    GetJointConfig(i)[2]

IsJointConfig(i) ==
    /\ GetJointConfig(i)[2] # {}

GetLearners(i) ==
    config[i].learners

\* Compute ConfState from history sequence for snapshot metadata.
\* @type: Seq([value: a, term: Int, type: Str]) => [voters: Set(Str), learners: Set(Str), outgoing: Set(Str), autoLeave: Bool];
ComputeConfStateFromHistory(history) ==
    LET configIndices == {k \in 1..Len(history) : history[k].type = ConfigEntry}
        lastConfigIdx == IF configIndices /= {} THEN Max(configIndices) ELSE 0
    IN
    IF lastConfigIdx = 0 THEN
        [voters |-> {}, learners |-> {}, outgoing |-> {}, autoLeave |-> FALSE]
    ELSE
        LET entry == history[lastConfigIdx]
            isLeaveJoint == "leaveJoint" \in DOMAIN entry.value /\ entry.value.leaveJoint = TRUE
        IN
        IF isLeaveJoint THEN
            [voters |-> entry.value.newconf,
             learners |-> IF "learners" \in DOMAIN entry.value THEN entry.value.learners ELSE {},
             outgoing |-> {},
             autoLeave |-> FALSE]
        ELSE
            LET hasEnterJoint == "enterJoint" \in DOMAIN entry.value
                enterJoint == IF hasEnterJoint THEN entry.value.enterJoint ELSE FALSE
                hasOldconf == enterJoint /\ "oldconf" \in DOMAIN entry.value
                oldconf == IF hasOldconf THEN entry.value.oldconf ELSE {}
            IN
            [voters |-> entry.value.newconf,
             learners |-> IF "learners" \in DOMAIN entry.value THEN entry.value.learners ELSE {},
             outgoing |-> oldconf,
             autoLeave |-> enterJoint /\ oldconf /= {}]

\* Reference: raft.go applyConfChange(), confchange.go LeaveJoint()
ApplyConfigUpdate(i, k) ==
    LET entry == LogEntry(i, k)
        isLeaveJoint == "leaveJoint" \in DOMAIN entry.value /\ entry.value.leaveJoint = TRUE
        newVoters == IF isLeaveJoint THEN GetConfig(i) ELSE entry.value.newconf
        newLearners == IF "learners" \in DOMAIN entry.value THEN entry.value.learners ELSE {}
        enterJoint == IF "enterJoint" \in DOMAIN entry.value THEN entry.value.enterJoint ELSE FALSE
        outgoing == IF enterJoint THEN entry.value.oldconf ELSE {}
        newAutoLeave == IF isLeaveJoint THEN FALSE ELSE enterJoint
    IN
    [config EXCEPT ![i]= [jointConfig |-> << newVoters, outgoing >>, learners |-> newLearners, autoLeave |-> newAutoLeave]]

\* Apply a single config change to a config record.
\* @type: ([nid: Str, action: Str], [voters: Set(Str), learners: Set(Str)]) => [voters: Set(Str), learners: Set(Str)];
ApplyChange(change, conf) ==
    CASE change.action = "AddNewServer" ->
            [voters   |-> conf.voters \union {change.nid},
             learners |-> conf.learners \ {change.nid}]
      [] change.action = "RemoveServer" ->
            [voters   |-> conf.voters \ {change.nid},
             learners |-> conf.learners \ {change.nid}]
      [] change.action = "AddLearner" ->
            [voters   |-> conf.voters \ {change.nid},
             learners |-> conf.learners \union {change.nid}]
      [] OTHER -> conf

CommitTo(i, c) ==
    commitIndex' = [commitIndex EXCEPT ![i] = Max({@, c})]

CurrentLeaders == {i \in Server : state[i] = Leader}

PersistState(i) ==
    durableState' = [durableState EXCEPT ![i] = [
        currentTerm |-> currentTerm[i],
        votedFor |-> votedFor[i],
        log |-> LastIndex(log[i]),
        entries |-> log[i].entries,
        snapshotIndex |-> log[i].snapshotIndex,
        snapshotTerm |-> log[i].snapshotTerm,
        snapshotHistory |-> @.snapshotHistory,
        commitIndex |-> commitIndex[i],
        config |-> config[i]
    ]]

\* ============================================================================
\* Progress and Inflights helper functions
\* ============================================================================

\* Reference: inflights.go Count()
InflightsCount(i, j) == Cardinality(inflights[i][j])

\* Reference: inflights.go Full()
InflightsFull(i, j) == InflightsCount(i, j) >= MaxInflightMsgs

\* Reference: progress.go IsPaused() - checks msgAppFlowPaused flag
IsPaused(i, j) ==
    CASE progressState[i][j] = StateProbe      -> msgAppFlowPaused[i][j]
      [] progressState[i][j] = StateReplicate  -> msgAppFlowPaused[i][j]
      [] progressState[i][j] = StateSnapshot   -> TRUE
      [] OTHER -> FALSE

\* Reference: inflights.go Add()
AddInflight(i, j, lastIndex) ==
    inflights' = [inflights EXCEPT ![i][j] = @ \cup {lastIndex}]

\* Reference: inflights.go FreeLE()
FreeInflightsLE(i, j, index) ==
    inflights' = [inflights EXCEPT ![i][j] = {idx \in @ : idx > index}]

\* Reference: progress.go ResetState()
ResetInflights(i, j) ==
    inflights' = [inflights EXCEPT ![i][j] = {}]

----
InitMessageVars == /\ messages = EmptyBag
                   /\ pendingMessages = EmptyBag
                   /\ msgSeqCounter = 0
InitServerVars == /\ currentTerm = [i \in Server |-> 0]
                  /\ state       = [i \in Server |-> Follower]
                  /\ votedFor    = [i \in Server |-> Nil]
InitCandidateVars == /\ votesResponded = [i \in Server |-> {}]
                     /\ votesGranted   = [i \in Server |-> {}]
InitLeaderVars == /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
                  /\ pendingConfChangeIndex = [i \in Server |-> 0]
InitLogVars == /\ log          = [i \in Server |-> [offset |-> 1, entries |-> <<>>, snapshotIndex |-> 0, snapshotTerm |-> 0]]
               /\ historyLog   = [i \in Server |-> <<>>]
               /\ commitIndex  = [i \in Server |-> 0]
               /\ applied      = [i \in Server |-> 0]
InitConfigVars == /\ config = [i \in Server |-> [ jointConfig |-> <<InitServer, {}>>, learners |-> {}, autoLeave |-> FALSE]]
                  /\ reconfigCount = 0
                  /\ appliedConfigIndex = [i \in Server |-> 0] 
InitDurableState ==
    durableState = [ i \in Server |-> [
        currentTerm |-> currentTerm[i],
        votedFor |-> votedFor[i],
        log |-> 0,
        entries |-> <<>>,
        snapshotIndex |-> 0,
        snapshotTerm |-> 0,
        snapshotHistory |-> <<>>,
        commitIndex |-> commitIndex[i],
        config |-> config[i]
    ]]

\* Reference: raft.go becomeFollower - all Progress starts at StateProbe
InitProgressVars ==
    /\ progressState = [i \in Server |-> [j \in Server |-> StateProbe]]
    /\ pendingSnapshot = [i \in Server |-> [j \in Server |-> 0]]
    /\ nextIndex = [i \in Server |-> [j \in Server |-> 1]]
    /\ msgAppFlowPaused = [i \in Server |-> [j \in Server |-> FALSE]]
    /\ inflights = [i \in Server |-> [j \in Server |-> {}]]

\* No partition active initially.
InitPartitionVars ==
    /\ partitions = [i \in Server |-> 0]

Init == /\ InitMessageVars
        /\ InitServerVars
        /\ InitCandidateVars
        /\ InitLeaderVars
        /\ InitLogVars
        /\ InitConfigVars
        /\ InitDurableState
        /\ InitProgressVars
        /\ InitPartitionVars

----
\* Define state transitions

\* Server i restarts from stable storage, losing all volatile state.
\* @type: Int => Bool;
Restart(i) ==
    /\ state'          = [state EXCEPT ![i] = Follower]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
    /\ matchIndex'     = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = 0]
    /\ pendingMessages' = ClearPendingMessages(i)
    /\ currentTerm' = [currentTerm EXCEPT ![i] = durableState[i].currentTerm]
    /\ commitIndex' = [commitIndex EXCEPT ![i] = durableState[i].commitIndex]
    /\ votedFor' = [votedFor EXCEPT ![i] = durableState[i].votedFor]
    /\ log' = [log EXCEPT ![i] = [
                    offset |-> durableState[i].snapshotIndex + 1,
                    entries |-> durableState[i].entries,
                    snapshotIndex |-> durableState[i].snapshotIndex,
                    snapshotTerm |-> durableState[i].snapshotTerm
       ]]
    /\ applied' = [applied EXCEPT ![i] = durableState[i].snapshotIndex]
    /\ config' = [config EXCEPT ![i] = durableState[i].config]
    /\ appliedConfigIndex' = [appliedConfigIndex EXCEPT ![i] = durableState[i].commitIndex]
    /\ progressState' = [progressState EXCEPT ![i] = [j \in Server |-> StateProbe]]
    /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i] = [j \in Server |-> FALSE]]
    /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i] = [j \in Server |-> 0]]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
    /\ inflights' = [inflights EXCEPT ![i] = [j \in Server |-> {}]]
    /\ IF RestartDropsMessages
       THEN messages' = messages (-) SetToBag(MessagesToOrFrom(i))
       ELSE UNCHANGED messages
    \* historyLog restored from durable state (entries beyond snapshot lost on crash)
    /\ historyLog' = [historyLog EXCEPT ![i] = durableState[i].snapshotHistory \o durableState[i].entries]
    /\ UNCHANGED <<msgSeqCounter, durableState, reconfigCount, partitions>>

\* Server i times out and starts a new election.
\* @type: Int => Bool;
Timeout(i) == /\ state[i] \in {Follower, Candidate}
              /\ i \in GetConfig(i) \union GetOutgoingConfig(i)
              /\ state' = [state EXCEPT ![i] = Candidate]
              /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
              /\ votedFor' = [votedFor EXCEPT ![i] = i]
              /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
              /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
              /\ UNCHANGED <<messageVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Candidate i sends j a RequestVote request.
\* @type: (Int, Int) => Bool;
RequestVote(i, j) ==
    /\ state[i] = Candidate
    /\ j \in ((GetConfig(i) \union GetOutgoingConfig(i) \union GetLearners(i)) \ votesResponded[i])
    /\ IF i # j 
        THEN Send([mtype            |-> RequestVoteRequest,
                   mterm            |-> currentTerm[i],
                   mlastLogTerm     |-> LastTerm(log[i]),
                   mlastLogIndex    |-> LastIndex(log[i]),
                   msource          |-> i,
                   mdest            |-> j])
        ELSE Send([mtype            |-> RequestVoteResponse,
                   mterm            |-> currentTerm[i],
                   mvoteGranted     |-> TRUE,
                   msource          |-> i,
                   mdest            |-> i])
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Leader i sends j an AppendEntries request containing entries in [b,e) range.
\* N.B. range is right open
\* @type: (Int, Int, <<Int, Int>>, Int) => Bool;
AppendEntriesInRangeToPeer(subtype, i, j, range) ==
    /\ i /= j
    /\ range[1] <= range[2]
    /\ state[i] = Leader
    /\ j \in GetConfig(i) \union GetOutgoingConfig(i) \union GetLearners(i)
    \* Entries must be available (not compacted); heartbeat skips this check
    /\ (range[1] = range[2] \/ range[1] >= log[i].offset)
    \* prevLogIndex must have retrievable term (Bug 76f1249 fix)
    /\ (range[1] = 1 \/ range[1] - 1 = log[i].snapshotIndex \/ range[1] - 1 >= log[i].offset)
    \* Heartbeat bypasses flow control (Reference: raft.go bcastHeartbeat())
    /\ (subtype = "heartbeat" \/ ~IsPaused(i, j))
    /\ LET
        prevLogIndex == range[1] - 1
        \* Upper bound simplifies verification.
        prevLogTerm == IF prevLogIndex > 0 /\ prevLogIndex <= LastIndex(log[i]) THEN
                            LogTerm(i, prevLogIndex)
                        ELSE
                            0
        lastEntry == Min({LastIndex(log[i]), range[2]-1})
        entries == SubSeq(log[i].entries, range[1] - log[i].offset + 1, lastEntry - log[i].offset + 1)
        \* Commit bounded differently per subtype (Reference: raft.go sendAppend())
        commit == CASE subtype = "heartbeat"  -> Min({commitIndex[i], matchIndex[i][j]})
                    [] lastEntry < range[1]   -> commitIndex[i]
                    [] OTHER                  -> Min({commitIndex[i], lastEntry})
        numEntries == Len(entries)
        newInflights == IF lastEntry >= range[1]
                        THEN inflights[i][j] \cup {lastEntry}
                        ELSE inflights[i][j]
        \* Reference: progress.go SentEntries() / SentCommit()
        newMsgAppFlowPaused ==
            CASE subtype = "heartbeat"
                    -> msgAppFlowPaused[i][j]
              [] progressState[i][j] = StateReplicate
                    -> Cardinality(newInflights) >= MaxInflightMsgs
              [] progressState[i][j] = StateProbe /\ numEntries > 0
                    -> TRUE
              [] OTHER -> msgAppFlowPaused[i][j]
       IN /\ Send( [mtype          |-> AppendEntriesRequest,
                    msubtype       |-> subtype,
                    mterm          |-> currentTerm[i],
                    mprevLogIndex  |-> prevLogIndex,
                    mprevLogTerm   |-> prevLogTerm,
                    mentries       |-> entries,
                    mcommitIndex   |-> commit,
                    msource        |-> i,
                    mdest          |-> j])
          \* Inflights only added in StateReplicate for non-heartbeat (Reference: progress.go SentEntries())
          /\ IF lastEntry >= range[1] /\ subtype /= "heartbeat" /\ progressState[i][j] = StateReplicate
             THEN AddInflight(i, j, lastEntry)
             ELSE UNCHANGED inflights
          /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = newMsgAppFlowPaused]
          \* nextIndex advanced only in StateReplicate
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
              IF numEntries > 0 /\ progressState[i][j] = StateReplicate /\ subtype /= "heartbeat"
              THEN @ + numEntries
              ELSE @]
          /\ UNCHANGED <<progressState, pendingSnapshot>>
          /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, partitions>> 

\* etcd leader sends MsgAppResp to itself immediately after appending log entry
AppendEntriesToSelf(i) ==
    /\ state[i] = Leader
    /\ Send([mtype           |-> AppendEntriesResponse,
             msubtype        |-> "app",
             mterm           |-> currentTerm[i],
             msuccess        |-> TRUE,
             mmatchIndex     |-> LastIndex(log[i]),
             mrejectHint     |-> 0,
             mlogTerm        |-> 0,
             msource         |-> i,
             mdest           |-> i])
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

AppendEntries(i, j, range) ==
    AppendEntriesInRangeToPeer("app", i, j, range)

Heartbeat(i, j) ==
    \* heartbeat is equivalent to an append-entry request with 0 entry index 1
    AppendEntriesInRangeToPeer("heartbeat", i, j, <<1,1>>)

SendSnapshot(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ j \in GetConfig(i) \union GetLearners(i)
    \* prevLogIndex not available, must send snapshot instead
    /\ LET prevLogIndex == nextIndex[i][j] - 1 IN
       ~IsAvailable(i, prevLogIndex)
    /\ log[i].snapshotIndex > 0
    /\ LET snapshotHistory == durableState[i].snapshotHistory
       IN Send([mtype          |-> SnapshotRequest,
                mterm          |-> currentTerm[i],
                msnapshotIndex |-> log[i].snapshotIndex,
                msnapshotTerm  |-> log[i].snapshotTerm,
                mhistory       |-> snapshotHistory,
                mconfState     |-> ComputeConfStateFromHistory(snapshotHistory),
                msource        |-> i,
                mdest          |-> j])
    \* Reference: raft.go sendSnapshot() -> pr.BecomeSnapshot()
    /\ progressState' = [progressState EXCEPT ![i][j] = StateSnapshot]
    /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
    /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i][j] = log[i].snapshotIndex]
    /\ nextIndex' = [nextIndex EXCEPT ![i][j] = log[i].snapshotIndex + 1]
    /\ ResetInflights(i, j)
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, historyLog, partitions>>

\* Send snapshot with compaction. Reference: raft.go maybeSendSnapshot()
SendSnapshotWithCompaction(i, j, snapshoti) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ j \in GetConfig(i) \union GetLearners(i)
    /\ snapshoti <= applied[i]
    /\ snapshoti >= log[i].snapshotIndex
    /\ snapshoti >= matchIndex[i][j]
    /\ LET snapshotHistory == IF snapshoti <= durableState[i].snapshotIndex
                              THEN SubSeq(durableState[i].snapshotHistory, 1, snapshoti)
                              ELSE durableState[i].snapshotHistory \o
                                   SubSeq(log[i].entries, 1, snapshoti - log[i].offset + 1)
       IN SendDirect([mtype          |-> SnapshotRequest,
                      mterm          |-> currentTerm[i],
                      msnapshotIndex |-> snapshoti,
                      msnapshotTerm  |-> LogTerm(i, snapshoti),
                      mhistory       |-> snapshotHistory,
                      mconfState     |-> ComputeConfStateFromHistory(snapshotHistory),
                      msource        |-> i,
                      mdest          |-> j])
    /\ progressState' = [progressState EXCEPT ![i][j] = StateSnapshot]
    /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
    /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i][j] = snapshoti]
    /\ nextIndex' = [nextIndex EXCEPT ![i][j] = snapshoti + 1]
    /\ inflights' = [inflights EXCEPT ![i][j] = {}]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, historyLog, partitions>>

\* Test harness snapshot injection: bypasses normal raft path, does NOT modify progress state.
\* Creates snapshot from applied state. Reference: rafttest/interaction_env_handler_send_snapshot.go
ManualSendSnapshot(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ j \in GetConfig(i) \union GetLearners(i)
    /\ applied[i] > 0
    /\ LET snapshotHistory == IF applied[i] <= durableState[i].snapshotIndex
                              THEN SubSeq(durableState[i].snapshotHistory, 1, applied[i])
                              ELSE durableState[i].snapshotHistory \o
                                   SubSeq(log[i].entries, 1, applied[i] - log[i].offset + 1)
       IN Send([mtype          |-> SnapshotRequest,
                mterm          |-> currentTerm[i],
                msnapshotIndex |-> applied[i],
                msnapshotTerm  |-> LogTerm(i, applied[i]),
                mhistory       |-> snapshotHistory,
                mconfState     |-> ComputeConfStateFromHistory(snapshotHistory),
                msource        |-> i,
                mdest          |-> j])
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Candidate i transitions to leader.
\* @type: Int => Bool;
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ IF IsJointConfig(i) THEN
           /\ (votesGranted[i] \cap GetConfig(i)) \in Quorum(GetConfig(i))
           /\ (votesGranted[i] \cap GetOutgoingConfig(i)) \in Quorum(GetOutgoingConfig(i))
       ELSE
           votesGranted[i] \in Quorum(GetConfig(i))
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> IF j = i THEN LastIndex(log[i]) ELSE 0]]
    \* Reference: raft.go becomeLeader() -> reset()
    /\ progressState' = [progressState EXCEPT ![i] =
                            [j \in Server |-> IF j = i THEN StateReplicate ELSE StateProbe]]
    /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i] =
                            [j \in Server |-> FALSE]]
    /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i] =
                            [j \in Server |-> 0]]
    /\ nextIndex' = [nextIndex EXCEPT ![i] =
                            [j \in Server |-> LastIndex(log[i]) + 1]]
    /\ inflights' = [inflights EXCEPT ![i] =
                            [j \in Server |-> {}]]
    \* Conservatively delay future proposals until pending entries committed (raft.go:955-960)
    /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = LastIndex(log[i])]
    /\ UNCHANGED <<messageVars, currentTerm, votedFor, candidateVars, logVars, configVars, durableState, partitions>>
    
Replicate(i, v, t) == 
    /\ t \in {ValueEntry, ConfigEntry}
    /\ state[i] = Leader
    /\ LET entry == [term  |-> currentTerm[i],
                     type  |-> t,
                     value |-> v]
           newLog == [log[i] EXCEPT !.entries = Append(@, entry)]
       IN  /\ log' = [log EXCEPT ![i] = newLog]
           /\ historyLog' = [historyLog EXCEPT ![i] = Append(@, entry)]

\* Leader i receives a client request to add v to the log.
\* @type: (Int, Int) => Bool;
ClientRequest(i, v) ==
    /\ Replicate(i, [val |-> v], ValueEntry)
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, leaderVars, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* ClientRequest variant that also sends MsgAppResp (for trace validation).
ClientRequestAndSend(i, v) ==
    /\ Replicate(i, [val |-> v], ValueEntry)
    /\ Send([mtype       |-> AppendEntriesResponse,
             msubtype    |-> "app",
             mterm       |-> currentTerm[i],
             msuccess    |-> TRUE,
             mmatchIndex |-> LastIndex(log'[i]),
             mrejectHint |-> 0,
             mlogTerm    |-> 0,
             msource     |-> i,
             mdest       |-> i])
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, commitIndex, applied, configVars, durableState, progressVars, partitions>>


\* Leader replicates an implicit entry (self-message response in trace).
\* In joint config with auto-leave: creates LeaveJoint entry. Otherwise: normal value entry.
\* Also supports DisableConfChangeValidation (Reference: raft.go:1762-1770)
ReplicateImplicitEntry(i) ==
    /\ state[i] = Leader
    /\ LET isJoint == IsJointConfig(i)
           pendingIdx == pendingConfChangeIndex[i]
           hasPendingEnterJoint ==
               /\ pendingIdx > 0
               /\ pendingIdx > log[i].offset
               /\ pendingIdx <= LastIndex(log[i])
               /\ LET entry == LogEntry(i, pendingIdx)
                  IN /\ entry.type = ConfigEntry
                     /\ "enterJoint" \in DOMAIN entry.value
                     /\ entry.value.enterJoint = TRUE
           autoLeaveCondition ==
               /\ isJoint
               /\ config[i].autoLeave = TRUE
               /\ applied[i] >= pendingConfChangeIndex[i]
           \* Reference: raft.go:1334-1338 - bypasses "not in joint state" check
           disableValidationCondition ==
               /\ DisableConfChangeValidation
               /\ ~isJoint
               /\ hasPendingEnterJoint
           shouldCreateLeaveJoint == autoLeaveCondition \/ disableValidationCondition
           \* disableValidation: learners from pending entry (not yet applied)
           leaveJointLearners == IF disableValidationCondition
                                 THEN LogEntry(i, pendingIdx).value.learners
                                 ELSE GetLearners(i)
       IN
       /\ (isJoint => (config[i].autoLeave = TRUE /\ applied[i] >= pendingConfChangeIndex[i]))
       /\ LET entryType == IF shouldCreateLeaveJoint THEN ConfigEntry ELSE ValueEntry
              entryValue == IF shouldCreateLeaveJoint
                            THEN [leaveJoint |-> TRUE, newconf |-> GetConfig(i), learners |-> leaveJointLearners]
                            ELSE [val |-> 0]
          IN
          /\ Replicate(i, entryValue, entryType)
          /\ Send([mtype       |-> AppendEntriesResponse,
                   msubtype    |-> "app",
                   mterm       |-> currentTerm[i],
                   msuccess    |-> TRUE,
                   mmatchIndex |-> LastIndex(log'[i]),
                   mrejectHint |-> 0,
                   mlogTerm    |-> 0,
                   msource     |-> i,
                   mdest       |-> i])
          /\ IF shouldCreateLeaveJoint
             THEN pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = LastIndex(log'[i])]
             ELSE UNCHANGED pendingConfChangeIndex
    /\ UNCHANGED <<messages, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* Leader i advances its commitIndex using current applied config's quorum.
\* Reference: raft.go maybeCommit() -> trk.Committed()
\* @type: Int => Bool;
AdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ LET AllVoters == GetConfig(i) \union GetOutgoingConfig(i)
           Agree(index) == {k \in AllVoters : matchIndex[i][k] >= index}
           logSize == LastIndex(log[i])
           IsCommitted(index) ==
               IF IsJointConfig(i) THEN
                   /\ (Agree(index) \cap GetConfig(i)) \in Quorum(GetConfig(i))
                   /\ (Agree(index) \cap GetOutgoingConfig(i)) \in Quorum(GetOutgoingConfig(i))
               ELSE
                   Agree(index) \in Quorum(GetConfig(i))

           agreeIndexes == {index \in (commitIndex[i]+1)..logSize : IsCommitted(index)}
           newCommitIndex ==
              IF /\ agreeIndexes /= {}
                 /\ LogTerm(i, Max(agreeIndexes)) = currentTerm[i]
              THEN
                  Max(agreeIndexes)
              ELSE
                  commitIndex[i]
       IN
        /\ CommitTo(i, newCommitIndex)
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, leaderVars, log, applied, configVars, durableState, progressVars, historyLog, partitions>>

    
\* Leader i adds a new server j or promote learner j
AddNewServer(i, j) ==
    /\ state[i] = Leader
    /\ j \notin GetConfig(i)
    /\ ~IsJointConfig(i)
    /\ IF pendingConfChangeIndex[i] = 0 THEN
            /\ Replicate(i, [newconf |-> GetConfig(i) \union {j}, learners |-> GetLearners(i)], ConfigEntry)
            /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i]=LastIndex(log'[i])]
       ELSE
            /\ Replicate(i, <<>>, ValueEntry)
            /\ UNCHANGED <<pendingConfChangeIndex>>
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* Leader i adds a leaner j to the cluster.
AddLearner(i, j) ==
    /\ state[i] = Leader
    /\ j \notin GetConfig(i) \union GetLearners(i)
    /\ ~IsJointConfig(i)
    /\ IF pendingConfChangeIndex[i] = 0 THEN
            /\ Replicate(i, [newconf |-> GetConfig(i), learners |-> GetLearners(i) \union {j}], ConfigEntry)
            /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i]=LastIndex(log'[i])]
       ELSE
            /\ Replicate(i, <<>>, ValueEntry)
            /\ UNCHANGED <<pendingConfChangeIndex>>
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* Leader i removes a server j (possibly itself) from the cluster.
DeleteServer(i, j) ==
    /\ state[i] = Leader
    /\ j \in GetConfig(i) \union GetLearners(i)
    /\ ~IsJointConfig(i)
    /\ IF pendingConfChangeIndex[i] = 0 THEN
            /\ Replicate(i, [newconf |-> GetConfig(i) \ {j}, learners |-> GetLearners(i) \ {j}], ConfigEntry)
            /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i]=LastIndex(log'[i])]
       ELSE
            /\ Replicate(i, <<>>, ValueEntry)
            /\ UNCHANGED <<pendingConfChangeIndex>>
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* Leader i proposes a configuration change. Reference: confchange/confchange.go
ChangeConf(i) ==
    /\ state[i] = Leader
    /\ IF pendingConfChangeIndex[i] <= applied[i] THEN
            \E newVoters \in SUBSET Server, newLearners \in SUBSET Server, enterJoint \in {TRUE, FALSE}:
                /\ ~IsJointConfig(i)
                \* Simple change: at most one voter changed
                /\ (enterJoint = FALSE) =>
                   Cardinality((GetConfig(i) \ newVoters) \union (newVoters \ GetConfig(i))) <= 1
                /\ newVoters \cap newLearners = {}
                /\ (enterJoint = TRUE) => (GetConfig(i) \cap newLearners = {})
                /\ newVoters /= {}
                /\ Replicate(i, [newconf |-> newVoters, learners |-> newLearners, enterJoint |-> enterJoint, oldconf |-> GetConfig(i)], ConfigEntry)
                /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i]=LastIndex(log'[i])]
       ELSE
            /\ Replicate(i, <<>>, ValueEntry)
            /\ UNCHANGED <<pendingConfChangeIndex>>
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* ChangeConf variant that also sends MsgAppResp (for trace validation).
ChangeConfAndSend(i) ==
    /\ state[i] = Leader
    /\ IF pendingConfChangeIndex[i] = 0 THEN
            \E newVoters \in SUBSET Server, newLearners \in SUBSET Server, enterJoint \in {TRUE, FALSE}:
                /\ ~IsJointConfig(i)
                /\ (enterJoint = FALSE) =>
                   Cardinality((GetConfig(i) \ newVoters) \union (newVoters \ GetConfig(i))) <= 1
                /\ newVoters \cap newLearners = {}
                /\ (enterJoint = TRUE) => (GetConfig(i) \cap newLearners = {})
                /\ newVoters /= {}
                /\ Replicate(i, [newconf |-> newVoters, learners |-> newLearners, enterJoint |-> enterJoint, oldconf |-> GetConfig(i)], ConfigEntry)
                /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i]=LastIndex(log'[i])]
                /\ Send([mtype       |-> AppendEntriesResponse,
                         msubtype    |-> "app",
                         mterm       |-> currentTerm[i],
                         msuccess    |-> TRUE,
                         mmatchIndex |-> LastIndex(log'[i]),
                         mrejectHint |-> 0,
                         mlogTerm    |-> 0,
                         msource     |-> i,
                         mdest       |-> i])
       ELSE
            /\ Replicate(i, <<>>, ValueEntry)
            /\ UNCHANGED <<pendingConfChangeIndex>>
            /\ Send([mtype       |-> AppendEntriesResponse,
                     msubtype    |-> "app",
                     mterm       |-> currentTerm[i],
                     msuccess    |-> TRUE,
                     mmatchIndex |-> LastIndex(log'[i]),
                     mrejectHint |-> 0,
                     mlogTerm    |-> 0,
                     msource     |-> i,
                     mdest       |-> i])
    /\ UNCHANGED <<messages, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* Apply the next committed config entry in order
\* Reference: etcd processes CommittedEntries sequentially (for _, entry := range rd.CommittedEntries)
\* and calls ApplyConfChange for each config entry as it's encountered
\* This ensures configs are applied one at a time, in log order
ApplySimpleConfChange(i) ==
    \* Find config entries that are committed but not yet applied
    LET validIndices == {x \in Max({log[i].offset, appliedConfigIndex[i]+1})..commitIndex[i] :
                          LogEntry(i, x).type = ConfigEntry}
    IN
    /\ validIndices /= {}
    /\ LET k == Min(validIndices)  \* Apply the NEXT config entry, not MAX
           oldConfig == GetConfig(i) \cup GetOutgoingConfig(i) \cup GetLearners(i)
           newConfigFn == ApplyConfigUpdate(i, k)
           newConfig == newConfigFn[i].jointConfig[1] \cup newConfigFn[i].jointConfig[2] \cup newConfigFn[i].learners
           addedNodes == newConfig \ oldConfig
       IN
        /\ k > 0
        /\ k <= commitIndex[i]
        /\ config' = newConfigFn
        /\ appliedConfigIndex' = [appliedConfigIndex EXCEPT ![i] = k]  \* Track applied config
        /\ IF state[i] = Leader /\ pendingConfChangeIndex[i] = k THEN
            /\ reconfigCount' = reconfigCount + 1
            /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = 0]
           ELSE UNCHANGED <<reconfigCount, pendingConfChangeIndex>>
        /\ IF state[i] = Leader /\ addedNodes # {}
           THEN /\ nextIndex' = [nextIndex EXCEPT ![i] =
                       [j \in Server |-> IF j \in addedNodes THEN Max({LastIndex(log[i]), 1}) ELSE nextIndex[i][j]]]
                \* Reference: confchange/confchange.go makeVoter() - only init Match=0 for truly new nodes
                \* Existing nodes (including leader itself) keep their Match value (pr != nil check)
                /\ matchIndex' = [matchIndex EXCEPT ![i] =
                       [j \in Server |-> IF j \in addedNodes /\ j # i THEN 0 ELSE matchIndex[i][j]]]
                /\ progressState' = [progressState EXCEPT ![i] =
                       [j \in Server |-> IF j \in addedNodes /\ j # i THEN StateProbe ELSE progressState[i][j]]]
                /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i] =
                       [j \in Server |-> IF j \in addedNodes THEN FALSE ELSE msgAppFlowPaused[i][j]]]
                /\ inflights' = [inflights EXCEPT ![i] =
                       [j \in Server |-> IF j \in addedNodes THEN {} ELSE inflights[i][j]]]
                /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i] =
                       [j \in Server |-> IF j \in addedNodes THEN 0 ELSE pendingSnapshot[i][j]]]
           ELSE /\ UNCHANGED progressVars
                /\ UNCHANGED matchIndex
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, logVars, durableState, historyLog, partitions>>

\* Leave joint consensus - transition from joint config to single config
\* This action is called when applying a LeaveJoint config entry (via ApplySimpleConfChange)
\* Reference: confchange/confchange.go:94-121 LeaveJoint()
\* LeaveJoint preserves Learners and moves LearnersNext into Learners
LeaveJoint(i) ==
    /\ IsJointConfig(i)
    /\ LET newVoters == GetConfig(i)  \* Keep incoming config (jointConfig[1])
       \* Preserve learners (Reference: confchange.go:103-107 - LearnersNext are added to Learners)
       IN config' = [config EXCEPT ![i] = [learners |-> GetLearners(i), jointConfig |-> <<newVoters, {}>>, autoLeave |-> FALSE]]
    /\ IF state[i] = Leader /\ pendingConfChangeIndex[i] > 0 THEN
        /\ reconfigCount' = reconfigCount + 1
        /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = 0]
       ELSE UNCHANGED <<reconfigCount, pendingConfChangeIndex>>
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, logVars, durableState, progressVars, appliedConfigIndex, partitions>>

\* Leader proposes LeaveJoint entry when autoLeave=TRUE and config entry is applied
\* Reference: raft.go:745-760 - AutoLeave mechanism
\* When r.trk.Config.AutoLeave && newApplied >= r.pendingConfIndex && r.state == StateLeader,
\* the leader automatically proposes an empty ConfChangeV2 to leave joint config.
\* This empty ConfChangeV2 must be committed with joint config's two quorums before applying.
ProposeLeaveJoint(i) ==
    /\ state[i] = Leader
    /\ IsJointConfig(i)
    /\ config[i].autoLeave = TRUE
    /\ pendingConfChangeIndex[i] = 0  \* Previous config change has been applied
    \* Propose a LeaveJoint config entry - represented as ConfigEntry with leaveJoint=TRUE
    \* This entry must be committed with joint quorum before being applied
    \* Reference: confchange.go:103-107 - LeaveJoint preserves Learners and adds LearnersNext
    /\ Replicate(i, [leaveJoint |-> TRUE, newconf |-> GetConfig(i), learners |-> GetLearners(i)], ConfigEntry)
    /\ pendingConfChangeIndex' = [pendingConfChangeIndex EXCEPT ![i] = LastIndex(log'[i])]
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, matchIndex, commitIndex, applied, configVars, durableState, progressVars, partitions>>

\* Apply configuration from snapshot
\* When a follower receives a snapshot, it applies the config directly
\* Use persisted snapshotHistory instead of ghost variable historyLog
\* Reference: confchange/restore.go - Restore() uses ConfState which contains Voters, Learners, VotersOutgoing, AutoLeave
ApplySnapshotConfChange(i, newVoters) ==
    \* Find the last config entry in persisted snapshotHistory to determine joint config
    LET snapshotHist == durableState[i].snapshotHistory
        configIndices == {k \in 1..Len(snapshotHist) : snapshotHist[k].type = ConfigEntry}
        lastConfigIdx == IF configIndices /= {} THEN Max(configIndices) ELSE 0
        entry == IF lastConfigIdx > 0 THEN snapshotHist[lastConfigIdx] ELSE [value |-> [newconf |-> {}, learners |-> {}]]
        \* Check if this is a leaveJoint entry
        isLeaveJoint == "leaveJoint" \in DOMAIN entry.value /\ entry.value.leaveJoint = TRUE
        \* Check if last config entry has enterJoint=TRUE
        hasEnterJoint == ~isLeaveJoint /\ "enterJoint" \in DOMAIN entry.value
        enterJoint == IF hasEnterJoint THEN entry.value.enterJoint ELSE FALSE
        hasOldconf == enterJoint /\ "oldconf" \in DOMAIN entry.value
        oldconf == IF hasOldconf THEN entry.value.oldconf ELSE {}
        \* Read learners from entry (Reference: confchange/restore.go:82-87)
        hasLearners == lastConfigIdx > 0 /\ "learners" \in DOMAIN entry.value
        newLearners == IF hasLearners THEN entry.value.learners ELSE {}
        \* AutoLeave is TRUE when entering joint config (snapshot may contain joint config)
        \* For leaveJoint, autoLeave should be FALSE
        newAutoLeave == ~isLeaveJoint /\ enterJoint /\ oldconf /= {}
    IN
    /\ config' = [config EXCEPT ![i] = [learners |-> newLearners, jointConfig |-> <<newVoters, oldconf>>, autoLeave |-> newAutoLeave]]
    /\ appliedConfigIndex' = [appliedConfigIndex EXCEPT ![i] = lastConfigIdx]
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, leaderVars, logVars, durableState, progressVars, reconfigCount, pendingConfChangeIndex, partitions>>

\* Apply committed entries to state machine
\* Reference: raft/node.go - application layer retrieves CommittedEntries from Ready()
\*            and calls Advance() after applying them
\* This advances 'applied' from its current value up to any point <= commitIndex
\* Invariant: applied <= commitIndex (AppliedBoundInv)
ApplyEntries(i, newApplied) ==
    /\ newApplied > applied[i]           \* Must make progress
    /\ newApplied <= commitIndex[i]      \* Cannot apply beyond committed
    /\ applied' = [applied EXCEPT ![i] = newApplied]
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, leaderVars, log, commitIndex, configVars, durableState, progressVars, historyLog, partitions>>

Ready(i) ==
    /\ PersistState(i)
    /\ SendPendingMessages(i)
    /\ UNCHANGED <<msgSeqCounter, serverVars, leaderVars, candidateVars, logVars, configVars, progressVars, historyLog, partitions>>

BecomeFollowerOfTerm(i, t) ==
    /\ currentTerm'    = [currentTerm EXCEPT ![i] = t]
    /\ state'          = [state       EXCEPT ![i] = Follower]
    /\ IF currentTerm[i] # t THEN  
            votedFor' = [votedFor    EXCEPT ![i] = Nil]
       ELSE 
            UNCHANGED <<votedFor>>

StepDownToFollower(i) ==
    /\ state[i] \in {Leader, Candidate}
    /\ BecomeFollowerOfTerm(i, currentTerm[i])
    /\ UNCHANGED <<messageVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Clear MsgAppFlowPaused on successful response. Reference: progress.go MaybeUpdate()
ClearMsgAppFlowPausedOnUpdate(i, j) ==
    msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]

----
\* Message handlers
\* i = recipient, j = sender, m = message

\* Server i receives a RequestVote request from server j with
\* m.mterm <= currentTerm[i].
\* @type: (Int, Int, RVREQT) => Bool;
HandleRequestVoteRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
                    /\ m.mlastLogIndex >= LastIndex(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
          \/ ~grant /\ UNCHANGED votedFor
       /\ Reply([mtype        |-> RequestVoteResponse,
                 mterm        |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 msource      |-> i,
                 mdest        |-> j],
                 m)
       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Server i receives a RequestVote response from server j.
\* @type: (Int, Int, RVRESPT) => Bool;
HandleRequestVoteResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ j = i \/ j \in (GetConfig(i) \union GetOutgoingConfig(i) \union GetLearners(i))
    /\ votesResponded' = [votesResponded EXCEPT ![i] =
                              votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                  votesGranted[i] \cup {j}]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* @type: (Int, Int, AEREQT, Bool) => Bool;
RejectAppendEntriesRequest(i, j, m, logOk) ==
    /\ \/ m.mterm < currentTerm[i]
       \/ /\ m.mterm = currentTerm[i]
          /\ state[i] = Follower
          /\ \lnot logOk
    \* Rejection uses findConflictByTerm for fast backtracking
    /\ LET hintIndex == Min({m.mprevLogIndex, LastIndex(log[i])})
           rejectHint == FindConflictByTerm(i, hintIndex, m.mprevLogTerm)
           logTerm == LogTerm(i, rejectHint)
       IN Reply([mtype           |-> AppendEntriesResponse,
                 msubtype        |-> "app",
                 mterm           |-> currentTerm[i],
                 msuccess        |-> FALSE,
                 mmatchIndex     |-> m.mprevLogIndex,
                 mrejectHint     |-> rejectHint,
                 mlogTerm        |-> logTerm,
                 msource         |-> i,
                 mdest           |-> j],
                 m)
    /\ UNCHANGED <<serverVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* @type: (Int, MSG) => Bool;
ReturnToFollowerState(i, m) ==
    /\ m.mterm = currentTerm[i]
    /\ state[i] = Candidate
    /\ state' = [state EXCEPT ![i] = Follower]
    /\ UNCHANGED <<messageVars, currentTerm, votedFor, logVars, configVars, durableState, progressVars, historyLog, partitions>> 

HasNoConflict(i, index, ents) ==
    /\ index <= LastIndex(log[i]) + 1
    /\ \A k \in 1..Len(ents): index + k - 1 <= LastIndex(log[i]) => LogTerm(i, index+k-1) = ents[k].term

\* Find first conflicting entry index (0 if none). Reference: log.go findConflict
\* @type: (Int, Int, Seq(ENTRY)) => Int;
FindFirstConflict(i, index, ents) ==
    LET conflicting == {k \in 1..Len(ents):
            /\ index + k - 1 <= LastIndex(log[i])
            /\ LogTerm(i, index + k - 1) /= ents[k].term}
    IN IF conflicting = {} THEN 0 ELSE index + Min(conflicting) - 1

\* @type: (Int, Int, Int, AEREQT) => Bool;
AppendEntriesAlreadyDone(i, j, index, m) ==
    /\ \/ index <= commitIndex[i]
       \/ /\ index > commitIndex[i]
          /\ \/ m.mentries = << >>
             \/ /\ m.mentries /= << >>
                /\ m.mprevLogIndex + Len(m.mentries) <= LastIndex(log[i])
                /\ HasNoConflict(i, index, m.mentries)          
    /\ IF index <= commitIndex[i] THEN 
            IF m.msubtype = "heartbeat" THEN CommitTo(i, m.mcommitIndex) ELSE UNCHANGED commitIndex
       ELSE 
            CommitTo(i, Min({m.mcommitIndex, m.mprevLogIndex+Len(m.mentries)}))
    /\ Reply([  mtype           |-> AppendEntriesResponse,
                msubtype        |-> m.msubtype,
                mterm           |-> currentTerm[i],
                msuccess        |-> TRUE,
                mmatchIndex     |-> IF m.msubtype = "heartbeat" \/ index > commitIndex[i] THEN m.mprevLogIndex+Len(m.mentries) ELSE commitIndex[i],
                mrejectHint     |-> 0,
                mlogTerm        |-> 0,
                msource         |-> i,
                mdest           |-> j],
                m)
    /\ UNCHANGED <<serverVars, log, applied, configVars, durableState, progressVars, historyLog, partitions>>

\* @type: (Int, Int, Int, AEREQT) => Bool;
ConflictAppendEntriesRequest(i, j, index, m) ==
    /\ m.mentries /= << >>
    /\ index > commitIndex[i]
    /\ ~HasNoConflict(i, index, m.mentries)
    \* Find conflict, truncate, and append (Reference: log.go maybeAppend + findConflict)
    /\ LET ci == FindFirstConflict(i, index, m.mentries)
           entsOffset == ci - index + 1
           newEntries == SubSeq(m.mentries, entsOffset, Len(m.mentries))
           keepUntil == ci - log[i].offset
       IN /\ ci > commitIndex[i]
          /\ log' = [log EXCEPT ![i].entries = SubSeq(@, 1, keepUntil) \o newEntries]
          /\ historyLog' = [historyLog EXCEPT ![i] = SubSeq(@, 1, ci - 1) \o newEntries]
    /\ CommitTo(i, Min({m.mcommitIndex, m.mprevLogIndex + Len(m.mentries)}))
    /\ Reply([mtype           |-> AppendEntriesResponse,
              msubtype        |-> m.msubtype,
              mterm           |-> currentTerm[i],
              msuccess        |-> TRUE,
              mmatchIndex     |-> m.mprevLogIndex + Len(m.mentries),
              mrejectHint     |-> 0,
              mlogTerm        |-> 0,
              msource         |-> i,
              mdest           |-> j],
              m)
    /\ UNCHANGED <<serverVars, applied, durableState, progressVars, partitions>>

\* @type: (Int, Int, Int, AEREQT) => Bool;
NoConflictAppendEntriesRequest(i, j, index, m) ==
    /\ m.mentries /= << >>
    /\ index > commitIndex[i]
    /\ HasNoConflict(i, index, m.mentries)
    /\ m.mprevLogIndex + Len(m.mentries) > LastIndex(log[i])
    /\ LET newEntries == SubSeq(m.mentries, LastIndex(log[i])-index+2, Len(m.mentries))
       IN /\ log' = [log EXCEPT ![i].entries = @ \o newEntries]
          /\ historyLog' = [historyLog EXCEPT ![i] = @ \o newEntries]
    /\ CommitTo(i, Min({m.mcommitIndex, m.mprevLogIndex + Len(m.mentries)}))
    /\ Reply([mtype           |-> AppendEntriesResponse,
              msubtype        |-> m.msubtype,
              mterm           |-> currentTerm[i],
              msuccess        |-> TRUE,
              mmatchIndex     |-> m.mprevLogIndex + Len(m.mentries),
              mrejectHint     |-> 0,
              mlogTerm        |-> 0,
              msource         |-> i,
              mdest           |-> j],
              m)
    /\ UNCHANGED <<serverVars, applied, durableState, progressVars, partitions>>

\* @type: (Int, Int, Bool, AEREQT) => Bool;
AcceptAppendEntriesRequest(i, j, logOk, m) ==
    \* accept request
    /\ m.mterm = currentTerm[i]
    /\ state[i] = Follower
    /\ logOk
    /\ LET index == m.mprevLogIndex + 1
       IN \/ AppendEntriesAlreadyDone(i, j, index, m)
          \/ ConflictAppendEntriesRequest(i, j, index, m)
          \/ NoConflictAppendEntriesRequest(i, j, index, m)

\* Server i receives an AppendEntries request from server j with
\* m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
\* implementations could safely accept more by treating them the same as
\* multiple independent requests of 1 entry.
\* @type: (Int, Int, AEREQT) => Bool;
HandleAppendEntriesRequest(i, j, m) ==
    LET logOk == \/ m.mprevLogIndex = 0
                 \/ /\ m.mprevLogIndex > 0
                    /\ m.mprevLogIndex <= LastIndex(log[i])
                    /\ m.mprevLogTerm = LogTerm(i, m.mprevLogIndex)
    IN 
       /\ m.mterm <= currentTerm[i]
       /\ \/ RejectAppendEntriesRequest(i, j, m, logOk)
          \/ ReturnToFollowerState(i, m)
          \/ AcceptAppendEntriesRequest(i, j, logOk, m)
       /\ UNCHANGED <<candidateVars, leaderVars, configVars, durableState, progressVars, partitions>>

\* Server i receives an AppendEntries response from server j.
\* @type: (Int, Int, AERESPT) => Bool;
HandleAppendEntriesResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ m.msubtype /= "heartbeat"  \* Heartbeat responses handled by HandleHeartbeatResponse
    /\ \/ /\ m.msuccess \* successful
          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = Max({@, m.mmatchIndex})]
          /\ UNCHANGED <<pendingConfChangeIndex>>
          /\ FreeInflightsLE(i, j, m.mmatchIndex)
          \* State transition logic (Reference: raft.go handleAppendEntriesResponse())
          /\ LET maybeUpdated == m.mmatchIndex > matchIndex[i][j]
                 alreadyMatched == m.mmatchIndex = matchIndex[i][j]
                 newMatchIndex == Max({matchIndex[i][j], m.mmatchIndex})
                 \* Check if follower caught up enough to resume from snapshot
                 canResumeFromSnapshot == \/ newMatchIndex + 1 >= log[i].offset
                                          \/ newMatchIndex + 1 >= pendingSnapshot[i][j]
             IN CASE \* StateProbe -> StateReplicate
                     progressState[i][j] = StateProbe
                     /\ (maybeUpdated \/ alreadyMatched) ->
                        /\ progressState' = [progressState EXCEPT ![i][j] = StateReplicate]
                        /\ nextIndex' = [nextIndex EXCEPT ![i][j] = Max({@, m.mmatchIndex + 1})]
                        /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
                        /\ UNCHANGED pendingSnapshot
                  \* StateSnapshot -> StateReplicate (if caught up)
                  [] progressState[i][j] = StateSnapshot
                     /\ maybeUpdated
                     /\ canResumeFromSnapshot ->
                        /\ progressState' = [progressState EXCEPT ![i][j] = StateReplicate]
                        /\ nextIndex' = [nextIndex EXCEPT ![i][j] = m.mmatchIndex + 1]
                        /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
                        /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i][j] = 0]
                  \* Other: only clear MsgAppFlowPaused if matchIndex actually updated
                  [] OTHER ->
                        /\ UNCHANGED <<progressState, pendingSnapshot>>
                        /\ nextIndex' = [nextIndex EXCEPT ![i][j] = Max({@, m.mmatchIndex + 1})]
                        /\ IF maybeUpdated
                           THEN msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
                           ELSE UNCHANGED msgAppFlowPaused
       \/ /\ \lnot m.msuccess \* MaybeDecrTo (progress.go:226-252)
          /\ LET rejected == m.mmatchIndex
                 matchHint == m.mrejectHint
             IN IF progressState[i][j] = StateReplicate
                THEN IF rejected <= matchIndex[i][j]
                     THEN /\ UNCHANGED <<leaderVars, progressVars, partitions>>  \* stale
                     ELSE \* transition to Probe
                          /\ progressState' = [progressState EXCEPT ![i][j] = StateProbe]
                          /\ nextIndex' = [nextIndex EXCEPT ![i][j] = matchIndex[i][j] + 1]
                          /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
                          /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i][j] = 0]
                          /\ inflights' = [inflights EXCEPT ![i][j] = {}]
                          /\ UNCHANGED <<matchIndex, pendingConfChangeIndex>>
                ELSE IF nextIndex[i][j] - 1 /= rejected
                     THEN \* stale, just unpause
                          /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
                          /\ UNCHANGED <<progressState, pendingSnapshot, inflights, nextIndex, matchIndex, pendingConfChangeIndex>>
                     ELSE \* Leader-side findConflictByTerm optimization
                          LET leaderMatchIdx == FindConflictByTerm(i, matchHint, m.mlogTerm)
                              newNext == IF leaderMatchIdx > matchIndex[i][j]
                                         THEN leaderMatchIdx + 1
                                         ELSE Max({Min({rejected, matchHint + 1}), matchIndex[i][j] + 1})
                          IN
                          /\ nextIndex' = [nextIndex EXCEPT ![i][j] = newNext]
                          /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
                          /\ UNCHANGED <<progressState, pendingSnapshot, inflights, matchIndex, pendingConfChangeIndex>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, logVars, configVars, durableState, partitions>>

\* Server i receives a heartbeat response from server j. Only clears flow control.
HandleHeartbeatResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ m.msubtype = "heartbeat"
    /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = FALSE]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, configVars, durableState,
                   matchIndex, nextIndex, progressState, pendingSnapshot, inflights, partitions>>

\* Compacts log of server i up to newStart (exclusive). Reference: storage.go Compact()
CompactLog(i, newStart) ==
    /\ newStart > log[i].offset
    /\ newStart <= applied[i] + 1
    /\ LET compactedEntries == SubSeq(log[i].entries, 1, newStart - log[i].offset)
       IN
       /\ log' = [log EXCEPT ![i] = [
             offset  |-> newStart,
             entries |-> SubSeq(@.entries, newStart - @.offset + 1, Len(@.entries)),
             snapshotIndex |-> newStart - 1,
             snapshotTerm  |-> LogTerm(i, newStart - 1)
          ]]
       /\ durableState' = [durableState EXCEPT ![i] = [
             @ EXCEPT !.snapshotHistory = @ \o compactedEntries,
                      !.snapshotIndex = newStart - 1,
                      !.snapshotTerm = LogTerm(i, newStart - 1),
                      !.entries = SubSeq(log[i].entries, newStart - log[i].offset + 1, Len(log[i].entries))
          ]]
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, leaderVars, commitIndex, applied, configVars, progressVars, historyLog, partitions>>

\* Server i receives a SnapshotRequest. Reference: raft.go restore()
HandleSnapshotRequest(i, j, m) ==
    /\ m.mterm <= currentTerm[i]
    /\ IF m.mterm < currentTerm[i] THEN
           \* Stale term: ignore
           /\ Discard(m)
           /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>
       ELSE IF m.msnapshotIndex <= commitIndex[i] THEN
           \* Stale snapshot (already committed)
           /\ Reply([mtype       |-> AppendEntriesResponse,
                     msubtype    |-> "app",
                     mterm       |-> currentTerm[i],
                     msuccess    |-> TRUE,
                     mmatchIndex |-> commitIndex[i],
                     mrejectHint |-> 0,
                     mlogTerm    |-> 0,
                     msource     |-> i,
                     mdest       |-> j],
                     m)
           /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>
       ELSE IF LogTerm(i, m.msnapshotIndex) = m.msnapshotTerm THEN
           \* Fast-forward: log already contains this snapshot's index/term
           /\ commitIndex' = [commitIndex EXCEPT ![i] = m.msnapshotIndex]
           /\ Reply([mtype       |-> AppendEntriesResponse,
                     msubtype    |-> "app",
                     mterm       |-> currentTerm[i],
                     msuccess    |-> TRUE,
                     mmatchIndex |-> m.msnapshotIndex,
                     mrejectHint |-> 0,
                     mlogTerm    |-> 0,
                     msource     |-> i,
                     mdest       |-> j],
                     m)
           /\ UNCHANGED <<serverVars, candidateVars, leaderVars, log, applied, configVars, durableState, progressVars, historyLog, partitions>>
       ELSE
           \* Actual restore: wipe log and restore config atomically
           LET confState == m.mconfState
               configIndices == {k \in 1..Len(m.mhistory) : m.mhistory[k].type = ConfigEntry}
               lastConfigIdx == IF configIndices /= {} THEN Max(configIndices) ELSE 0
           IN
           /\ log' = [log EXCEPT ![i] = [
                 offset  |-> m.msnapshotIndex + 1,
                 entries |-> <<>>,
                 snapshotIndex |-> m.msnapshotIndex,
                 snapshotTerm  |-> m.msnapshotTerm
              ]]
           /\ historyLog' = [historyLog EXCEPT ![i] = m.mhistory]
           /\ commitIndex' = [commitIndex EXCEPT ![i] = m.msnapshotIndex]
           \* applied updated later via ApplyEntries action
           /\ UNCHANGED applied
           /\ config' = [config EXCEPT ![i] = [
                 jointConfig |-> <<confState.voters, confState.outgoing>>,
                 learners    |-> confState.learners,
                 autoLeave   |-> confState.autoLeave
              ]]
           /\ appliedConfigIndex' = [appliedConfigIndex EXCEPT ![i] = lastConfigIdx]
           /\ durableState' = [durableState EXCEPT ![i] = [
                 @ EXCEPT !.log = m.msnapshotIndex,
                          !.entries = <<>>,
                          !.snapshotIndex = m.msnapshotIndex,
                          !.snapshotTerm = m.msnapshotTerm,
                          !.snapshotHistory = m.mhistory,
                          !.commitIndex = m.msnapshotIndex,
                          !.config = config'[i]
              ]]
           /\ Reply([mtype       |-> AppendEntriesResponse,
                     msubtype    |-> "snapshot",
                     mterm       |-> currentTerm[i],
                     msuccess    |-> TRUE,
                     mmatchIndex |-> m.msnapshotIndex,
                     mrejectHint |-> 0,
                     mlogTerm    |-> 0,
                     msource     |-> i,
                     mdest       |-> j],
                     m)
           /\ UNCHANGED <<serverVars, candidateVars, leaderVars, progressVars, reconfigCount, pendingConfChangeIndex, partitions>>

\* Handle ReportUnreachable from application layer
\* Reference: raft.go:1624-1632
\* Application reports that a peer is unreachable, causing StateReplicate -> StateProbe
\* Reference: tracker/progress.go:121-126 ResetState() clears Inflights
\* @type: (Int, Int) => Bool;
ReportUnreachable(i, j) ==
    /\ state[i] = Leader
    /\ i # j
    /\ IF progressState[i][j] = StateReplicate
       THEN /\ progressState' = [progressState EXCEPT ![i][j] = StateProbe]
            /\ inflights' = [inflights EXCEPT ![i][j] = {}]
       ELSE UNCHANGED <<progressState, inflights>>
    /\ UNCHANGED <<serverVars, candidateVars, messageVars, logVars, configVars,
                   durableState, leaderVars, nextIndex, pendingSnapshot,
                   msgAppFlowPaused, historyLog, partitions>>

\* Handle ReportSnapshot from application layer. Reference: raft.go:1608-1625
\* @type: (Int, Int, Bool) => Bool;
ReportSnapshotStatus(i, j, success) ==
    /\ state[i] = Leader
    /\ progressState[i][j] = StateSnapshot
    /\ LET oldPendingSnapshot == IF success THEN pendingSnapshot[i][j] ELSE 0
           newNext == Max({matchIndex[i][j] + 1, oldPendingSnapshot + 1})
       IN /\ progressState' = [progressState EXCEPT ![i][j] = StateProbe]
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] = newNext]
          /\ pendingSnapshot' = [pendingSnapshot EXCEPT ![i][j] = 0]
          /\ msgAppFlowPaused' = [msgAppFlowPaused EXCEPT ![i][j] = TRUE]
          /\ inflights' = [inflights EXCEPT ![i][j] = {}]
    /\ UNCHANGED <<serverVars, candidateVars, messageVars, logVars, configVars,
                   durableState, matchIndex, pendingConfChangeIndex, historyLog, partitions>>

\* Any RPC with a newer term causes the recipient to advance its term first.
\* @type: (Int, Int, MSG) => Bool;
UpdateTerm(i, j, m) ==
    /\ m.mterm > currentTerm[i]
    /\ BecomeFollowerOfTerm(i, m.mterm)
       \* messages is unchanged so m can be processed further.
    /\ UNCHANGED <<messageVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Responses with stale terms are ignored.
\* @type: (Int, Int, MSG) => Bool;
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Drop responses from non-members. Self-directed messages bypass this check.
\* Reference: rawnode.go:123-125
\* @type: (Int, Int, MSG) => Bool;
DropResponseFromNonMember(i, j, m) ==
    /\ j /= i
    /\ j \notin (GetConfig(i) \union GetOutgoingConfig(i) \union GetLearners(i))
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Atomic term update + vote handling (single Step call in raft.go).
UpdateTermAndHandleRequestVote(i, j, m) ==
    /\ m.mtype = RequestVoteRequest
    /\ m.mterm > currentTerm[i]
    /\ LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                    \/ /\ m.mlastLogTerm = LastTerm(log[i])
                       /\ m.mlastLogIndex >= LastIndex(log[i])
           grant == logOk \* Term is equal (after update), Vote is Nil (after update)
       IN
           /\ Reply([mtype        |-> RequestVoteResponse,
                     mterm        |-> m.mterm,
                     mvoteGranted |-> grant,
                     msource      |-> i,
                     mdest        |-> j],
                     m)
           /\ currentTerm' = [currentTerm EXCEPT ![i] = m.mterm]
           /\ state'       = [state       EXCEPT ![i] = Follower]
           /\ votedFor'    = [votedFor    EXCEPT ![i] = IF grant THEN j ELSE Nil]
           /\ UNCHANGED <<candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Receive a message (with optional FIFO ordering and partition checks).
ReceiveDirect(m) ==
    LET i == m.mdest
        j == m.msource
    IN /\ CanCommunicate(j, i)  \* Partition check: source and dest must be able to communicate
       /\ IsFifoFirst(m)  \* FIFO constraint: only receive if first in order
       /\ \* Any RPC with a newer term causes the recipient to advance
          \* its term first. Responses with stale terms are ignored.
          \/ UpdateTermAndHandleRequestVote(i, j, m)
          \/ /\ m.mtype /= RequestVoteRequest
             /\ UpdateTerm(i, j, m)
          \/ /\ m.mtype = RequestVoteRequest
             /\ HandleRequestVoteRequest(i, j, m)
          \/ /\ m.mtype = RequestVoteResponse
             /\ \/ DropStaleResponse(i, j, m)
                \/ DropResponseFromNonMember(i, j, m)
                \/ HandleRequestVoteResponse(i, j, m)
          \/ /\ m.mtype = AppendEntriesRequest
             /\ HandleAppendEntriesRequest(i, j, m)
          \/ /\ m.mtype = AppendEntriesResponse
             /\ \/ DropStaleResponse(i, j, m)
                \/ DropResponseFromNonMember(i, j, m)
                \/ HandleHeartbeatResponse(i, j, m)
                \/ HandleAppendEntriesResponse(i, j, m)
          \/ /\ m.mtype = SnapshotRequest
             /\ HandleSnapshotRequest(i, j, m)

Receive(m) == ReceiveDirect(m)

NextRequestVoteRequest == \E m \in DOMAIN messages : m.mtype = RequestVoteRequest /\ Receive(m)
NextRequestVoteResponse == \E m \in DOMAIN messages : m.mtype = RequestVoteResponse /\ Receive(m)
NextAppendEntriesRequest == \E m \in DOMAIN messages : m.mtype = AppendEntriesRequest /\ Receive(m)
NextAppendEntriesResponse == \E m \in DOMAIN messages : m.mtype = AppendEntriesResponse /\ Receive(m)

\* End of message handlers.
----
\* Network state transitions

\* The network duplicates a message
\* @type: MSG => Bool;
DuplicateMessage(m) ==
    /\ m \in DOMAIN messages
    /\ messages' = WithMessage(m, messages)
    /\ UNCHANGED <<pendingMessages, msgSeqCounter, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* The network drops a message
\* @type: MSG => Bool;
DropMessage(m) ==
    \* Do not drop loopback messages
    \* /\ m.msource /= m.mdest
    /\ Discard(m)
    /\ UNCHANGED <<pendingMessages, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog, partitions>>

\* Network Partition Actions

\* Create a network partition, dropping all cross-partition messages.
CreatePartition(partitionAssignment) ==
    /\ ~IsPartitioned
    /\ \A i \in Server : partitionAssignment[i] \in 1..MaxPartitions
    \* /\ Cardinality({partitionAssignment[i] : i \in Server}) >= 2
    /\ partitions' = partitionAssignment
    /\ messages' = messages (-) SetToBag(CrossPartitionMessages)
    /\ pendingMessages' = pendingMessages (-) SetToBag(CrossPartitionPendingMessages)
    /\ UNCHANGED <<msgSeqCounter, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog>>

\* Heal the network partition.
HealPartition ==
    /\ IsPartitioned
    /\ partitions' = [i \in Server |-> 0]
    /\ UNCHANGED <<messages, pendingMessages, msgSeqCounter, serverVars, candidateVars, leaderVars, logVars, configVars, durableState, progressVars, historyLog>>

----

\* Defines how the variables may transition.
NextAsync == 
    \/ \E i,j \in Server : RequestVote(i, j)
    \/ \E i \in Server : BecomeLeader(i)
    \/ \E i \in Server: ClientRequest(i, 0)
    \/ \E i \in Server: ClientRequestAndSend(i, 0)
    \/ \E i \in Server : AdvanceCommitIndex(i)
    \/ \E i,j \in Server : \E b,e \in matchIndex[i][j]+1..LastIndex(log[i])+1 : AppendEntries(i, j, <<b,e>>)
    \/ \E i \in Server : AppendEntriesToSelf(i)
    \/ \E i,j \in Server : Heartbeat(i, j)
    \/ \E i,j \in Server : SendSnapshot(i, j)
    \/ \E i \in Server : IF applied[i] < commitIndex[i] THEN ApplyEntries(i, commitIndex[i]) ELSE FALSE
    \/ \E i \in Server : IF log[i].offset < applied[i] THEN CompactLog(i, applied[i]) ELSE FALSE
    \/ \E m \in DOMAIN messages : Receive(m)
    \/ \E i \in Server : Timeout(i)
    \/ \E i \in Server : Ready(i)
    \/ \E i \in Server : StepDownToFollower(i)
    \/ \E i,j \in Server : ReportUnreachable(i, j)
    \/ \E i,j \in Server : ReportSnapshotStatus(i, j, TRUE)
    \/ \E i,j \in Server : ReportSnapshotStatus(i, j, FALSE)

NextCrash == \E i \in Server : Restart(i)

NextAsyncCrash ==
    \/ NextAsync
    \/ NextCrash

NextUnreliable ==    
    \* Only duplicate once
    \/ \E m \in DOMAIN messages : 
        /\ messages[m] = 1
        /\ DuplicateMessage(m)
    \* Only drop if it makes a difference            
    \/ \E m \in DOMAIN messages : 
        /\ messages[m] = 1
        /\ DropMessage(m)

\* Most pessimistic network model
Next == \/ NextAsync
        \/ NextCrash
        \/ NextUnreliable

\* Membership changes
\* Note: AddNewServer, AddLearner, DeleteServer are removed from NextDynamic.
\* They bypass ChangeConf constraints and can cause QuorumLogInv violations.
\* Use ChangeConf with enterJoint parameter instead.
NextDynamic ==
    \/ Next
    \/ \E i \in Server : ChangeConf(i)
    \/ \E i \in Server : ChangeConfAndSend(i)
    \/ \E i \in Server : ApplySimpleConfChange(i)
    \/ \E i \in Server : ProposeLeaveJoint(i)

\* The specification must start with the initial state and transition according
\* to Next.
Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The main safety properties are below                                    *)
(***************************************************************************)
----

ASSUME DistinctRoles == /\ Leader /= Candidate
                        /\ Candidate /= Follower
                        /\ Follower /= Leader

ASSUME DistinctMessageTypes == /\ RequestVoteRequest /= AppendEntriesRequest
                               /\ RequestVoteRequest /= RequestVoteResponse
                               /\ RequestVoteRequest /= AppendEntriesResponse
                               /\ AppendEntriesRequest /= RequestVoteResponse
                               /\ AppendEntriesRequest /= AppendEntriesResponse
                               /\ RequestVoteResponse /= AppendEntriesResponse
                               /\ SnapshotRequest /= RequestVoteRequest
                               /\ SnapshotRequest /= AppendEntriesRequest
                               /\ SnapshotRequest /= RequestVoteResponse
                               /\ SnapshotRequest /= AppendEntriesResponse
                               /\ SnapshotResponse /= RequestVoteRequest
                               /\ SnapshotResponse /= AppendEntriesRequest
                               /\ SnapshotResponse /= RequestVoteResponse
                               /\ SnapshotResponse /= AppendEntriesResponse
                               /\ SnapshotRequest /= SnapshotResponse

----
\* Correctness invariants

\* The prefix of the log of server i that has been committed
Committed(i) == SubSeq(historyLog[i],1,commitIndex[i])

\* @type: MSG => Bool;
MessageTermsLtCurrentTerm(m) ==
    m.mterm <= currentTerm[m.msource]

\* Committed log entries should never conflict between servers
LogInv ==
    \A i, j \in Server : i /= j =>
        \/ IsPrefix(Committed(i),Committed(j)) 
        \/ IsPrefix(Committed(j),Committed(i))


\* At most one leader per term
MoreThanOneLeaderInv ==
    \A i,j \in Server :
        (/\ currentTerm[i] = currentTerm[j]
         /\ state[i] = Leader
         /\ state[j] = Leader)
        => i = j

\* Every (index, term) pair determines a log prefix
LogMatchingInv ==
    \A i, j \in Server : i /= j =>
        LET minLen == Min({Len(historyLog[i]), Len(historyLog[j])})
            \* Find all indices where terms match
            matchingTerms == {n \in 1..minLen : historyLog[i][n].term = historyLog[j][n].term}
        IN matchingTerms /= {} =>
            \* Only check the maximum matching index
            SubSeq(historyLog[i], 1, Max(matchingTerms)) = SubSeq(historyLog[j], 1, Max(matchingTerms))

\* All committed entries exist in at least one server per quorum.
\* In joint config, checks outgoing config's quorum.
\* Only checks servers with up-to-date config.
QuorumLogInv ==
    \A i \in Server :
        \* Find config entries within the committed range
        LET configIndicesInCommitted == {k \in 1..commitIndex[i] :
                k <= Len(historyLog[i]) /\ historyLog[i][k].type = ConfigEntry}
            \* Check if server's config is up-to-date (applied all committed config entries)
            configUpToDate == configIndicesInCommitted = {} \/
                              appliedConfigIndex[i] >= Max(configIndicesInCommitted)
            \* In joint config, use outgoing config for quorum check
            \* because incoming config hasn't taken effect yet (LeaveJoint not committed)
            effectiveConfig == IF IsJointConfig(i) THEN GetOutgoingConfig(i) ELSE GetConfig(i)
        IN
        \* Only check servers with up-to-date config
        configUpToDate =>
            \A S \in Quorum(effectiveConfig) :
                \E j \in S : IsPrefix(Committed(i), historyLog[j])

\* A more up-to-date server has all committed entries of less up-to-date servers
MoreUpToDateCorrectInv ==
    \A i, j \in Server : i /= j =>
       ((\/ LastTerm(log[i]) > LastTerm(log[j])
         \/ /\ LastTerm(log[i]) = LastTerm(log[j])
            /\ LastIndex(log[i]) >= LastIndex(log[j])) =>
        IsPrefix(Committed(j), historyLog[i]))

\* Leader must contain all entries committed in previous terms by any server.

\* Helper: prefix of committed log where term <= t
CommittedTermPrefix(j, t) ==
    LET committed == Committed(j)
        validIndices == {k \in 1..Len(committed) : committed[k].term <= t}
    IN IF validIndices = {} THEN <<>>
       ELSE SubSeq(committed, 1, Max(validIndices))

LeaderCompletenessInv ==
    \A i \in Server :
        state[i] = Leader =>
        \A j \in Server : i /= j =>
            IsPrefix(CommittedTermPrefix(j, currentTerm[i]), historyLog[i])

\* Any entry committed by leader shall be persisted already
CommittedIsDurableInv ==
    \A i \in Server :
        state[i] = Leader => commitIndex[i] <= durableState[i].log



ProbeLimitInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateProbe)
            => InflightsCount(i, j) <= 1

\* In StateProbe, at most 1 AppendEntries message in the network
ProbeNetworkMessageLimitInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ i /= j /\ progressState[i][j] = StateProbe) =>
            LET msgsInNetwork == {m \in DOMAIN messages :
                    m.mtype = AppendEntriesRequest /\
                    m.msource = i /\
                    m.mdest = j}
                totalCount == FoldSet(LAMBDA m, acc: acc + messages[m], 0, msgsInNetwork)
            IN totalCount <= 1

\* In StateReplicate, paused only when inflights full
ReplicatePauseInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateReplicate /\ msgAppFlowPaused[i][j])
            => InflightsFull(i, j)


\* In StateSnapshot, inflights must be empty
SnapshotInflightsInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateSnapshot)
            => InflightsCount(i, j) = 0

\* Inflight indices must be <= LastIndex(log)
InflightsLogIndexInv ==
    \A i \in Server : \A j \in Server :
        state[i] = Leader =>
            \A idx \in inflights[i][j] : idx <= LastIndex(log[i])

\* Inflight indices must be > matchIndex
InflightsMatchIndexInv ==
    \A i \in Server : \A j \in Server :
        state[i] = Leader =>
            \A idx \in inflights[i][j] : idx > matchIndex[i][j]


ProgressStateTypeInv ==
    \A i, j \in Server:
        progressState[i][j] \in {StateProbe, StateReplicate, StateSnapshot}

\* Reference: inflights.go:66-68 Add() panics on full inflights
InflightsInv ==
    \A i, j \in Server:
        InflightsCount(i, j) <= MaxInflightMsgs

SnapshotPendingInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateSnapshot)
            => pendingSnapshot[i][j] > 0

NoPendingSnapshotInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] /= StateSnapshot)
            => pendingSnapshot[i][j] = 0

LeaderSelfReplicateInv ==
    \A i \in Server :
        state[i] = Leader => progressState[i][i] = StateReplicate

\* Comprehensive StateSnapshot consistency check
SnapshotStateInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateSnapshot) =>
            /\ InflightsCount(i, j) = 0              \* Inflights must be empty
            /\ pendingSnapshot[i][j] > 0             \* Must have pending snapshot
            /\ pendingSnapshot[i][j] <= LastIndex(log[i])  \* Snapshot index must be valid

InflightsMonotonicInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ inflights[i][j] # {}) =>
            LET maxIdx == Max(inflights[i][j])
                minIdx == Min(inflights[i][j])
            IN
                maxIdx >= minIdx  \* Trivial sanity check

\* Reference: progress.go:37 "0 <= Match < Next"
MatchIndexLessThanLogInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ j /= i) =>
            matchIndex[i][j] <= LastIndex(log[i])

MatchIndexNonNegativeInv ==
    \A i \in Server : \A j \in Server :
        matchIndex[i][j] >= 0

\* Inflight indices in (Match, Next) interval
InflightsAboveMatchInv ==
    \A i \in Server : \A j \in Server :
        state[i] = Leader =>
            \A idx \in inflights[i][j] : idx > matchIndex[i][j]

\* Match < Next (fundamental Progress invariant)
\* Reference: progress.go:37
MatchIndexLessThanNextInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ j /= i) =>
            matchIndex[i][j] < nextIndex[i][j]


\* In StateReplicate: not paused => inflights not full
MsgAppFlowPausedConsistencyInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateReplicate
         /\ ~msgAppFlowPaused[i][j]) =>
            ~InflightsFull(i, j)

ProbeOneInflightMaxInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateProbe) =>
            InflightsCount(i, j) <= 1

SnapshotNoInflightsStrictInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateSnapshot) =>
            InflightsCount(i, j) = 0

ProgressSafety ==
    /\ ProbeLimitInv
    /\ ProbeNetworkMessageLimitInv
    /\ ReplicatePauseInv
    /\ SnapshotInflightsInv
    /\ InflightsLogIndexInv
    /\ InflightsMatchIndexInv
    /\ ProgressStateTypeInv
    /\ InflightsInv
    /\ SnapshotPendingInv
    /\ NoPendingSnapshotInv
    /\ LeaderSelfReplicateInv
    /\ SnapshotStateInv
    /\ InflightsMonotonicInv
    /\ MatchIndexLessThanLogInv
    /\ MatchIndexNonNegativeInv
    /\ InflightsAboveMatchInv
    /\ MatchIndexLessThanNextInv
    /\ MsgAppFlowPausedConsistencyInv
    /\ ProbeOneInflightMaxInv
    /\ SnapshotNoInflightsStrictInv



AllMessageTermsValid ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        MessageTermsLtCurrentTerm(m)

MessageIndexValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        (m.mtype = AppendEntriesRequest) =>
            /\ m.mprevLogIndex >= 0
            /\ m.mcommitIndex >= 0

\* All nodes agree on committed entries
StateMachineConsistency ==
    \A i, j \in Server : i /= j =>
        LET minCommit == Min({commitIndex[i], commitIndex[j]})
        IN SubSeq(historyLog[i], 1, minCommit) = SubSeq(historyLog[j], 1, minCommit)

CommitIndexBoundInv ==
    \A i \in Server :
        commitIndex[i] <= LastIndex(log[i])

\* Log terms are monotonically non-decreasing
LogTermMonotonic ==
    \A i \in Server :
        \A idx \in 1..(LastIndex(log[i]) - 1) :
            LogTerm(i, idx) <= LogTerm(i, idx + 1)

CommittedEntriesTermInv ==
    \A i \in Server :
        \A idx \in 1..commitIndex[i] :
            historyLog[i][idx].term > 0

PendingConfigBoundInv ==
    \A i \in Server :
        state[i] = Leader =>
            pendingConfChangeIndex[i] <= LastIndex(log[i])

LeaderLogLengthInv ==
    \A i \in Server :
        state[i] = Leader =>
            commitIndex[i] <= LastIndex(log[i])

\* currentTerm >= all log entry terms
CurrentTermAtLeastLogTerm ==
    \A i \in Server :
        \* Check snapshot term
        /\ log[i].snapshotTerm <= currentTerm[i]
        \* Check only the last entry (highest term due to monotonicity)
        /\ Len(log[i].entries) > 0 =>
            log[i].entries[Len(log[i].entries)].term <= currentTerm[i]


CandidateVotedForSelfInv ==
    \A i \in Server :
        state[i] = Candidate =>
            votedFor[i] = i

\* Durable state <= volatile state
DurableStateConsistency ==
    \A i \in Server :
        /\ durableState[i].currentTerm <= currentTerm[i]
        /\ durableState[i].commitIndex <= commitIndex[i]

MessageEndpointsValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        /\ m.msource \in Server
        /\ m.mdest \in Server

LeaderDurableTermInv ==
    \A i \in Server :
        state[i] = Leader =>
            durableState[i].currentTerm = currentTerm[i]

AdditionalSafety ==
    /\ AllMessageTermsValid
    /\ MessageIndexValidInv
    /\ StateMachineConsistency
    /\ CommitIndexBoundInv
    /\ CommittedEntriesTermInv
    /\ PendingConfigBoundInv
    /\ LeaderLogLengthInv
    /\ CurrentTermAtLeastLogTerm
    /\ CandidateVotedForSelfInv
    /\ DurableStateConsistency
    /\ MessageEndpointsValidInv
    /\ LeaderDurableTermInv

LogOffsetMinInv ==
    \A i \in Server :
        log[i].offset >= 1

\* snapshotIndex = offset - 1
SnapshotOffsetConsistencyInv ==
    \A i \in Server :
        log[i].snapshotIndex = log[i].offset - 1

SnapshotTermValidInv ==
    \A i \in Server :
        log[i].snapshotIndex > 0 => log[i].snapshotTerm > 0

SnapshotTermBoundInv ==
    \A i \in Server :
        log[i].snapshotTerm <= currentTerm[i]

HistoryLogLengthInv ==
    \A i \in Server :
        Len(historyLog[i]) = LastIndex(log[i])

LogStructureInv ==
    /\ LogOffsetMinInv
    /\ SnapshotOffsetConsistencyInv
    /\ SnapshotTermValidInv
    /\ SnapshotTermBoundInv
    /\ HistoryLogLengthInv


JointConfigNonEmptyInv ==
    \A i \in Server :
        IsJointConfig(i) =>
            /\ GetConfig(i) /= {}
            /\ GetOutgoingConfig(i) /= {}

SingleConfigOutgoingEmptyInv ==
    \A i \in Server :
        ~IsJointConfig(i) => GetOutgoingConfig(i) = {}

\* Reference: tracker/tracker.go:37-41
LearnersVotersDisjointInv ==
    \A i \in Server :
        GetLearners(i) \cap (GetConfig(i) \union GetOutgoingConfig(i)) = {}

\* Initialized servers must have at least one voter
ConfigNonEmptyInv ==
    \A i \in Server :
        LET configIndices == {k \in 1..Len(historyLog[i]) : historyLog[i][k].type = ConfigEntry}
            lastConfigIdx == IF configIndices /= {} THEN Max(configIndices) ELSE 0
            \* Config is considered applied if no config entries exist or appliedConfigIndex >= last config
            configApplied == lastConfigIdx = 0 \/ appliedConfigIndex[i] >= lastConfigIdx
        IN
        (LastIndex(log[i]) > 0 /\ configApplied) => GetConfig(i) /= {}

ConfigurationInv ==
    /\ JointConfigNonEmptyInv
    /\ SingleConfigOutgoingEmptyInv
    /\ LearnersVotersDisjointInv
    /\ ConfigNonEmptyInv


SnapshotMsgIndexValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = SnapshotRequest =>
            m.msnapshotIndex <= applied[m.msource]

SnapshotMsgTermValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = SnapshotRequest =>
            /\ m.msnapshotTerm > 0
            /\ m.msnapshotTerm <= m.mterm

AppendEntriesPrevIndexNonNegInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = AppendEntriesRequest =>
            m.mprevLogIndex >= 0

AppendEntriesCommitBoundInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = AppendEntriesRequest =>
            m.mcommitIndex >= 0

VoteRequestLogIndexNonNegInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = RequestVoteRequest =>
            m.mlastLogIndex >= 0

VoteRequestLogTermNonNegInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = RequestVoteRequest =>
            m.mlastLogTerm >= 0

MessageContentInv ==
    /\ SnapshotMsgIndexValidInv
    /\ SnapshotMsgTermValidInv
    /\ AppendEntriesPrevIndexNonNegInv
    /\ AppendEntriesCommitBoundInv
    /\ VoteRequestLogIndexNonNegInv
    /\ VoteRequestLogTermNonNegInv

\* Only StateReplicate can have non-empty inflights
InflightsOnlyInReplicateInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] /= StateReplicate) =>
            inflights[i][j] = {}

InflightsBelowNextInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ inflights[i][j] /= {}) =>
            \A idx \in inflights[i][j] : idx < nextIndex[i][j]

InflightsRefinedInv ==
    /\ InflightsOnlyInReplicateInv
    /\ InflightsBelowNextInv

SnapshotCommitConsistencyInv ==
    \A i \in Server :
        log[i].snapshotIndex <= commitIndex[i]


\* Pending config change index must be within valid log bounds
PendingConfIndexValidInv ==
    \A i \in Server :
        (state[i] = Leader /\ pendingConfChangeIndex[i] > applied[i]) =>
            /\ pendingConfChangeIndex[i] <= LastIndex(log[i])
            /\ pendingConfChangeIndex[i] >= log[i].offset


NextIndexPositiveInv ==
    \A i \in Server : \A j \in Server :
        state[i] = Leader => nextIndex[i][j] >= 1

NextIndexBoundInv ==
    \A i \in Server : \A j \in Server :
        state[i] = Leader => nextIndex[i][j] <= LastIndex(log[i]) + 1

MatchIndexBoundInv ==
    \A i \in Server : \A j \in Server :
        state[i] = Leader => matchIndex[i][j] <= LastIndex(log[i])

PendingSnapshotBoundInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ pendingSnapshot[i][j] > 0) =>
            pendingSnapshot[i][j] <= LastIndex(log[i])


AppendEntriesTermConsistentInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        (m.mtype = AppendEntriesRequest /\ Len(m.mentries) > 0) =>
            \A k \in 1..Len(m.mentries) :
                m.mentries[k].term <= m.mterm

SnapshotMsgIndexPositiveInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = SnapshotRequest =>
            m.msnapshotIndex > 0

ResponseTermPositiveInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        (m.mtype = RequestVoteResponse \/ m.mtype = AppendEntriesResponse) =>
            m.mterm > 0


TermPositiveInv ==
    \A i \in Server :
        currentTerm[i] >= 0

LeaderTermPositiveInv ==
    \A i \in Server :
        state[i] = Leader => currentTerm[i] > 0

CandidateTermPositiveInv ==
    \A i \in Server :
        state[i] = Candidate => currentTerm[i] > 0

\* votesResponded (excluding self) subset of config
VotesRespondedSubsetInv ==
    \A i \in Server :
        state[i] = Candidate =>
            (votesResponded[i] \ {i}) \subseteq (GetConfig(i) \union GetOutgoingConfig(i) \union GetLearners(i))

VotesGrantedSubsetInv ==
    \A i \in Server :
        state[i] = Candidate =>
            votesGranted[i] \subseteq votesResponded[i]


AdditionalSnapshotInv ==
    /\ SnapshotCommitConsistencyInv

AdditionalConfigInv ==
    /\ PendingConfIndexValidInv

AdditionalProgressInv ==
    /\ NextIndexPositiveInv
    /\ NextIndexBoundInv
    /\ MatchIndexBoundInv
    /\ PendingSnapshotBoundInv

AdditionalMessageInv ==
    /\ AppendEntriesTermConsistentInv
    /\ SnapshotMsgIndexPositiveInv
    /\ ResponseTermPositiveInv

TermAndVoteInv ==
    /\ TermPositiveInv
    /\ LeaderTermPositiveInv
    /\ CandidateTermPositiveInv
    /\ VotesRespondedSubsetInv
    /\ VotesGrantedSubsetInv


NewInvariants ==
    /\ LogStructureInv
    /\ ConfigurationInv
    /\ MessageContentInv
    /\ InflightsRefinedInv
    /\ AdditionalSnapshotInv
    /\ AdditionalConfigInv
    /\ AdditionalProgressInv
    /\ AdditionalMessageInv
    /\ TermAndVoteInv

\* Bug 76f1249: prevLogTerm must not be 0 when prevLogIndex > 0

AppendEntriesPrevLogTermValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        (m.mtype = AppendEntriesRequest /\ m.mprevLogIndex > 0) =>
            m.mprevLogTerm > 0

\* Bug bd3c759: at most one pending leave-joint entry in uncommitted log

SinglePendingLeaveJointInv ==
    \A i \in Server :
        (state[i] = Leader /\ IsJointConfig(i) /\ config[i].autoLeave) =>
            LET \* Find all uncommitted leave-joint entries
                uncommittedLeaveJoints == {k \in (commitIndex[i]+1)..LastIndex(log[i]) :
                    /\ IsAvailable(i, k)
                    /\ LogEntry(i, k).type = ConfigEntry
                    /\ "leaveJoint" \in DOMAIN LogEntry(i, k).value
                    /\ LogEntry(i, k).value.leaveJoint = TRUE}
            IN Cardinality(uncommittedLeaveJoints) <= 1

\* pendingConfIndex should cover pending leave-joint entries
PendingConfIndexAutoLeaveInv ==
    \A i \in Server :
        (state[i] = Leader /\ IsJointConfig(i)) =>
            LET leaveJointIndices == {k \in (commitIndex[i]+1)..LastIndex(log[i]) :
                    /\ IsAvailable(i, k)
                    /\ LogEntry(i, k).type = ConfigEntry
                    /\ "leaveJoint" \in DOMAIN LogEntry(i, k).value
                    /\ LogEntry(i, k).value.leaveJoint = TRUE}
            IN leaveJointIndices /= {} =>
                pendingConfChangeIndex[i] >= Min(leaveJointIndices)

\* Bug #12136: joint state must not get stuck
JointStateAutoLeavePossibleInv ==
    \A i \in Server :
        (state[i] = Leader /\ IsJointConfig(i) /\ config[i].autoLeave = TRUE) =>
            ((applied[i] >= pendingConfChangeIndex[i]) =>
                pendingConfChangeIndex[i] < LastIndex(log[i]))

\* Bug #7280: config must be applied before election
ElectionConfigAppliedInv ==
    \A i \in Server :
        state[i] = Candidate =>
            LET configIndicesInCommitted == {k \in 1..commitIndex[i] :
                    k <= Len(historyLog[i]) /\ historyLog[i][k].type = ConfigEntry}
            IN configIndicesInCommitted = {} \/ appliedConfigIndex[i] >= Max(configIndicesInCommitted)

\* Bug #124: matchIndex <= pendingSnapshot in StateSnapshot
SnapshotTransitionCorrectInv ==
    \A i, j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateSnapshot) =>
            matchIndex[i][j] <= pendingSnapshot[i][j]


BugDetectionInv ==
    /\ AppendEntriesPrevLogTermValidInv
    /\ SinglePendingLeaveJointInv
    /\ PendingConfIndexAutoLeaveInv
    /\ JointStateAutoLeavePossibleInv
    /\ ElectionConfigAppliedInv
    /\ SnapshotTransitionCorrectInv


\* Reference: log.go:46 "applied <= committed"
AppliedBoundInv ==
    \A i \in Server :
        applied[i] <= commitIndex[i]

\* Reference: Issue #17081
MessageDestinationValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        /\ m.mdest \in Server
        /\ m.msource \in Server

\* snapshotIndex <= applied
SnapshotAppliedConsistencyInv ==
    \A i \in Server :
        log[i].snapshotIndex <= applied[i]

AppliedConfigBoundInv ==
    \A i \in Server :
        appliedConfigIndex[i] <= commitIndex[i]

CriticalInv ==
    /\ AppliedBoundInv
    /\ CommitIndexBoundInv
    /\ MessageDestinationValidInv

HighPriorityInv ==
    /\ SnapshotAppliedConsistencyInv

DetailedInv ==
    /\ AppliedConfigBoundInv


\* Reference: progress.go:40 "In StateSnapshot, Next == PendingSnapshot + 1"
StateSnapshotNextInv ==
    \A i \in Server : \A j \in Server :
        (state[i] = Leader /\ progressState[i][j] = StateSnapshot) =>
            nextIndex[i][j] = pendingSnapshot[i][j] + 1

CodeDerivedInv ==
    /\ StateSnapshotNextInv

\* votedFor must be a valid server
VotedForConsistencyInv ==
    \A i \in Server :
        votedFor[i] /= Nil =>
            \* If votedFor is set, it should be a valid server
            votedFor[i] \in Server

\* Entries with leader's current term must exist in leader's log
LeaderCurrentTermEntriesInv ==
    \A leader \in Server :
        state[leader] = Leader =>
            \A other \in Server :
                \A idx \in log[other].offset..(LastIndex(log[other])) :
                    (IsAvailable(other, idx) /\ LogEntry(other, idx).term = currentTerm[leader]) =>
                        \* Entry must exist in leader's log
                        /\ idx <= LastIndex(log[leader])
                        /\ IsAvailable(leader, idx)
                        /\ LogEntry(leader, idx).term = currentTerm[leader]

RequestVoteTermBoundInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = RequestVoteRequest =>
            \* The message term should be positive
            /\ m.mterm > 0
            \* Log term in request should be <= message term
            /\ m.mlastLogTerm <= m.mterm

AppendEntriesResponseTermInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = AppendEntriesResponse =>
            \* Response term is positive
            m.mterm > 0

SnapshotResponseTermValidInv ==
    \A m \in DOMAIN messages \union DOMAIN pendingMessages :
        m.mtype = SnapshotResponse =>
            m.mterm > 0

VerdiRaftInspiredInv ==
    /\ VotedForConsistencyInv
    /\ LeaderCurrentTermEntriesInv
    /\ RequestVoteTermBoundInv
    /\ AppendEntriesResponseTermInv
    /\ SnapshotResponseTermValidInv


\* Log entries must have term > 0
TermNeverZeroInLogInv ==
    \A i \in Server :
        \A idx \in log[i].offset..LastIndex(log[i]) :
            LogEntry(i, idx).term > 0

GitHistoryBugPreventionInv ==
    /\ TermNeverZeroInLogInv

===============================================================================