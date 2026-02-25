package raftpb

import (
	fmt "fmt"
	"reflect"

	"go.etcd.io/raft/v3/coder"
)

type EntryType int8

const (
	EntryNormal       EntryType = 0
	EntryConfChange   EntryType = 1
	EntryConfChangeV2 EntryType = 2
)

var EntryType_name = map[int8]string{
	0: "EntryNormal",
	1: "EntryConfChange",
	2: "EntryConfChangeV2",
}

var EntryType_value = map[string]int8{
	"EntryNormal":       0,
	"EntryConfChange":   1,
	"EntryConfChangeV2": 2,
}

func (e EntryType) String() string {
	return EntryType_name[int8(e)]
}

// For description of different message types, see:
// https://pkg.go.dev/go.etcd.io/raft/v3#hdr-MessageType
type MessageType int8

const (
	MsgHup               MessageType = 0
	MsgBeat              MessageType = 1
	MsgProp              MessageType = 2
	MsgApp               MessageType = 3
	MsgAppResp           MessageType = 4
	MsgVote              MessageType = 5
	MsgVoteResp          MessageType = 6
	MsgSnap              MessageType = 7
	MsgHeartbeat         MessageType = 8
	MsgHeartbeatResp     MessageType = 9
	MsgUnreachable       MessageType = 10
	MsgSnapStatus        MessageType = 11
	MsgCheckQuorum       MessageType = 12
	MsgTransferLeader    MessageType = 13
	MsgTimeoutNow        MessageType = 14
	MsgReadIndex         MessageType = 15
	MsgReadIndexResp     MessageType = 16
	MsgPreVote           MessageType = 17
	MsgPreVoteResp       MessageType = 18
	MsgStorageAppend     MessageType = 19
	MsgStorageAppendResp MessageType = 20
	MsgStorageApply      MessageType = 21
	MsgStorageApplyResp  MessageType = 22
	MsgForgetLeader      MessageType = 23
)

var MessageType_name = map[int8]string{
	0:  "MsgHup",
	1:  "MsgBeat",
	2:  "MsgProp",
	3:  "MsgApp",
	4:  "MsgAppResp",
	5:  "MsgVote",
	6:  "MsgVoteResp",
	7:  "MsgSnap",
	8:  "MsgHeartbeat",
	9:  "MsgHeartbeatResp",
	10: "MsgUnreachable",
	11: "MsgSnapStatus",
	12: "MsgCheckQuorum",
	13: "MsgTransferLeader",
	14: "MsgTimeoutNow",
	15: "MsgReadIndex",
	16: "MsgReadIndexResp",
	17: "MsgPreVote",
	18: "MsgPreVoteResp",
	19: "MsgStorageAppend",
	20: "MsgStorageAppendResp",
	21: "MsgStorageApply",
	22: "MsgStorageApplyResp",
	23: "MsgForgetLeader",
}

var MessageType_value = map[string]int8{
	"MsgHup":               0,
	"MsgBeat":              1,
	"MsgProp":              2,
	"MsgApp":               3,
	"MsgAppResp":           4,
	"MsgVote":              5,
	"MsgVoteResp":          6,
	"MsgSnap":              7,
	"MsgHeartbeat":         8,
	"MsgHeartbeatResp":     9,
	"MsgUnreachable":       10,
	"MsgSnapStatus":        11,
	"MsgCheckQuorum":       12,
	"MsgTransferLeader":    13,
	"MsgTimeoutNow":        14,
	"MsgReadIndex":         15,
	"MsgReadIndexResp":     16,
	"MsgPreVote":           17,
	"MsgPreVoteResp":       18,
	"MsgStorageAppend":     19,
	"MsgStorageAppendResp": 20,
	"MsgStorageApply":      21,
	"MsgStorageApplyResp":  22,
	"MsgForgetLeader":      23,
}

func (m MessageType) String() string {
	return MessageType_name[int8(m)]
}

// ConfChangeTransition specifies the behavior of a configuration change with
// respect to joint consensus.
type ConfChangeTransition int8

const (
	// Automatically use the simple protocol if possible, otherwise fall back
	// to ConfChangeJointImplicit. Most applications will want to use this.
	ConfChangeTransitionAuto ConfChangeTransition = 0
	// Use joint consensus unconditionally, and transition out of them
	// automatically (by proposing a zero configuration change).
	//
	// This option is suitable for applications that want to minimize the time
	// spent in the joint configuration and do not store the joint configuration
	// in the state machine (outside of InitialState).
	ConfChangeTransitionJointImplicit ConfChangeTransition = 1
	// Use joint consensus and remain in the joint configuration until the
	// application proposes a no-op configuration change. This is suitable for
	// applications that want to explicitly control the transitions, for example
	// to use a custom payload (via the Context field).
	ConfChangeTransitionJointExplicit ConfChangeTransition = 2
)

var ConfChangeTransition_name = map[int8]string{
	0: "ConfChangeTransitionAuto",
	1: "ConfChangeTransitionJointImplicit",
	2: "ConfChangeTransitionJointExplicit",
}

var ConfChangeTransition_value = map[string]int8{
	"ConfChangeTransitionAuto":          0,
	"ConfChangeTransitionJointImplicit": 1,
	"ConfChangeTransitionJointExplicit": 2,
}

func (t ConfChangeTransition) String() string {
	return ConfChangeTransition_name[int8(t)]
}

type ConfChangeType int8

const (
	ConfChangeAddNode        ConfChangeType = 0
	ConfChangeRemoveNode     ConfChangeType = 1
	ConfChangeUpdateNode     ConfChangeType = 2
	ConfChangeAddLearnerNode ConfChangeType = 3
)

var ConfChangeType_name = map[int8]string{
	0: "ConfChangeAddNode",
	1: "ConfChangeRemoveNode",
	2: "ConfChangeUpdateNode",
	3: "ConfChangeAddLearnerNode",
}

var ConfChangeType_value = map[string]int8{
	"ConfChangeAddNode":        0,
	"ConfChangeRemoveNode":     1,
	"ConfChangeUpdateNode":     2,
	"ConfChangeAddLearnerNode": 3,
}

func (t ConfChangeType) String() string {
	return ConfChangeType_name[int8(t)]
}

type Entry struct {
	Type  EntryType
	Term  uint64
	Index uint64
	Data  []byte
}

func (e Entry) String() string {
	if e.Type == EntryNormal {
		return fmt.Sprintf("{%d %d %s %s}", e.Term, e.Index, e.Type, string(e.Data))
	}
	return fmt.Sprintf("{%d %d %s %v}", e.Term, e.Index, e.Type, e.Data)
}
func (e Entry) Size() int {
	return coder.Int8Size(int8(e.Type)) +
		coder.VarintSize(e.Term) +
		coder.VarintSize(e.Index) +
		coder.DataSize(e.Data)
}
func (e Entry) WriteTo(w coder.Encoder) error {
	w.WriteInt8(int8(e.Type))
	w.WriteVarint(e.Term)
	w.WriteVarint(e.Index)
	w.WriteData(e.Data)
	return nil
}
func (e *Entry) ReadFrom(r coder.Decoder) error {
	typ, err := r.ReadInt8()
	if err != nil {
		return err
	}
	term, err := r.ReadVarint()
	if err != nil {
		return err
	}
	index, err := r.ReadVarint()
	if err != nil {
		return err
	}
	data, err := r.ReadData()
	if err != nil {
		return err
	}
	e.Type = EntryType(typ)
	e.Term = term
	e.Index = index
	e.Data = data
	return nil
}
func (e Entry) Marshal() ([]byte, error) {
	return coder.Marshal(e)
}
func (e *Entry) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, e)
}

type SnapshotMetadata struct {
	Term      uint64
	Index     uint64
	ConfState ConfState
}

func (s SnapshotMetadata) String() string {
	return fmt.Sprintf("{%v %d %d}", s.ConfState, s.Index, s.Term)
}
func (s SnapshotMetadata) Size() int {
	return coder.VarintSize(s.Term) + coder.VarintSize(s.Index) + s.ConfState.Size()
}
func (s SnapshotMetadata) WriteTo(w coder.Encoder) error {
	w.WriteVarint(s.Term)
	w.WriteVarint(s.Index)
	s.ConfState.WriteTo(w)
	return nil
}
func (s *SnapshotMetadata) ReadFrom(r coder.Decoder) error {
	term, err := r.ReadVarint()
	if err != nil {
		return err
	}
	index, err := r.ReadVarint()
	if err != nil {
		return err
	}
	s.Term = term
	s.Index = index
	s.ConfState.ReadFrom(r)
	return nil
}
func (s SnapshotMetadata) Marshal() ([]byte, error) {
	return coder.Marshal(s)
}
func (s *SnapshotMetadata) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, s)
}

type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

func (s Snapshot) String() string {
	return fmt.Sprintf("{%v %v}", s.Metadata, s.Data)
}
func (s Snapshot) Size() int {
	return s.Metadata.Size() + coder.DataSize(s.Data)
}
func (s Snapshot) WriteTo(w coder.Encoder) error {
	s.Metadata.WriteTo(w)
	w.WriteData(s.Data)
	return nil
}
func (s *Snapshot) ReadFrom(r coder.Decoder) error {
	s.Metadata.ReadFrom(r)
	data, err := r.ReadData()
	if err != nil {
		return err
	}
	s.Data = data
	return nil
}
func (s Snapshot) Marshal() ([]byte, error) {
	return coder.Marshal(s)
}
func (s *Snapshot) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, s)
}

type Message struct {
	Type MessageType
	To   uint64
	From uint64
	Term uint64
	// logTerm is generally used for appending Raft logs to followers. For example,
	// (type=MsgApp,index=100,logTerm=5) means the leader appends entries starting
	// at index=101, and the term of the entry at index 100 is 5.
	// (type=MsgAppResp,reject=true,index=100,logTerm=5) means follower rejects some
	// entries from its leader as it already has an entry with term 5 at index 100.
	// (type=MsgStorageAppendResp,index=100,logTerm=5) means the local node wrote
	// entries up to index=100 in stable storage, and the term of the entry at index
	// 100 was 5. This doesn't always mean that the corresponding MsgStorageAppend
	// message was the one that carried these entries, just that those entries were
	// stable at the time of processing the corresponding MsgStorageAppend.
	LogTerm uint64
	Index   uint64
	Entries []Entry
	Commit  uint64
	// (type=MsgStorageAppend,vote=5,term=10) means the local node is voting for
	// peer 5 in term 10. For MsgStorageAppends, the term, vote, and commit fields
	// will either all be set (to facilitate the construction of a HardState) if
	// any of the fields have changed or will all be unset if none of the fields
	// have changed.
	Vote uint64
	// snapshot is non-nil and non-empty for MsgSnap messages and nil for all other
	// message types. However, peer nodes running older binary versions may send a
	// non-nil, empty value for the snapshot field of non-MsgSnap messages. Code
	// should be prepared to handle such messages.
	Snapshot   *Snapshot
	Reject     bool
	RejectHint uint64
	Context    []byte
	// responses are populated by a raft node to instruct storage threads on how
	// to respond and who to respond to when the work associated with a message
	// is complete. Populated for MsgStorageAppend and MsgStorageApply messages.
	Responses []Message
}

func (m Message) String() string {
	return fmt.Sprintf("{%s %d %d %d %d %d %v %d %d %v %v %d %v %v}",
		m.Type, m.To, m.From, m.Term, m.LogTerm, m.Index, m.Entries, m.Commit, m.Vote, m.Snapshot, m.Reject, m.RejectHint, m.Context, m.Responses)
}
func (m Message) Size() int {
	snapSize := 0
	if m.Snapshot != nil {
		snapSize = m.Snapshot.Size()
	}
	return coder.Int8Size(int8(m.Type)) +
		coder.VarintSize(m.To) +
		coder.VarintSize(m.From) +
		coder.VarintSize(m.Term) +
		coder.VarintSize(m.LogTerm) +
		coder.VarintSize(m.Index) +
		coder.ArraySize(m.Entries) +
		coder.VarintSize(m.Commit) +
		coder.VarintSize(m.Vote) +
		coder.VarintSize(uint64(snapSize)) + snapSize +
		coder.BoolSize(m.Reject) +
		coder.VarintSize(m.RejectHint) +
		coder.DataSize(m.Context) +
		coder.ArraySize(m.Responses)
}
func (m Message) WriteTo(w coder.Encoder) error {
	w.WriteInt8(int8(m.Type))
	w.WriteVarint(m.To)
	w.WriteVarint(m.From)
	w.WriteVarint(m.Term)
	w.WriteVarint(m.LogTerm)
	w.WriteVarint(m.Index)
	enlen := uint64(len(m.Entries))
	w.WriteVarint(enlen)
	for _, v := range m.Entries {
		v.WriteTo(w)
	}
	w.WriteVarint(m.Commit)
	w.WriteVarint(m.Vote)
	var sdata []byte
	if m.Snapshot != nil {
		var err error
		sdata, err = m.Snapshot.Marshal()
		if err != nil {
			return err
		}
	}
	w.WriteData(sdata)
	w.WriteBool(m.Reject)
	w.WriteVarint(m.RejectHint)
	w.WriteData(m.Context)
	relen := uint64(len(m.Responses))
	w.WriteVarint(relen)
	for _, v := range m.Responses {
		v.WriteTo(w)
	}
	return nil
}

func (m *Message) ReadFrom(r coder.Decoder) error {
	typ, err := r.ReadInt8()
	if err != nil {
		return err
	}
	to, err := r.ReadVarint()
	if err != nil {
		return err
	}
	from, err := r.ReadVarint()
	if err != nil {
		return err
	}
	term, err := r.ReadVarint()
	if err != nil {
		return err
	}
	logTerm, err := r.ReadVarint()
	if err != nil {
		return err
	}
	index, err := r.ReadVarint()
	if err != nil {
		return err
	}
	entlen, err := r.ReadVarint()
	if err != nil {
		return err
	}
	for i := 0; i < int(entlen); i++ {
		var entry Entry
		if err := entry.ReadFrom(r); err != nil {
			return err
		}
		m.Entries = append(m.Entries, entry)
	}
	if err != nil {
		return err
	}
	commit, err := r.ReadVarint()
	if err != nil {
		return err
	}
	vote, err := r.ReadVarint()
	if err != nil {
		return err
	}
	snapshotData, err := r.ReadData()
	if err != nil {
		return err
	}
	reject, err := r.ReadBool()
	if err != nil {
		return err
	}
	rejectHint, err := r.ReadVarint()
	if err != nil {
		return err
	}
	context, err := r.ReadData()
	if err != nil {
		return err
	}
	relen, err := r.ReadVarint()
	if err != nil {
		return err
	}
	for i := 0; i < int(relen); i++ {
		var resp Message
		if err := resp.ReadFrom(r); err != nil {
			return err
		}
		m.Responses = append(m.Responses, resp)
	}
	m.Type = MessageType(typ)
	m.To = to
	m.From = from
	m.Term = term
	m.LogTerm = logTerm
	m.Index = index
	m.Commit = commit
	m.Vote = vote
	if snapshotData != nil {
		var snapshot Snapshot
		if err := snapshot.Unmarshal(snapshotData); err != nil {
			return err
		}
		m.Snapshot = &snapshot
	}
	m.Reject = reject
	m.RejectHint = rejectHint
	m.Context = context
	return nil
}
func (m Message) Marshal() ([]byte, error) {
	return coder.Marshal(m)
}
func (m *Message) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, m)
}

type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

func (h HardState) String() string {
	return fmt.Sprintf("{%d %d %d}", h.Term, h.Vote, h.Commit)
}
func (h HardState) Size() int {
	return coder.VarintSize(h.Term) + coder.VarintSize(h.Vote) + coder.VarintSize(h.Commit)
}
func (h HardState) WriteTo(w coder.Encoder) error {
	w.WriteVarint(h.Term)
	w.WriteVarint(h.Vote)
	w.WriteVarint(h.Commit)
	return nil
}
func (h *HardState) ReadFrom(r coder.Decoder) error {
	term, err := r.ReadVarint()
	if err != nil {
		return err
	}
	vote, err := r.ReadVarint()
	if err != nil {
		return err
	}
	commit, err := r.ReadVarint()
	if err != nil {
		return err
	}
	h.Term = term
	h.Vote = vote
	h.Commit = commit
	return nil
}
func (h HardState) Marshal() ([]byte, error) {
	return coder.Marshal(h)
}
func (h *HardState) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, h)
}

type ConfState struct {
	// The voters in the incoming config. (If the configuration is not joint,
	// then the outgoing config is empty).
	Voters []uint64
	// The learners in the incoming config.
	Learners []uint64
	// The voters in the outgoing config.
	VotersOutgoing []uint64
	// The nodes that will become learners when the outgoing config is removed.
	// These nodes are necessarily currently in nodes_joint (or they would have
	// been added to the incoming config right away).
	LearnersNext []uint64
	// If set, the config is joint and Raft will automatically transition into
	// the final config (i.e. remove the outgoing config) when this is safe.
	AutoLeave bool
}

func (c ConfState) String() string {
	return fmt.Sprintf("{%v %v %v %v %v}",
		c.Voters, c.Learners, c.VotersOutgoing, c.LearnersNext, c.AutoLeave)
}
func (c ConfState) Size() int {
	return coder.VarintsSize(c.Voters) +
		coder.VarintsSize(c.Learners) +
		coder.VarintsSize(c.VotersOutgoing) +
		coder.VarintsSize(c.LearnersNext) +
		coder.BoolSize(c.AutoLeave)
}
func (c ConfState) WriteTo(w coder.Encoder) error {
	w.WriteVarints(c.Voters)
	w.WriteVarints(c.Learners)
	w.WriteVarints(c.VotersOutgoing)
	w.WriteVarints(c.LearnersNext)
	w.WriteBool(c.AutoLeave)
	return nil
}
func (c *ConfState) ReadFrom(r coder.Decoder) error {
	voters, err := r.ReadVarints()
	if err != nil {
		return err
	}
	learners, err := r.ReadVarints()
	if err != nil {
		return err
	}
	votersOutgoing, err := r.ReadVarints()
	if err != nil {
		return err
	}
	learnersNext, err := r.ReadVarints()
	if err != nil {
		return err
	}
	autoLeave, err := r.ReadBool()
	if err != nil {
		return err
	}
	c.Voters = voters
	c.Learners = learners
	c.VotersOutgoing = votersOutgoing
	c.LearnersNext = learnersNext
	c.AutoLeave = autoLeave
	return nil
}
func (c ConfState) Marshal() ([]byte, error) {
	return coder.Marshal(c)
}
func (c *ConfState) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, c)
}

type ConfChange struct {
	Type    ConfChangeType
	NodeID  uint64
	Context []byte
	// NB: this is used only by etcd to thread through a unique identifier.
	// Ideally it should really use the Context instead. No counterpart to
	// this field exists in ConfChangeV2.
	ID uint64
}

func (c ConfChange) String() string {
	return fmt.Sprintf("{%s %d %x %d}",
		c.Type, c.NodeID, c.Context, c.ID)
}
func (c ConfChange) Size() int {
	return coder.Int8Size(int8(c.Type)) +
		coder.VarintSize(c.NodeID) +
		coder.DataSize(c.Context) +
		coder.VarintSize(c.ID)
}
func (c ConfChange) WriteTo(w coder.Encoder) error {
	w.WriteInt8(int8(c.Type))
	w.WriteVarint(c.NodeID)
	w.WriteData(c.Context)
	w.WriteVarint(c.ID)
	return nil
}
func (c *ConfChange) ReadFrom(r coder.Decoder) error {
	typ, err := r.ReadInt8()
	if err != nil {
		return err
	}
	nodeID, err := r.ReadVarint()
	if err != nil {
		return err
	}
	context, err := r.ReadData()
	if err != nil {
		return err
	}
	id, err := r.ReadVarint()
	if err != nil {
		return err
	}
	c.Type = ConfChangeType(typ)
	c.NodeID = nodeID
	c.Context = context
	c.ID = id
	return nil
}
func (c ConfChange) Marshal() ([]byte, error) {
	return coder.Marshal(c)
}
func (c *ConfChange) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return coder.Unmarshal(data, c)
}

// ConfChangeSingle is an individual configuration change operation. Multiple
// such operations can be carried out atomically via a ConfChangeV2.
type ConfChangeSingle struct {
	Type   ConfChangeType
	NodeID uint64
}

func (c ConfChangeSingle) String() string {
	return fmt.Sprintf("{%s %d}",
		c.Type, c.NodeID)
}
func (c ConfChangeSingle) Size() int {
	return coder.Int8Size(int8(c.Type)) + coder.VarintSize(c.NodeID)
}
func (c ConfChangeSingle) WriteTo(w coder.Encoder) error {
	w.WriteInt8(int8(c.Type))
	w.WriteVarint(c.NodeID)
	return nil
}
func (c *ConfChangeSingle) ReadFrom(r coder.Decoder) error {
	typ, err := r.ReadInt8()
	if err != nil {
		return err
	}
	nodeID, err := r.ReadVarint()
	if err != nil {
		return err
	}
	c.Type = ConfChangeType(typ)
	c.NodeID = nodeID
	return nil
}
func (c ConfChangeSingle) Marshal() ([]byte, error) {
	return coder.Marshal(c)
}
func (c *ConfChangeSingle) Unmarshal(data []byte) error {
	return coder.Unmarshal(data, c)
}

// ConfChangeV2 messages initiate configuration changes. They support both the
// simple "one at a time" membership change protocol and full Joint Consensus
// allowing for arbitrary changes in membership.
//
// The supplied context is treated as an opaque payload and can be used to
// attach an action on the state machine to the application of the config change
// proposal. Note that contrary to Joint Consensus as outlined in the Raft
// paper[1], configuration changes become active when they are *applied* to the
// state machine (not when they are appended to the log).
//
// The simple protocol can be used whenever only a single change is made.
//
// Non-simple changes require the use of Joint Consensus, for which two
// configuration changes are run. The first configuration change specifies the
// desired changes and transitions the Raft group into the joint configuration,
// in which quorum requires a majority of both the pre-changes and post-changes
// configuration. Joint Consensus avoids entering fragile intermediate
// configurations that could compromise survivability. For example, without the
// use of Joint Consensus and running across three availability zones with a
// replication factor of three, it is not possible to replace a voter without
// entering an intermediate configuration that does not survive the outage of
// one availability zone.
//
// The provided ConfChangeTransition specifies how (and whether) Joint Consensus
// is used, and assigns the task of leaving the joint configuration either to
// Raft or the application. Leaving the joint configuration is accomplished by
// proposing a ConfChangeV2 with only and optionally the Context field
// populated.
//
// For details on Raft membership changes, see:
//
// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
type ConfChangeV2 struct {
	Transition ConfChangeTransition
	Changes    []ConfChangeSingle
	Context    []byte
}

func (c ConfChangeV2) String() string {
	return fmt.Sprintf("{%s %v %v}",
		c.Transition, c.Changes, c.Context)
}
func (c ConfChangeV2) Size() int {
	return coder.Int8Size(int8(c.Transition)) +
		coder.ArraySize(c.Changes) +
		coder.DataSize(c.Context)
}
func (c ConfChangeV2) WriteTo(w coder.Encoder) error {
	w.WriteInt8(int8(c.Transition))
	clen := uint64(len(c.Changes))
	w.WriteVarint(clen)
	for _, v := range c.Changes {
		v.WriteTo(w)
	}
	w.WriteData(c.Context)
	return nil
}
func (c *ConfChangeV2) ReadFrom(r coder.Decoder) error {
	transition, err := r.ReadInt8()
	if err != nil {
		return err
	}
	clen, err := r.ReadVarint()
	if err != nil {
		return err
	}
	for i := 0; i < int(clen); i++ {
		var change ConfChangeSingle
		if err = change.ReadFrom(r); err != nil {
			return err
		}
		c.Changes = append(c.Changes, change)
	}
	context, err := r.ReadData()
	if err != nil {
		return err
	}
	c.Transition = ConfChangeTransition(transition)
	c.Context = context
	return nil
}

func (c *ConfChangeV2) Equal(o *ConfChangeV2) bool {
	return c.Transition == o.Transition &&
		len(c.Changes) == len(o.Changes) &&
		reflect.DeepEqual(c.Changes, o.Changes) &&
		reflect.DeepEqual(c.Context, o.Context)
}
func (c ConfChangeV2) Marshal() ([]byte, error) {
	return coder.Marshal(c)
}
func (c *ConfChangeV2) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return coder.Unmarshal(data, c)
}

// var (
// 	ErrInvalidLengthRaft        = fmt.Errorf("proto: negative length found during unmarshaling")
// 	ErrIntOverflowRaft          = fmt.Errorf("proto: integer overflow")
// 	ErrUnexpectedEndOfGroupRaft = fmt.Errorf("proto: unexpected end of group")
// )
