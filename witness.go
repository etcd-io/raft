package raft

import (
	"errors"

	"go.etcd.io/raft/v3/raftpb"
)

var ErrWitnessStateModified = errors.New("witness state was modified")

type WitnessStorage interface {
	GetWitnessState() (WitnessState, error)
	Close()
}

type WitnessState interface {
	GetState() raftpb.WitnessHardState
	Update(st *raftpb.WitnessHardState) error
}

type WitnessConfig struct {
	ID          uint64
	Storage     WitnessStorage
	CheckQuorum bool
	PreVote     bool
}

type Witness interface {
	Process(m WitnessMessage) (*raftpb.Message, error)
	Close()
}

type witness struct {
	id          uint64
	storage     WitnessStorage
	preVote     bool
	checkQuorum bool
	state       raftpb.WitnessHardState
	lastError   error
}

func NewWitness(cfg WitnessConfig) Witness {
	return &witness{
		id:          cfg.ID,
		storage:     cfg.Storage,
		preVote:     cfg.PreVote,
		checkQuorum: cfg.CheckQuorum,
	}
}

func (w *witness) Close() {
	w.storage.Close()
}

func (w *witness) Process(m WitnessMessage) (*raftpb.Message, error) {
	switch m.Type {
	case raftpb.MsgApp, raftpb.MsgVote, raftpb.MsgPreVote, raftpb.MsgHeartbeat:
	default:
		return nil, nil
	}

	resp, handled := w.processNoUpdate(m)
	if handled {
		return resp, nil
	}

	for {
		state, err := w.storage.GetWitnessState()
		w.lastError = err
		if err != nil {
			return nil, err
		}
		w.state = state.GetState()

		resp, handled = w.processNoUpdate(m)
		if handled {
			return resp, nil
		}

		st := w.cloneState()
		if m.Term > st.State.Term {
			w.processMessageWithNewerTerm(m, &st)
		}
		switch m.Type {
		case raftpb.MsgHeartbeat:
			resp = w.handleHeartbeat(m)
		case raftpb.MsgApp:
			resp = w.handleAppend(m, &st)
		case raftpb.MsgPreVote, raftpb.MsgVote:
			resp = w.handleVote(m, &st)
		default:
			return nil, nil
		}

		err = state.Update(&st)
		if err != ErrWitnessStateModified {
			w.lastError = err
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
	}
}

func (w *witness) isHealthy() bool {
	return w.lastError == nil
}

func (w *witness) processNoUpdate(m WitnessMessage) (*raftpb.Message, bool) {
	if !w.isHealthy() && m.Type == raftpb.MsgHeartbeat {
		return nil, true
	}

	if m.Term < w.state.State.Term {
		resp := w.processMessageWithOlderTerm(m)
		return resp, true
	}

	var resp *raftpb.Message
	handled := false
	if m.Term == w.state.State.Term {
		switch m.Type {
		case raftpb.MsgHeartbeat:
			resp = w.handleHeartbeat(m)
			handled = true
		case raftpb.MsgApp:
			resp, handled = w.handleAppendNoUpdate(m)
		}
	}

	return resp, handled
}

func (w *witness) processMessageWithOlderTerm(m WitnessMessage) *raftpb.Message {
	var resp *raftpb.Message
	if (w.checkQuorum || w.preVote) && (m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgApp) {
		resp = &raftpb.Message{To: m.From, Type: raftpb.MsgAppResp}
	} else if m.Type == raftpb.MsgPreVote {
		resp = &raftpb.Message{To: m.From, Term: w.state.State.Term, Type: raftpb.MsgPreVoteResp, Reject: true}
	} else {
		resp = nil
	}

	return resp
}

func (w *witness) processMessageWithNewerTerm(m WitnessMessage, st *raftpb.WitnessHardState) {
	switch {
	case m.Type == raftpb.MsgPreVote:
	default:
		if m.Type == raftpb.MsgApp || m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgSnap {
			st.State.Term = m.Term
			st.State.Vote = None
			st.Lead = m.From
		} else {
			st.State.Term = m.Term
			st.State.Vote = None
			st.Lead = None
		}
	}
}

func (w *witness) cloneState() raftpb.WitnessHardState {
	st := raftpb.WitnessHardState{
		State: raftpb.HardState{
			Term:   w.state.State.Term,
			Vote:   w.state.State.Vote,
			Commit: w.state.State.Commit,
		},
		LastLogIndex:           w.state.LastLogIndex,
		LastLogTerm:            w.state.LastLogTerm,
		LastLogSubterm:         w.state.LastLogSubterm,
		Lead:                   w.state.Lead,
		ReplicationSet:         make([]uint64, len(w.state.ReplicationSet)),
		ReplicationSetOutgoing: make([]uint64, len(w.state.ReplicationSetOutgoing)),
	}
	copy(st.ReplicationSet, w.state.ReplicationSet)
	copy(st.ReplicationSetOutgoing, w.state.ReplicationSetOutgoing)

	return st
}

func (w *witness) handleHeartbeat(m WitnessMessage) *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgHeartbeatResp,
		Term:    w.state.State.Term,
		From:    w.id,
		To:      m.From,
		Context: m.Context,
	}
}

func (w *witness) handleAppendNoUpdate(m WitnessMessage) (*raftpb.Message, bool) {
	resp := &raftpb.Message{
		Type: raftpb.MsgAppResp,
		To:   m.From,
		From: w.id,
		Term: w.state.State.Term,
	}

	handled := true
	if m.LastLogTerm == w.state.LastLogTerm && m.LastLogSubterm == w.state.LastLogSubterm {
		// leader commits upon quorum-1 acks if the replicated entry is of same subterm as witness
		resp.Index = m.LastLogIndex
	} else if m.LastLogTerm < w.state.LastLogTerm || (m.LastLogTerm == w.state.LastLogTerm && m.LastLogSubterm < w.state.LastLogSubterm) {
		// previous replicated entry
		resp.Index = w.state.LastLogIndex
	} else {
		handled = false
	}

	return resp, handled
}

func (w *witness) handleAppend(m WitnessMessage, st *raftpb.WitnessHardState) *raftpb.Message {
	resp, handled := w.handleAppendNoUpdate(m)
	if handled {
		return resp
	}

	st.LastLogTerm = m.LastLogTerm
	st.LastLogSubterm = m.LastLogSubterm
	st.LastLogIndex = m.LastLogIndex
	st.ReplicationSet = m.ReplicationSet[0]
	st.ReplicationSetOutgoing = m.ReplicationSet[1]

	return &raftpb.Message{
		Type:  raftpb.MsgAppResp,
		To:    m.From,
		From:  w.id,
		Term:  w.state.State.Term,
		Index: st.LastLogIndex,
	}
}

func (w *witness) handleVote(m WitnessMessage, st *raftpb.WitnessHardState) *raftpb.Message {
	var msgType raftpb.MessageType
	if m.Type == raftpb.MsgPreVote {
		msgType = raftpb.MsgPreVoteResp
	} else {
		msgType = raftpb.MsgVoteResp
	}
	resp := &raftpb.Message{
		Type: msgType,
		To:   m.From,
		From: w.id,
	}

	canVote := st.State.Vote == m.From || (st.State.Vote == 0 && st.Lead == 0) || (m.Type == raftpb.MsgPreVote && m.Term > st.State.Term)
	reject := true
	if canVote {
		reject = false
		if m.LastLogTerm < st.LastLogTerm || (m.LastLogTerm == st.LastLogTerm && m.LastLogSubterm < st.LastLogSubterm) {
			reject = true
		} else if m.LastLogTerm == st.LastLogTerm && m.LastLogSubterm == st.LastLogSubterm {
			votesInSet := 0
			for _, x := range st.ReplicationSet {
				if _, ok := m.Votes[x]; ok {
					votesInSet++
				}
			}
			reject = votesInSet != len(m.Votes)
		}
	}

	if m.Type == raftpb.MsgVote && !reject {
		st.State.Vote = m.From
	}

	resp.Reject = reject

	if resp.Reject {
		resp.Term = st.State.Term
	} else {
		resp.Term = m.Term
	}

	return resp
}
