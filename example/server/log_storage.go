package server

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	log "github.com/sirupsen/logrus"
)

type raftLogStorage struct {
	nodeId    uint64
	walMu     sync.RWMutex
	wal       *raft.MemoryStorage
	ssc       *SnapshotCollection
	sm        StateMachine
	cs        pb.ConfState
	memberMu  sync.RWMutex
	members   map[uint64]Member
	applied   uint64
	snapIndex uint64
}

func NewRaftLogStorage(nodeId uint64, sm StateMachine, shotter *SnapshotCollection) (*raftLogStorage, error) {
	rs := &raftLogStorage{
		nodeId:  nodeId,
		ssc:     shotter,
		members: make(map[uint64]Member),
		sm:      sm,
	}

	rs.wal = raft.NewMemoryStorage()

	return rs, nil
}

func (s *raftLogStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	hs, _, err := s.wal.InitialState()
	return hs, s.cs, err
}

func (s *raftLogStorage) Term(index uint64) (uint64, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.Term(index)
}

func (s *raftLogStorage) LastIndex() (uint64, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.LastIndex()
}

func (s *raftLogStorage) FirstIndex() (uint64, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.FirstIndex()
}

func (s *raftLogStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.Entries(lo, hi, maxSize)
}

func (s *raftLogStorage) Snapshot() (pb.Snapshot, error) {
	var members []*Member
	s.memberMu.RLock()
	cs := s.cs
	for _, m := range s.members {
		member := &Member{}
		*member = m
		members = append(members, member)
	}
	s.memberMu.RUnlock()

	st, err := s.sm.Snapshot()
	if err != nil {
		return pb.Snapshot{}, err
	}
	snapIndex := st.Index()
	snapTerm, err := s.Term(snapIndex)
	if err != nil {
		st.Close()
		return pb.Snapshot{}, err
	}
	if snapIndex > s.Applied() {
		st.Close()
		return pb.Snapshot{}, fmt.Errorf("snapIndex(%d) greater than applied(%d)", snapIndex, s.Applied())
	}
	name := st.Name()
	snap := &snapshot{
		st: st,
		meta: SnapshotMeta{
			Name:     name,
			Index:    snapIndex,
			Term:     snapTerm,
			Mbs:      members,
			Voters:   cs.Voters,
			Learners: cs.Learners,
		},
	}
	runtime.SetFinalizer(snap, func(snap *snapshot) {
		snap.Close()
	})
	if err = s.ssc.Set(snap); err != nil {
		log.Errorf("set snapshot(%s) error: %v", name, err)
		return pb.Snapshot{}, err
	}
	log.Infof("generator a snapshot(%s)", name)
	return pb.Snapshot{
		Data: []byte(name),
		Metadata: pb.SnapshotMetadata{
			ConfState: cs,
			Index:     snapIndex,
			Term:      snapTerm,
		},
	}, nil
}

func (s *raftLogStorage) SaveEntries(entries []pb.Entry) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.wal.Append(entries)
}

func (s *raftLogStorage) SaveHardState(hs pb.HardState) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.wal.SetHardState(hs)
}

func (s *raftLogStorage) SetApplied(applied uint64) {
	atomic.StoreUint64(&s.applied, applied)
}

func (s *raftLogStorage) SetSnapIndex(index uint64) {
	atomic.StoreUint64(&s.snapIndex, index)
}

func (s *raftLogStorage) Applied() uint64 {
	return atomic.LoadUint64(&s.applied)
}

func (s *raftLogStorage) Truncate(index uint64) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.wal.Compact(index)
}

func (s *raftLogStorage) ApplySnapshot(st pb.Snapshot) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	if err := s.wal.ApplySnapshot(st); err != nil {
		return err
	}
	s.SetApplied(st.Metadata.Index)
	return nil
}

func (s *raftLogStorage) confState() pb.ConfState {
	var cs pb.ConfState

	for _, m := range s.members {
		if m.Learner {
			cs.Learners = append(cs.Learners, m.NodeID)
		} else {
			cs.Voters = append(cs.Voters, m.NodeID)
		}
	}

	return cs
}

func (s *raftLogStorage) AddMembers(m Member) {
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	s.members[m.NodeID] = m
	s.cs = s.confState()
}

func (s *raftLogStorage) RemoveMember(id uint64) {
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	delete(s.members, id)
	s.cs = s.confState()
}

func (s *raftLogStorage) SetMembers(members []*Member) {
	mbs := make(map[uint64]Member)
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	for i := 0; i < len(members); i++ {
		mbs[members[i].NodeID] = *members[i]
	}
	s.members = mbs
	s.cs = s.confState()
}

func (s *raftLogStorage) GetMember(id uint64) (Member, bool) {
	s.memberMu.RLock()
	defer s.memberMu.RUnlock()
	m, hit := s.members[id]
	return m, hit
}

func (s *raftLogStorage) Close() {
}
