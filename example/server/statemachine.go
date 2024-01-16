package server

import (
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type Snapshot interface {
	Read() ([]byte, error)
	Name() string
	Index() uint64
	Close()
}

// The StateMachine interface is supplied by the application to persist/snapshot data of application.
type StateMachine interface {
	Apply(data [][]byte, index uint64) error
	ApplyMemberChange(cc ConfChange, index uint64) error
	Snapshot() (Snapshot, error)
	ApplySnapshot(meta SnapshotMeta, st Snapshot) error

	UpdateCommittedIndex(index uint64)
	LeaderChange(leader uint64, host string)
}

func (s *Server) Lookup(key string) (string, bool) {
	return s.engine.Get(key)
}

// Server
func (s *Server) Apply(data [][]byte, index uint64) error {
	s.updateApplyIndex(index)

	return s.engine.Apply(data)
}

func (s *Server) ApplyMemberChange(cc ConfChange, index uint64) error {
	s.updateApplyIndex(index)

	//TODO implement me
	log.Warning("implement me ApplyMemberChange")
	return nil
}

func (s *Server) Snapshot() (Snapshot, error) {
	//TODO implement me
	log.Warning("implement me Snapshot")
	return nil, nil
}

func (s *Server) ApplySnapshot(meta SnapshotMeta, st Snapshot) error {
	//TODO implement me
	log.Warning("implement me ApplySnapshot")

	return nil
}

func (s *Server) updateApplyIndex(index uint64) {
	if atomic.LoadUint64(&s.appliedIndex) < index {
		atomic.StoreUint64(&s.appliedIndex, index)
	}
}

func (s *Server) UpdateCommittedIndex(index uint64) {
	if atomic.LoadUint64(&s.committedIndex) < index {
		atomic.StoreUint64(&s.committedIndex, index)
	}
}

func (s *Server) LeaderChange(leaderID uint64, host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	member := Member{
		NodeID:  leaderID,
		Host:    host,
		Learner: false,
	}
	s.leader = member
	log.Infof("receive leader change, member %s", member.String())
}
