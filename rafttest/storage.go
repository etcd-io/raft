package rafttest

import (
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type snapOverrideStorage struct {
	Storage
	snapshotOverride func() (pb.Snapshot, error)
	truncatedFrom    pb.Snapshot
}

func newSnapOverrideStorage(f func() (pb.Snapshot, error)) *snapOverrideStorage {
	return &snapOverrideStorage{
		Storage:          raft.NewMemoryStorage(),
		snapshotOverride: f,
	}
}

func (s *snapOverrideStorage) Snapshot() (pb.Snapshot, error) {
	if s.snapshotOverride != nil {
		return s.snapshotOverride()
	}
	return s.Storage.Snapshot()
}

func (s *snapOverrideStorage) TruncateAt() error {
	var err error
	s.truncatedFrom, err = s.snapshotOverride()
	return err
}

func (s *snapOverrideStorage) Truncate() error {
	s.Storage = raft.NewMemoryStorage()
	err := s.Storage.ApplySnapshot(s.truncatedFrom)
	if err != nil {
		return err
	}
	return nil
}
