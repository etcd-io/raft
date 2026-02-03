package main

import (
	"fmt"
	"log"
	"os"

	"go.etcd.io/raft/v3/raftpb"
)

type snapshotStorage interface {
	// SaveSnap saves `snapshot` to persistent storage.
	saveSnap(snapshot raftpb.Snapshot) error

	// saveHardState(hardstate raftpb.HardState) error

	// saveEntries(entries []raftpb.Entry) error

	// Load reads and returns the newest snapshot that is
	// available.
	load() (*raftpb.Snapshot, error)

	// // LoadNewestAvailable loads the newest available snapshot
	// // whose term and index matches one of those in walSnaps.
	// LoadNewestAvailable(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
}

type snapStore struct {
	dir string
}

func (ss *snapStore) saveSnap(snapshot raftpb.Snapshot) error {
	log.Println("ss: saveSnap is being called")
	snap, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	// name := fmt.Sprintf("snap_%d_%d", snapshot.Metadata.Term, snapshot.Metadata.Index)
	return ss.saveToFile("snap", snap)
}

// func (ss *snapStore) saveHardState(hardstate raftpb.HardState) error {
// 	hs, err := hardstate.Marshal()
// 	if err != nil {
// 		return err
// 	}
// 	name := fmt.Sprintf("hardstate_%d_%d", hardstate.Term, hardstate)
// 	return ss.saveToFile(name, snap)
// }

func (ss *snapStore) saveToFile(name string, data []byte) error {
	if err := os.WriteFile(fmt.Sprintf("%s/%s", ss.dir, name), data, 0644); err != nil {
		return err
	}
	return nil
}

func (ss *snapStore) load() (*raftpb.Snapshot, error) {
	data, err := os.ReadFile(fmt.Sprintf("%s/snap", ss.dir))
	if err != nil {
		return nil, err
	}

	var newSnap raftpb.Snapshot
	if err := newSnap.Unmarshal(data); err != nil {
		return nil, err
	}
	return &newSnap, nil
}

func newSnapshotStorage(dir string) (snapshotStorage, error) {
	err := os.MkdirAll(dir, 0750)
	if err != nil {
		return nil, err
	}
	ss := snapStore{dir: dir}
	return &ss, nil
}
