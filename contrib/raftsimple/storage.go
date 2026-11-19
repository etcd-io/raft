package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.etcd.io/raft/v3/raftpb"
)

type snapshotStorage interface {
	saveSnap(snapshot raftpb.Snapshot) error
	loadSnap() (*raftpb.Snapshot, error)

	saveHardState(st raftpb.HardState) error
	loadHardState() (*raftpb.HardState, error)

	saveEntries(entries []raftpb.Entry) error
	loadEntries() ([]raftpb.Entry, error)
}

type snapStore struct {
	dir string
}

func newSnapshotStorage(dir string) (snapshotStorage, error) {
	err := os.MkdirAll(dir, 0o750)
	if err != nil {
		return nil, err
	}
	ss := snapStore{dir: dir}
	return &ss, nil
}

func (ss *snapStore) saveSnap(snapshot raftpb.Snapshot) error {
	snap, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	return ss.saveToFile("snap", snap)
}

func (ss *snapStore) loadSnap() (*raftpb.Snapshot, error) {
	data, err := os.ReadFile(fmt.Sprintf("%s/snap", ss.dir))
	if err != nil {
		if os.IsNotExist(err) {
			return &raftpb.Snapshot{}, nil
		}
		return nil, err
	}

	var newSnap raftpb.Snapshot
	if err := newSnap.Unmarshal(data); err != nil {
		return nil, err
	}
	return &newSnap, nil
}

func (ss *snapStore) saveHardState(st raftpb.HardState) error {
	data, err := st.Marshal()
	if err != nil {
		return err
	}
	return ss.saveToFile("hardstate", data)
}

func (ss *snapStore) loadHardState() (*raftpb.HardState, error) {
	data, err := os.ReadFile(fmt.Sprintf("%s/hardstate", ss.dir))
	if err != nil {
		if os.IsNotExist(err) {
			return &raftpb.HardState{}, nil
		}
		return nil, err
	}

	var st raftpb.HardState
	if err := st.Unmarshal(data); err != nil {
		return nil, err
	}
	return &st, nil
}

func (ss *snapStore) saveEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/wal", ss.dir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, ent := range entries {
		data, err := ent.Marshal()
		if err != nil {
			return err
		}

		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err := f.Write(lenBuf); err != nil {
			return err
		}

		if _, err := f.Write(data); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (ss *snapStore) loadEntries() ([]raftpb.Entry, error) {
	walPath := fmt.Sprintf("%s/wal", ss.dir)
	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var entries []raftpb.Entry
	lenBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(f, lenBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				return entries, fmt.Errorf("corrupted wal: incomplete length prefix")
			}
			return entries, err
		}

		entryLen := binary.LittleEndian.Uint32(lenBuf)
		dataBuf := make([]byte, entryLen)

		_, err = io.ReadFull(f, dataBuf)
		if err != nil {
			return entries, fmt.Errorf("corrupted wal: unable to read full entry payload: %v", err)
		}

		var ent raftpb.Entry
		if err := ent.Unmarshal(dataBuf); err != nil {
			return entries, fmt.Errorf("failed to unmarshal entry: %v", err)
		}

		entries = append(entries, ent)
	}

	return entries, nil
}

func (ss *snapStore) saveToFile(name string, data []byte) error {
	if err := os.WriteFile(fmt.Sprintf("%s/%s", ss.dir, name), data, 0o644); err != nil {
		return err
	}
	return nil
}
