package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

// FSM is the interface that must be implemented by a finite state
// machine for it to be driven by raft.
type FSM interface {
	// TakeSnapshot takes a snapshot of the current state of the
	// finite state machine, returning the snapshot as a slice of
	// bytes that can be saved or loaded by a `SnapshotStorage`.
	TakeSnapshot() ([]byte, error)

	// RestoreSnapshot restores the finite state machine to the state
	// represented by `snapshot` (which, in turn, was returned by
	// `TakeSnapshot`).
	RestoreSnapshot(snapshot []byte) error

	// ApplyCommits applies the changes from `commit` to the finite
	// state machine. `commit` is never `nil`. (By contrast, the
	// commits that are handled by `ProcessCommits()` can be `nil` to
	// signal that a snapshot should be loaded.)
	ApplyCommits(commit *commit) error
}

type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan *commit             // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id    uint64
	peers []raft.Peer
	fsm   FSM

	ss            snapshotStorage
	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64
	snapCount     uint64
	t             transport

	// When serveChannels is done, `err` is set to any error and then
	// `done` is closed.
	err   error
	donec chan struct{}

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	stopc chan struct{} // signals proposal channel closed
	// httpstopc chan struct{} // signals http server to shutdown
	// httpdonec chan struct{} // signals http server shutdown complete
}

var DefaultSnapshotCount uint64 = 10000

func newRaftNode(id uint64, peers []uint64, fsm FSM, ss snapshotStorage, nw *network, proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) *raftNode {
	commitC := make(chan *commit)
	errorC := make(chan error)

	t := transport{id: id, peers: make(map[uint64]bool, len(peers)), nw: nw}
	rpeers := make([]raft.Peer, len(peers))
	for i, pID := range peers {
		rpeers[i] = raft.Peer{ID: pID}
		t.peers[pID] = true
	}

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		donec:       make(chan struct{}),
		id:          id,
		peers:       rpeers,
		fsm:         fsm,
		ss:          ss,
		snapCount:   DefaultSnapshotCount,
		t:           t,
		stopc:       make(chan struct{}),
		// httpstopc:   make(chan struct{}),
		// httpdonec:   make(chan struct{}),
	}

	go rc.startRaft()
	return rc
}

func (rc *raftNode) startRaft() {
	rc.raftStorage = raft.NewMemoryStorage()

	hasState := rc.replayWAL()

	c := &raft.Config{
		ID:              rc.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	if hasState || len(rc.peers) == 0 {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rc.peers)
	}

	go rc.serveChannels()
}

func (rc *raftNode) replayWAL() bool {
	hasState := false

	snapshot, err := rc.ss.loadSnap()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Panic(err)
	}
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		rc.raftStorage.ApplySnapshot(*snapshot)
		hasState = true
	}

	hs, err := rc.ss.loadHardState()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Panic(err)
	}
	if hs != nil && !raft.IsEmptyHardState(*hs) {
		rc.raftStorage.SetHardState(*hs)
		hasState = true
	}

	ents, err := rc.ss.loadEntries()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Panic(err)
	}
	if len(ents) > 0 {
		rc.raftStorage.Append(ents)
		hasState = true
	}

	return hasState
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.fsm.TakeSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.ss.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		if !errors.Is(err, raft.ErrCompacted) {
			panic(err)
		}
	} else {
		log.Printf("compacted log at index %d", compactIndex)
	}

	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		return ents[rc.appliedIndex-firstIdx+1:]
	}
	return nil
}

func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for _, entry := range ents {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(entry.Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entry.Data)
			rc.confState = *rc.node.ApplyConfChange(cc)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				rc.t.addPeer(cc.NodeID)
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rc.id {
					log.Printf("Node %d: I've been removed from the cluster! Shutting down.", rc.id)
					return nil, false
				}
				rc.t.removePeer(cc.NodeID)
			}
		}
	}
	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{data, applyDoneC}:
		case <-rc.stopc:
			log.Println("stopping at applying commits")
			return nil, false
		}
	}
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {

			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}
			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		// store raft entries, then publish over commit channel
		case rd := <-rc.node.Ready():
			// saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.ss.saveSnap(rd.Snapshot)
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				rc.ss.saveHardState(rd.HardState)
			}
			if len(rd.Entries) > 0 {
				rc.ss.saveEntries(rd.Entries)
			}

			rc.t.send(rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)

			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}

			rc.maybeTriggerSnapshot(applyDoneC)
			rc.node.Advance()
		case <-rc.stopc:
			log.Println("stopping at serveChannels")
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) loadAndApplySnapshot() error {
	snapshot, err := rc.ss.loadSnap()
	if err != nil {
		return err
	}
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		log.Printf("loading snapshot at index %d", snapshot.Metadata.Index)
		if err := rc.fsm.RestoreSnapshot(snapshot.Data); err != nil {
			return err
		}
	}
	return nil
}

func (rc *raftNode) processCommits() error {
	if err := rc.loadAndApplySnapshot(); err != nil {
		return err
	}
	for commit := range rc.commitC {
		if commit == nil {
			if err := rc.loadAndApplySnapshot(); err != nil {
				return err
			}
			continue
		}
		if err := rc.fsm.ApplyCommits(commit); err != nil {
			return err
		}
	}
	<-rc.donec
	return rc.err
}

func (rc *raftNode) stop() {
	log.Printf("node %d: Executing stop()\n", rc.id)
	close(rc.commitC)
	close(rc.errorC)
	close(rc.donec)
	rc.t.leave()
	rc.node.Stop()
}
