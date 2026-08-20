package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
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
	TakeSnapshot() ([]byte, error)
	RestoreSnapshot(snapshot []byte) error
	ApplyCommits(commit *commit) error
}

type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan *commit             // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id        uint64
	join      bool
	peers     []raft.Peer
	peerURLs  map[uint64]string
	fsm       FSM
	ss        snapshotStorage
	transport *transport

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64
	snapCount     uint64

	node        raft.Node
	raftStorage *raft.MemoryStorage
	stopc       chan struct{} // signals proposal channel closed
	donec       chan struct{} // signals serveChannels has exited
	err         error
}

var DefaultSnapshotCount uint64 = 10000

func newRaftNode(id uint64, peerURLs map[uint64]string, join bool, fsm FSM, ss snapshotStorage, proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) *raftNode {
	commitC := make(chan *commit)
	errorC := make(chan error)

	rpeers := make([]raft.Peer, 0, len(peerURLs))
	for pID := range peerURLs {
		rpeers = append(rpeers, raft.Peer{ID: pID})
	}

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		join:        join,
		peers:       rpeers,
		peerURLs:    peerURLs,
		fsm:         fsm,
		ss:          ss,
		snapCount:   DefaultSnapshotCount,
		stopc:       make(chan struct{}),
		donec:       make(chan struct{}),
		transport:   newTransport(id, peerURLs),
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

	if hasState || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rc.peers)
	}

	rc.transport.node = rc.node

	go rc.serveRaft()
	go rc.serveChannels()
}

func (rc *raftNode) serveRaft() {
	u, err := url.Parse(rc.peerURLs[rc.id])
	if err != nil {
		log.Fatalf("raftsimple: failed to parse peer URL: %v", err)
	}

	ln, err := net.Listen("tcp", u.Host)
	if err != nil {
		log.Fatalf("raftsimple: failed to listen on %s: %v", u.Host, err)
	}

	rc.transport.mu.Lock()
	rc.transport.server = &http.Server{Handler: rc.transport.Handler()}
	rc.transport.mu.Unlock()

	err = rc.transport.server.Serve(ln)
	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("raftsimple: failed to serve raft HTTP: %v", err)
	}
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
				if len(cc.Context) > 0 {
					rc.transport.addPeer(cc.NodeID, string(cc.Context))
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rc.id {
					log.Printf("Node %d: I've been removed from the cluster! Shutting down.", rc.id)
					return nil, false
				}
				rc.transport.removePeer(cc.NodeID)
			}
		}
	}
	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{data, applyDoneC}:
		case <-rc.stopc:
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

	go func() {
		confChangeCount := uint64(0)
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
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
		close(rc.stopc)
	}()

	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.ss.saveSnap(rd.Snapshot)
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				rc.ss.saveHardState(rd.HardState)
			}
			if len(rd.Entries) > 0 {
				rc.ss.saveEntries(rd.Entries)
			}

			rc.transport.send(rd.Messages)

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
	rc.transport.Close()
	rc.node.Stop()
}
