package main

import (
	"context"
	"log"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

type raftNode struct {
	proposeC    <-chan string
	confChangeC <-chan raftpb.ConfChange
	commitC     chan<- *commit
	errorC      chan<- error

	id    uint64
	peers []raft.Peer

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	nw *network

	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

func newRaftNode(id uint64, peers []uint64, nw *network, proposeC <-chan string, confChangeC <-chan raftpb.ConfChange, commitC chan<- *commit, errorC chan<- error) *raftNode {
	rpeers := make([]raft.Peer, len(peers))
	for i, pID := range peers {
		rpeers[i] = raft.Peer{ID: pID}
	}

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       rpeers,
		nw:          nw,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
	}

	go rc.startRaft()
	return rc
}

func (rc *raftNode) startRaft() {
	rc.raftStorage = raft.NewMemoryStorage()

	c := &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	rc.node = raft.StartNode(c, rc.peers)

	go rc.serveChannels()
}

func (rc *raftNode) serveChannels() {
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
			rc.raftStorage.Append(rd.Entries)

			rc.nw.send(rd.Messages)

			// apply committedEntries
			data := make([]string, 0, len(rd.CommittedEntries))
			for _, entry := range rd.CommittedEntries {
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
					rc.node.ApplyConfChange(cc)
					switch cc.Type {
					case raftpb.ConfChangeAddNode:
						// TODO: add node to network
					case raftpb.ConfChangeRemoveNode:
						if cc.NodeID == rc.id {
							log.Println("I've been removed from the cluster! Shutting down.")
							rc.stop()
							return
						}
						// TODO: remove node from the network
						rc.nw.removePeer(cc.NodeID)
					}
				}
			}
			var applyDoneC chan struct{}
			if len(data) > 0 {
				applyDoneC = make(chan struct{}, 1)
				select {
				case rc.commitC <- &commit{data, applyDoneC}:
				case <-rc.stopc:
					rc.stop()
					return
				}
			}
			// TODO: after commit, update appliedIndex
			rc.node.Advance()
		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) stop() {
	log.Println("Executing stop()")
	// TODO: stop the network layer
	// TODO: stop the KVStore HTTP layer
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}
