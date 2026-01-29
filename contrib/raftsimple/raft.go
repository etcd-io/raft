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
	proposeC <-chan string
	commitC  chan<- *commit
	errorC   chan<- error

	id    uint64
	peers []raft.Peer

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

func newRaftNode(id uint64, peers []uint64, proposeC <-chan string) (<-chan *commit, <-chan error) {
	log.Printf("Creating raft node %d\n", id)
	commitC := make(chan *commit)
	errorC := make(chan error)

	rpeers := make([]raft.Peer, len(peers))
	for i, pID := range peers {
		rpeers[i] = raft.Peer{ID: pID}
	}

	rc := &raftNode{
		proposeC:  proposeC,
		commitC:   commitC,
		errorC:    errorC,
		id:        id,
		peers:     rpeers,
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),
	}

	go rc.startRaft()
	return commitC, errorC
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

	log.Println("start node")
	rc.node = raft.StartNode(c, rc.peers)

	go rc.serveChannels()
}

func (rc *raftNode) serveChannels() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		for rc.proposeC != nil {
			prop, ok := <-rc.proposeC
			if !ok {
				rc.proposeC = nil
			} else {
				// blocks until accepted by raft state machine
				rc.node.Propose(context.TODO(), []byte(prop))
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
			rc.raftStorage.Append(rd.Entries)
			data := make([]string, 0, len(rd.Entries))
			for i := range rd.Entries {
				entry := rd.Entries[i]
				switch entry.Type {
				case raftpb.EntryNormal:
					if len(entry.Data) == 0 {
						// ignore empty messages
						break
					}
					s := string(entry.Data)
					data = append(data, s)
				default:
					// log.Fatal("Unknown entry type when committing.")
					log.Printf("Unknown entry type: %s\n", entry.Type.String())
				}
			}
			var applyDoneC chan struct{}
			if len(rd.Entries) > 0 {
				applyDoneC = make(chan struct{}, 1)
				select {
				case rc.commitC <- &commit{data, applyDoneC}:
				case <-rc.stopc:
					log.Fatal("serveChannels stopping 2")
					return
				}
			}
			// TODO: after commit, update appliedIndex
			rc.node.Advance()
		case <-rc.stopc:
			log.Fatal("serveChannels stopping 1")
			return
		}
	}
}
