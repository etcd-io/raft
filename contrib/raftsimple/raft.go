package main

import (
	"context"
	"log"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
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
	peers []string

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}

func newRaftNode(id uint64, peers []string, proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) (<-chan *commit, <-chan error) {
	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		logger:      zap.NewExample(),
	}

	go rc.startRaft()
	return commitC, errorC
}

func (rc *raftNode) startRaft() {
	rc.raftStorage = raft.NewMemoryStorage()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	c := &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	rc.node = raft.StartNode(c, rpeers)

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
					// default:
					// 	log.Fatal("Unknown entry type when committing.")
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
