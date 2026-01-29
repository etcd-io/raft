package main

import (
	"context"
	"log"

	"go.etcd.io/raft/v3/raftpb"
)

type network struct {
	peers map[uint64]*raftNode
}

func (nw *network) send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		p := nw.peers[m.To]
		_ = p.node.Step(context.TODO(), m)
	}
}

func (nw *network) removePeer(nodeID uint64) {
	log.Printf("Deleting node %d from peers list\n", nodeID)
	delete(nw.peers, nodeID)
}
