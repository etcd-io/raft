package main

import (
	"context"

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

func (nw *network) addPeer(nodeID uint64, node *raftNode) {
	nw.peers[nodeID] = node
}

func (nw *network) removePeer(nodeID uint64) {
	delete(nw.peers, nodeID)
}
