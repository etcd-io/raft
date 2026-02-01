package main

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

type network struct {
	peers map[uint64]*raftNode
}

func (nw *network) register(nodeID uint64, rn *raftNode) {
	for _, node := range nw.peers {
		node.t.addPeer(nodeID)
	}
	nw.peers[nodeID] = rn
}

func (nw *network) deregister(nodeID uint64) {
	delete(nw.peers, nodeID)
}

func (nw *network) send(m raftpb.Message) {
	p := nw.peers[m.To]
	_ = p.node.Step(context.TODO(), m)
}

func (nw *network) exists(nodeID uint64) bool {
	_, ok := nw.peers[nodeID]
	return ok
}

type transport struct {
	id    uint64
	peers map[uint64]bool
	nw    *network
}

func (t *transport) addPeer(nodeID uint64) {
	t.peers[nodeID] = true
}

func (t *transport) removePeer(nodeID uint64) {
	delete(t.peers, nodeID)
}

func (t *transport) send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		// if m.Type != 8 && m.Type != 9 {
		// 	log.Printf("node %d: sending %s msg to node %d\n", m.From, m.Type.String(), m.To)
		// 	for k, _ := range t.nw.peers {
		// 		log.Print(k)
		// 	}
		// }
		t.nw.send(m)
	}
}

func (t *transport) leave() {
	t.nw.deregister(t.id)
}
