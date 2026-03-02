package main

import (
	"context"
	"log"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type network struct {
	mu    sync.RWMutex
	peers map[uint64]*raftNode
}

func (nw *network) register(nodeID uint64, rn *raftNode) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for _, node := range nw.peers {
		node.t.addPeer(nodeID)
	}
	nw.peers[nodeID] = rn
}

func (nw *network) deregister(nodeID uint64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.peers, nodeID)
}

func (nw *network) send(m raftpb.Message) {
	nw.mu.RLock()
	p, ok := nw.peers[m.To]
	nw.mu.RUnlock()

	if !ok {
		log.Printf("node %d: unable to find node %d to send %s\n", m.From, m.To, m.Type.String())
		return
	}

	if p.node == nil {
		// Node is registered but not yet initialized. Drop the message.
		// Raft will retry once the node is up.
		return
	}

	_ = p.node.Step(context.TODO(), m)
}

func (nw *network) getNode(nodeID uint64) (*raftNode, bool) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	n, ok := nw.peers[nodeID]
	return n, ok
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
		t.nw.send(m)
	}
}

func (t *transport) leave() {
	t.nw.deregister(t.id)
}
