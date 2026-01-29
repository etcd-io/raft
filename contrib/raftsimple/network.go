package main

import (
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
		log.Printf("Sending to node %d\n", m.To)
	}

}
