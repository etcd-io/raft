package main

import (
	"flag"
	"log"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type orchest struct {
	raftNodes map[uint64]*raftNode
	wg        sync.WaitGroup
}

var oc orchest

func (oc *orchest) createNode(nodeID uint64, peers []uint64) {
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)
	commitC := make(chan *commit)
	errorC := make(chan error)
	rn := newRaftNode(nodeID, peers, proposeC, confChangeC, commitC, errorC)
	for nid, node := range oc.raftNodes {
		rn.nw.addPeer(nid, node)
	}

	for _, node := range oc.raftNodes {
		node.nw.addPeer(nodeID, rn)
	}

	oc.raftNodes[nodeID] = rn

	kvs := newKVStore(proposeC, commitC, errorC)

	oc.wg.Add(1)
	go func() {
		defer oc.wg.Done()
		serveHTTPKVAPI(kvs, 9120+int(nodeID), confChangeC, errorC)
		log.Printf("node %d: KVstore has stopped running\n", nodeID)
	}()
}

func main() {
	nodesCount := flag.Int("nodes", 3, "number of nodes")
	flag.Parse()

	oc := orchest{raftNodes: make(map[uint64]*raftNode, *nodesCount)}

	peers := make([]uint64, *nodesCount)
	for i := range *nodesCount {
		peers[i] = uint64(i + 1)
	}

	for i := range *nodesCount {
		id := uint64(i + 1)
		oc.createNode(id, peers)
	}

	log.Println("Main goroutine waiting for workesr to finish...")
	oc.wg.Wait()
	log.Println("All workers finished, main goroutine exiting.")
}
