package main

import (
	"flag"
	"fmt"
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
	// defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	// defer close(confChangeC)

	snapdir := fmt.Sprintf("raftsimple-%d-snap", nodeID)
	ss, err := newSnapshotStorage(snapdir)
	if err != nil {
		log.Fatalf("raftsimple: %v", err)
	}

	kvs, fsm := newKVStore(proposeC)

	rn := newRaftNode(nodeID, peers, fsm, ss, proposeC, confChangeC)
	for nid, node := range oc.raftNodes {
		rn.nw.addPeer(nid, node)
		node.nw.addPeer(nodeID, rn)
	}
	oc.raftNodes[nodeID] = rn

	// start processing commits loop
	go func() {
		if err := rn.processCommits(); err != nil {
			log.Fatalf("raftsimple: %v", err)
		}
	}()

	oc.wg.Add(1)
	go func() {
		defer oc.wg.Done()
		serveHTTPKVAPI(kvs, 9120+int(nodeID), confChangeC, rn.donec)
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
