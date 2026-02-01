package main

import (
	"flag"
	"log"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type orchest struct {
	nw *network
	wg sync.WaitGroup
}

var oc orchest

var nw network

func (oc *orchest) createNode(nodeID uint64, peers []uint64) bool {
	if oc.nw.exists(nodeID) {
		return false
	}

	proposeC := make(chan string)
	// defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	// defer close(confChangeC)

	kvs, fsm := newKVStore(proposeC)

	rn := newRaftNode(nodeID, peers, fsm, oc.nw, proposeC, confChangeC)

	oc.nw.register(nodeID, rn)

	// start processing commits loop
	go func() {
		if err := rn.processCommits(); err != nil {
			log.Fatalf("raftsimple: %v", err)
		}
	}()

	oc.wg.Add(1)
	go func() {
		defer oc.wg.Done()
		serveHTTPKVAPI(oc, kvs, 9120+int(nodeID), confChangeC, rn.donec)
		log.Printf("node %d: KVstore has stopped running\n", nodeID)
	}()

	return true
}

func main() {
	nodesCount := flag.Int("nodes", 3, "number of nodes")
	flag.Parse()

	nw := network{peers: make(map[uint64]*raftNode, *nodesCount)}
	oc := orchest{nw: &nw}

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
