package main

import (
	"flag"
	"log"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

func main() {
	nodesCount := flag.Int("nodes", 3, "number of nodes")
	flag.Parse()

	var wg sync.WaitGroup

	peers := make([]uint64, *nodesCount)
	for i := range *nodesCount {
		peers[i] = uint64(i + 1)
	}

	raftNodes := make(map[uint64]*raftNode, *nodesCount)

	for i := range *nodesCount {
		id := uint64(i + 1)
		proposeC := make(chan string)
		confChangeC := make(chan raftpb.ConfChange)
		commitC := make(chan *commit)
		errorC := make(chan error)

		rn := newRaftNode(id, peers, proposeC, confChangeC, commitC, errorC)
		for nid, node := range raftNodes {
			rn.nw.addPeer(nid, node)
		}

		raftNodes[id] = rn

		for _, node := range raftNodes {
			node.nw.addPeer(id, rn)
		}

		kvs := newKVStore(proposeC, commitC, errorC)

		wg.Add(1)
		go func() {
			defer wg.Done()
			serveHTTPKVAPI(kvs, 9121+i, confChangeC, errorC)
			log.Printf("node %d: KVstore has stopped running\n", id)
		}()
	}

	log.Println("Main goroutine waiting for workesr to finish...")
	wg.Wait()
	log.Println("All workers finished, main goroutine exiting.")
}
