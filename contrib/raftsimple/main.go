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

	nw := &network{peers: make(map[uint64]*raftNode)}
	var wg sync.WaitGroup

	peers := make([]uint64, *nodesCount)
	for i := range *nodesCount {
		peers[i] = uint64(i + 1)
	}

	for i := range *nodesCount {
		id := uint64(i + 1)
		proposeC := make(chan string)
		confChangeC := make(chan raftpb.ConfChange)
		commitC := make(chan *commit)
		errorC := make(chan error)

		nw.peers[id] = newRaftNode(id, peers, nw, proposeC, confChangeC, commitC, errorC)

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
