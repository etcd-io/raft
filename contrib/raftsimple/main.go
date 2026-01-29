package main

import (
	"flag"
	"log"
	"sync"
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
		commitC := make(chan *commit)
		errorC := make(chan error)

		rn := newRaftNode(id, peers, nw, proposeC, commitC, errorC)
		nw.peers[id] = rn

		kvs := newKVStore(proposeC, commitC, errorC)

		wg.Add(1)
		go serveHTTPKVAPI(kvs, 9121+i, errorC, &wg)
	}

	log.Println("Main goroutine waiting for workesr to finish...")
	wg.Wait()
	log.Println("All workers finished, main goroutine exiting.")
}

/*
- create a cluster of N nodes. All should be in-memory.
- the transportion will loop through each node in-memory to transport messages.
- the KVAPI should be able to GET/PUT keys/values. Also DELETE to delete a node.
*/
