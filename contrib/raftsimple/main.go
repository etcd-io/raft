package main

import (
	"flag"
	"log"
	"sync"
)

func main() {
	nodesCount := flag.Int("nodes", 3, "number of nodes")
	flag.Parse()

	var wg sync.WaitGroup

	peers := make([]uint64, *nodesCount)
	for i := range *nodesCount {
		peers[i] = uint64(i + 1)
	}

	lstCommitC := make([]<-chan *commit, *nodesCount)
	lstErrorC := make([]<-chan error, *nodesCount)
	for i := range *nodesCount {
		proposeC := make(chan string)
		commitC, errorC := newRaftNode(uint64(i+1), peers, proposeC)
		lstCommitC[i] = commitC
		lstErrorC[i] = errorC

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
