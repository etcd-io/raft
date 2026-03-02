package main

import (
	"flag"
	"log"
)

func main() {
	nodesCount := flag.Int("nodes", 3, "number of nodes")
	flag.Parse()

	nw := network{peers: make(map[uint64]*raftNode, *nodesCount)}
	nm := NodeManager{nw: &nw}

	peers := make([]uint64, *nodesCount)
	for i := range *nodesCount {
		peers[i] = uint64(i + 1)
	}

	for i := range *nodesCount {
		id := uint64(i + 1)
		nm.createOrRecoverNode(id, peers)
	}

	log.Println("Main goroutine waiting for workers to finish...")
	nm.wg.Wait()
	log.Println("All workers finished, main goroutine exiting.")
}
