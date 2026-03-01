package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type NodeManager struct {
	nw *network
	wg sync.WaitGroup
}

var nw network

// createOrRecoverNode spins up a raft node. It returns (isNewNode, success).
// If the node's storage directory already exists, it treats it as a recovery.
func (nm *NodeManager) createOrRecoverNode(nodeID uint64, peers []uint64) (bool, bool) {
	_, ok := nm.nw.getNode(nodeID)
	if ok {
		log.Printf("node %d already running in network\n", nodeID)
		return false, false
	}

	snapdir := fmt.Sprintf("raftsimple-%d-snap", nodeID)

	// Determine if this is a fresh node or a recovery based on directory existence
	isNewNode := true
	if _, err := os.Stat(snapdir); err == nil {
		isNewNode = false
		log.Printf("node %d: found existing storage, recovering...", nodeID)
	} else {
		log.Printf("node %d: no existing storage found, creating new node...", nodeID)
	}

	ss, err := newSnapshotStorage(snapdir)
	if err != nil {
		log.Fatalf("raftsimple storage error: %v", err)
		return false, false
	}

	proposeC := make(chan string) // Updated to []byte to match earlier kv_store.go changes
	confChangeC := make(chan raftpb.ConfChange)

	kvs, fsm := newKVStore(proposeC)

	// In a real recovery scenario where peers aren't known, you'd read them from the WAL.
	// For this simplified example, if peers is nil (like when called from the HTTP API),
	// we assume it's joining an existing cluster or recovering and pass an empty slice.
	if peers == nil {
		peers = []uint64{}
	}

	rn := newRaftNode(nodeID, peers, fsm, ss, nm.nw, proposeC, confChangeC)

	// Register it with the network so peers can send it messages
	nm.nw.register(nodeID, rn)

	// Start processing commits loop
	go func() {
		if err := rn.processCommits(); err != nil {
			log.Printf("node %d: processCommits exited with error: %v\n", nodeID, err)
		}
	}()

	nm.wg.Add(1)
	go func() {
		defer nm.wg.Done()
		// Start the HTTP API
		serveHTTPKVAPI(nm, kvs, 9120+nodeID, confChangeC, rn.donec)
		log.Printf("node %d: KVstore HTTP API has stopped running\n", nodeID)
	}()

	return isNewNode, true
}

// stopNode safely halts a node's goroutines and removes it from the network
// to simulate a crash or partition.
func (nm *NodeManager) stopNode(nodeID uint64) error {
	rn, ok := nm.nw.getNode(nodeID)
	if !ok {
		return fmt.Errorf("node %d not found in network", nodeID)
	}

	nm.nw.deregister(nodeID)

	rn.stop()

	return nil
}

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

	log.Println("Main goroutine waiting for workesr to finish...")
	nm.wg.Wait()
	log.Println("All workers finished, main goroutine exiting.")
}
