package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Uint64("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "set to true to join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	// For this simple example, we assume node IDs are 1, 2, 3... corresponding to the list order.
	peerURLs := make(map[uint64]string)
	for i, url := range strings.Split(*cluster, ",") {
		peerURLs[uint64(i+1)] = url
	}

	snapdir := fmt.Sprintf("raftsimple-%d-snap", *id)
	ss, err := newSnapshotStorage(snapdir)
	if err != nil {
		log.Fatalf("raftsimple: error creating storage: %v", err)
	}

	kvs, fsm := newKVStore(proposeC)

	rn := newRaftNode(*id, peerURLs, *join, fsm, ss, proposeC, confChangeC)

	go func() {
		if err := rn.processCommits(); err != nil {
			log.Fatalf("raftsimple: processCommits exited: %v", err)
		}
	}()

	log.Printf("Node %d starting KV API on port %d", *id, *kvport)
	serveHTTPKVAPI(kvs, uint64(*kvport), confChangeC, rn.donec)
}
