package main 

import (
	"fmt"
	"flag"
	"strings"
	"time"

	"go.etcd.io/raft/v3"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	flag.Parse()

	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID: uint64(*id),
		ElectionTick: 10,
		HeartbeatTick: 1,
		Storage: storage,
		MaxSizePerMsg: 4096,
		MaxInflightMsgs: 256,
	}

	peers := strings.Split(*cluster, ",")

	peersLst := make([]raft.Peer, len(peers))

	for i := range peers {
		peersLst[i] = raft.Peer{ID: uint64(i + 1)}
	}

	if len(peersLst) != len(peers) {
		// TODO: Add error here
		return
	}

	n := raft.StartNode(c, peersLst)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.Tick()
		case _ := <-n.Ready():
			// snapshot
			// entry
			// hardstate
			n.Advance()
		}
	}
}
