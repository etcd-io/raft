package main

import (
	"flag"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Uint64("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	commitC, errorC := newRaftNode(*id, strings.Split(*cluster, ","), proposeC, confChangeC)

	kvs := newKVStore(proposeC, commitC, errorC)

	serveHTTPKVAPI(kvs, *kvport, confChangeC, errorC)
}
