package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func findLeader(nodes map[uint64]*raftNode) *raftNode {
	for range 50 {
		for _, rn := range nodes {
			if rn.node != nil && rn.node.Status().Lead == rn.id {
				return rn
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func TestIntegration_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	peers := []uint64{1, 2, 3}
	peerURLs := make(map[uint64]string)
	for _, id := range peers {
		peerURLs[id] = fmt.Sprintf("http://127.0.0.1:%d", 10000+id)
	}

	nodes := make(map[uint64]*raftNode)
	kvstores := make(map[uint64]*kvstore)

	createNode := func(id uint64) {
		snapdir := fmt.Sprintf("%s/node-%d", tmpDir, id)
		ss, _ := newSnapshotStorage(snapdir)
		proposeC := make(chan string)
		confChangeC := make(chan raftpb.ConfChange)
		kvs, fsm := newKVStore(proposeC)
		rn := newRaftNode(id, peerURLs, false, fsm, ss, proposeC, confChangeC)
		nodes[id] = rn
		kvstores[id] = kvs
		go rn.processCommits()
	}

	for _, id := range peers {
		createNode(id)
	}

	defer func() {
		for _, rn := range nodes {
			rn.stop()
		}
	}()

	leader := findLeader(nodes)
	require.NotNil(t, leader, "A leader should be elected")
	kvstores[leader.id].Propose("key1", "val1")

	require.Eventually(t, func() bool {
		v, ok := kvstores[3].Lookup("key1")
		return ok && v == "val1"
	}, 5*time.Second, 100*time.Millisecond, "Data should replicate to Node 3")

	// Simulate node 2 crash
	nodes[2].stop()

	leader = findLeader(nodes)
	require.NotNil(t, leader, "A leader should be elected after crash")
	kvstores[leader.id].Propose("key2", "val2")

	require.Eventually(t, func() bool {
		v, ok := kvstores[3].Lookup("key2")
		return ok && v == "val2"
	}, 5*time.Second, 100*time.Millisecond, "Data should replicate to Node 3 while Node 2 is offline")

	// Restart node 2
	createNode(2)

	require.Eventually(t, func() bool {
		v1, ok1 := kvstores[2].Lookup("key1")
		v2, ok2 := kvstores[2].Lookup("key2")
		return ok1 && ok2 && v1 == "val1" && v2 == "val2"
	}, 10*time.Second, 100*time.Millisecond, "Node 2 should recover and catch up on all data")
}
