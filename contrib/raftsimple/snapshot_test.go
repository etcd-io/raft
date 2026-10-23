package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func findFollower(nodes map[uint64]*raftNode) *raftNode {
	for range 50 {
		for _, rn := range nodes {
			if rn.node != nil && rn.node.Status().Lead != rn.id && rn.node.Status().Lead != 0 {
				return rn
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func TestSnapshot_Recovery(t *testing.T) {
	oldSnapCount := DefaultSnapshotCount
	DefaultSnapshotCount = 5
	defer func() { DefaultSnapshotCount = oldSnapCount }()

	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	peers := []uint64{1, 2, 3}
	peerURLs := make(map[uint64]string)
	for _, id := range peers {
		peerURLs[id] = fmt.Sprintf("http://127.0.0.1:%d", 20000+id)
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

	follower := findFollower(nodes)
	require.NotNil(t, follower, "A follower should be found")
	followerID := follower.id

	// Stop follower
	nodes[followerID].stop()

	for i := 0; i < int(DefaultSnapshotCount)+2; i++ {
		kvstores[leader.id].Propose(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	require.Eventually(t, func() bool {
		return nodes[leader.id].snapshotIndex > 0
	}, 10*time.Second, 100*time.Millisecond, "Leader should have taken a snapshot")

	// Restart follower
	createNode(followerID)

	require.Eventually(t, func() bool {
		v, ok := kvstores[followerID].Lookup("key0")
		return ok && v == "val0"
	}, 10*time.Second, 100*time.Millisecond, "Follower should catch up via snapshot")

	require.Eventually(t, func() bool {
		lastIdx := int(DefaultSnapshotCount) + 1
		v, ok := kvstores[followerID].Lookup(fmt.Sprintf("key%d", lastIdx))
		return ok && v == fmt.Sprintf("val%d", lastIdx)
	}, 5*time.Second, 100*time.Millisecond, "Follower should catch up to the latest entry")
}
