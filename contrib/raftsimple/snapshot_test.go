package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func findFollower(t *testing.T, nw *network) *raftNode {
	var follower *raftNode
	require.Eventually(t, func() bool {
		nw.mu.RLock()
		defer nw.mu.RUnlock()
		for _, rn := range nw.peers {
			if rn.node != nil && rn.node.Status().Lead != rn.id && rn.node.Status().Lead != 0 {
				follower = rn
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "A follower should be found")
	return follower
}

func TestSnapshot_Recovery(t *testing.T) {
	oldSnapCount := DefaultSnapshotCount
	DefaultSnapshotCount = 5
	defer func() { DefaultSnapshotCount = oldSnapCount }()

	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	nw := &network{peers: make(map[uint64]*raftNode)}
	nm := &NodeManager{nw: nw, SnapDir: tmpDir}

	peers := []uint64{1, 2, 3}
	for _, id := range peers {
		nm.createOrRecoverNode(id, peers)
	}

	defer func() {
		for _, id := range peers {
			_ = nm.stopNode(id)
		}
		nm.wg.Wait()
	}()

	leader := findLeader(t, nw)
	follower := findFollower(t, nw)
	followerID := follower.id

	err := nm.stopNode(followerID)
	require.NoError(t, err)

	for i := 0; i < int(DefaultSnapshotCount)+2; i++ {
		kvs := leader.fsm.(kvfsm).kvs
		kvs.Propose(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	require.Eventually(t, func() bool {
		return leader.snapshotIndex > 0
	}, 5*time.Second, 100*time.Millisecond, "Leader should have taken a snapshot")

	nm.createOrRecoverNode(followerID, peers)
	followerRecovered, _ := nw.getNode(followerID)

	require.Eventually(t, func() bool {
		v, ok := followerRecovered.fsm.(kvfsm).kvs.Lookup("key0")
		return ok && v == "val0"
	}, 10*time.Second, 100*time.Millisecond, "Follower should catch up via snapshot")

	require.Eventually(t, func() bool {
		lastIdx := int(DefaultSnapshotCount) + 1
		v, ok := followerRecovered.fsm.(kvfsm).kvs.Lookup(fmt.Sprintf("key%d", lastIdx))
		return ok && v == fmt.Sprintf("val%d", lastIdx)
	}, 5*time.Second, 100*time.Millisecond, "Follower should catch up to the latest entry")
}
