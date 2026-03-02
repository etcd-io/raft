package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func findLeader(t *testing.T, nw *network) *raftNode {
	var leader *raftNode
	require.Eventually(t, func() bool {
		nw.mu.RLock()
		defer nw.mu.RUnlock()
		for _, rn := range nw.peers {
			if rn.node != nil && rn.node.Status().Lead == rn.id {
				leader = rn
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "A leader should be elected")
	return leader
}

func TestIntegration_Lifecycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftsimple-lifecycle-*")
	require.NoError(t, err)
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
		time.Sleep(1 * time.Second) // Give OS time to release ports
	}()

	leader := findLeader(t, nw)
	kvs := leader.fsm.(kvfsm).kvs
	kvs.Propose("key1", "val1")

	node3, _ := nw.getNode(3)
	require.Eventually(t, func() bool {
		v, ok := node3.fsm.(kvfsm).kvs.Lookup("key1")
		return ok && v == "val1"
	}, 5*time.Second, 100*time.Millisecond, "Data should replicate to Node 3")

	err = nm.stopNode(2)
	require.NoError(t, err)

	leader = findLeader(t, nw)
	kvs = leader.fsm.(kvfsm).kvs
	kvs.Propose("key2", "val2")

	require.Eventually(t, func() bool {
		v, ok := node3.fsm.(kvfsm).kvs.Lookup("key2")
		return ok && v == "val2"
	}, 5*time.Second, 100*time.Millisecond, "Data should replicate to Node 3 while Node 2 is offline")

	nm.createOrRecoverNode(2, peers)
	node2Recovered, _ := nw.getNode(2)

	require.Eventually(t, func() bool {
		v1, ok1 := node2Recovered.fsm.(kvfsm).kvs.Lookup("key1")
		v2, ok2 := node2Recovered.fsm.(kvfsm).kvs.Lookup("key2")
		return ok1 && ok2 && v1 == "val1" && v2 == "val2"
	}, 5*time.Second, 100*time.Millisecond, "Node 2 should recover and catch up on all data")
}
