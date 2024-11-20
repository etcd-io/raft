// Copyright 2024 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafttest

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
type clusterConfig struct {
	size         int
	traceLogger  raft.TraceLogger
	tickInterval time.Duration
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
type endpoint struct {
	index int
	node  *node
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
type getEndpoint struct {
	i int
	c chan endpoint
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
type cluster struct {
	nodes   map[uint64]*node
	network *raftNetwork

	stopc   chan struct{}
	removec chan uint64
	addc    chan uint64
	getc    chan getEndpoint
	faultc  chan func(*cluster)

	traceLogger  raft.TraceLogger
	tickInterval time.Duration
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func newCluster(c clusterConfig) *cluster {
	tickInterval := c.tickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ids := make([]uint64, c.size)
	peers := make([]raft.Peer, c.size)
	for i := 0; i < c.size; i++ {
		peers[i].ID = uint64(i + 1)
		ids[i] = peers[i].ID
	}
	network := newRaftNetwork(ids...)
	nodes := make(map[uint64]*node, c.size)
	for i := 0; i < c.size; i++ {
		nodes[uint64(i+1)] = startNodeWithConfig(nodeConfig{
			id:           uint64(i + 1),
			peers:        peers,
			iface:        network.nodeNetwork(uint64(i + 1)),
			traceLogger:  c.traceLogger,
			tickInterval: tickInterval,
		})
	}

	cl := &cluster{
		nodes:        nodes,
		network:      network,
		stopc:        make(chan struct{}),
		removec:      make(chan uint64),
		addc:         make(chan uint64),
		getc:         make(chan getEndpoint),
		faultc:       make(chan func(*cluster)),
		traceLogger:  c.traceLogger,
		tickInterval: tickInterval,
	}

	cl.waitLeader()

	go cl.mgmtLoop()

	return cl
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *cluster) mgmtLoop() {
	peers := []raft.Peer{}
	for _, n := range cl.nodes {
		peers = append(peers, raft.Peer{ID: n.id, Context: []byte{}})
	}
	for {
		select {
		case id := <-cl.addc:
			cl.network.changeFace(id, true)
			peers = append(peers, raft.Peer{ID: id, Context: []byte{}})
			node := startNodeWithConfig(nodeConfig{
				id:           id,
				peers:        nil,
				iface:        cl.network.nodeNetwork(id),
				traceLogger:  cl.traceLogger,
				tickInterval: cl.tickInterval,
			})
			cl.nodes[id] = node
		case id := <-cl.removec:
			cl.network.changeFace(id, false)
			cl.nodes[id].stop()
			delete(cl.nodes, id)
			for i, p := range peers {
				if p.ID == id {
					peers = append(peers[:i], peers[i+1:]...)
					break
				}
			}
		case gn := <-cl.getc:
			nPeers := len(peers)
			found := false
			for off := 0; off < nPeers; off = off + 1 {
				i := (gn.i + off) % nPeers
				nid := peers[i].ID
				n := cl.nodes[nid]
				if n != nil && n.Node != nil {
					gn.c <- endpoint{index: i, node: n}
					found = true
					break
				}
			}
			if !found {
				// all nodes are removed
				gn.c <- endpoint{index: 0, node: nil}
			}
		case <-cl.stopc:
			for _, n := range cl.nodes {
				n.stop()
			}
			cl.network.stop()
			close(cl.stopc)
			return
		case f := <-cl.faultc:
			cl.network.clearFault()
			for _, n := range cl.nodes {
				if n.stopped {
					n.restart()
				}
			}
			f(cl)
		}
	}
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *cluster) stop() {
	cl.stopc <- struct{}{}
	<-cl.stopc
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *cluster) removeNode(id uint64) {
	cl.removec <- id
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *cluster) addNode(id uint64) {
	cl.addc <- id
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *cluster) newClient() *client {
	return &client{cluster: cl, epc: make(chan endpoint)}
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *cluster) waitLeader() uint64 {
	var l map[uint64]struct{}
	var lindex uint64

	for {
		l = make(map[uint64]struct{})

		for i, n := range cl.nodes {
			lead := n.Status().SoftState.Lead
			if lead != 0 {
				l[lead] = struct{}{}
				if n.id == lead {
					lindex = i
				}
			}
		}

		if len(l) == 1 {
			return lindex
		}
	}
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
type client struct {
	cluster *cluster
	epi     int
	epc     chan endpoint
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *client) propose(ctx context.Context, data []byte) error {
	ep := cl.getEndpoint()
	if ep == nil {
		return fmt.Errorf("all nodes down")
	}
	return ep.Propose(ctx, data)
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *client) addNode(ctx context.Context, n uint64) error {
	change := raftpb.ConfChangeSingle{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: n,
	}
	cc := raftpb.ConfChangeV2{
		Transition: 0,
		Changes:    []raftpb.ConfChangeSingle{change},
		Context:    []byte{},
	}

	ep := cl.getEndpoint()
	if err := ep.ProposeConfChange(ctx, cc); err != nil {
		return err
	}

	toc := time.After(cl.cluster.tickInterval * 50)
	for {
		select {
		case <-toc:
			return fmt.Errorf("addNode timeout")
		default:
		}
		st := ep.Status()
		if _, exist := st.Config.Voters[0][n]; exist {
			break
		}
	}
	cl.cluster.addNode(n)
	return nil
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *client) removeNode(ctx context.Context, n uint64) error {
	change := raftpb.ConfChangeSingle{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: n,
	}
	cc := raftpb.ConfChangeV2{
		Transition: 0,
		Changes:    []raftpb.ConfChangeSingle{change},
		Context:    []byte{},
	}

	ep := cl.getEndpoint()
	if err := ep.ProposeConfChange(ctx, cc); err != nil {
		return err
	}

	toc := time.After(cl.cluster.tickInterval * 50)
	for {
		select {
		case <-toc:
			return fmt.Errorf("removeNode timeout")
		default:
		}
		st := ep.Status()
		if _, exist := st.Config.Voters[0][n]; !exist {
			break
		}
	}

	cl.cluster.removeNode(n)
	return nil
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (cl *client) getEndpoint() *node {
	// round robin
	ge := getEndpoint{
		i: cl.epi + 1,
		c: cl.epc,
	}
	cl.cluster.getc <- ge
	ep := <-ge.c
	cl.epi = ep.index
	return ep.node
}
