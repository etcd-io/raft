package rafttest

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type clusterConfig struct {
	size         int
	traceLogger  raft.TraceLogger
	tickInterval time.Duration
}

type endpoint struct {
	index int
	node  *node
}

type getEndpoint struct {
	i int
	c chan endpoint
}

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
			i := gn.i % len(peers)
			nid := peers[i].ID
			gn.c <- endpoint{index: i, node: cl.nodes[nid]}
		case <-cl.stopc:
			for _, n := range cl.nodes {
				n.stop()
			}
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

func (cl *cluster) stop() {
	cl.stopc <- struct{}{}
	<-cl.stopc
}

func (cl *cluster) removeNode(id uint64) {
	cl.removec <- id
}

func (cl *cluster) addNode(id uint64) {
	cl.addc <- id
}

func (cl *cluster) newClient() *client {
	return &client{cluster: cl, epc: make(chan endpoint)}
}

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

type client struct {
	cluster *cluster
	epi     int
	epc     chan endpoint
}

func (cl *client) propose(ctx context.Context, data []byte) error {
	ep := cl.getEndpoint()
	return ep.Propose(ctx, data)
}

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
