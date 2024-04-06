package rafttest

import "math/rand"

func pickRandomNode(c *cluster) *node {
	k := rand.Intn(len(c.nodes))
	for _, n := range c.nodes {
		if k == 0 {
			return n
		}
		k--
	}

	return nil
}

func getNodeIds(c *cluster) []uint64 {
	ids := []uint64{}
	for i := range c.nodes {
		ids = append(ids, i)
	}
	return ids
}

func makeDelayFault(d delay, dual bool) func(c *cluster) {
	return func(c *cluster) {
		from := pickRandomNode(c).id
		to := pickRandomNode(c).id
		c.network.delay(from, to, d)
		if dual {
			c.network.delay(to, from, d)
		}
	}
}

func makeDropFault(p float64, dual bool) func(c *cluster) {
	return func(c *cluster) {
		from := pickRandomNode(c).id
		to := pickRandomNode(c).id
		c.network.drop(from, to, p)
		if dual {
			c.network.drop(to, from, p)
		}
	}
}

func makeNetworkPartition() func(c *cluster) {
	return func(c *cluster) {
		ids := getNodeIds(c)
		perm := rand.Perm(len(ids))
		k := rand.Intn(len(ids) - 1)
		for _, i := range perm[:k+1] {
			for _, j := range perm[k+1:] {
				c.network.drop(ids[i], ids[j], 1)
				c.network.drop(ids[j], ids[i], 1)
			}
		}
	}
}

func makeNodesDown() func(c *cluster) {
	return func(c *cluster) {
		ids := getNodeIds(c)
		perm := rand.Perm(len(ids))
		n := rand.Intn(len(ids)/2 + 1)
		for _, i := range perm[:n] {
			c.nodes[ids[i]].stop()
		}
	}
}
