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

import "math/rand"

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
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

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func getNodeIDs(c *cluster) []uint64 {
	ids := []uint64{}
	for i := range c.nodes {
		ids = append(ids, i)
	}
	return ids
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
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

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
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

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func makeNetworkPartition() func(c *cluster) {
	return func(c *cluster) {
		ids := getNodeIDs(c)
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

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func makeNodesDown() func(c *cluster) {
	return func(c *cluster) {
		ids := getNodeIDs(c)
		perm := rand.Perm(len(ids))
		n := rand.Intn(len(ids)/2 + 1)
		for _, i := range perm[:n] {
			c.nodes[ids[i]].stop()
		}
	}
}
