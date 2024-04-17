// Copyright 2015 The etcd Authors
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
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// a network interface
type iface interface {
	send(m raftpb.Message)
	recv() chan raftpb.Message
	disconnect()
	connect()
}

type raftNetwork struct {
	mq *messageQueue
}

type delay struct {
	d    time.Duration
	rate float64
}

func newRaftNetwork(nodes ...uint64) *raftNetwork {
	pn := &raftNetwork{
		mq: newMessageQueue(),
	}

	for _, n := range nodes {
		pn.mq.changeFace(n, true)
	}

	return pn
}

func (rn *raftNetwork) nodeNetwork(id uint64) iface {
	return &nodeNetwork{id: id, raftNetwork: rn, connected: true}
}

func (rn *raftNetwork) send(m raftpb.Message) {
	rn.mq.send(m)
}

func (rn *raftNetwork) recvFrom(from uint64) chan raftpb.Message {
	return rn.mq.recvFrom(from)
}

func (rn *raftNetwork) drop(from, to uint64, rate float64) {
	rn.mq.setDrop(from, to, rate)
}

func (rn *raftNetwork) delay(from, to uint64, d delay) {
	rn.mq.setDelay(from, to, d)
}

func (rn *raftNetwork) clearFault() {
	rn.mq.clearFault()
}

func (rn *raftNetwork) changeFace(id uint64, add bool) {
	rn.mq.changeFace(id, add)
}

type nodeNetwork struct {
	id        uint64
	connected bool
	*raftNetwork
}

func (nt *nodeNetwork) connect() {
	nt.connected = true
}

func (nt *nodeNetwork) disconnect() {
	nt.connected = false
}

func (nt *nodeNetwork) send(m raftpb.Message) {
	if nt.connected {
		nt.raftNetwork.send(m)
	}
}

func (nt *nodeNetwork) recv() chan raftpb.Message {
	if nt.connected {
		return nt.recvFrom(nt.id)
	}

	return nil
}
