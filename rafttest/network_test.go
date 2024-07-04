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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.etcd.io/raft/v3/raftpb"
)

func TestNetworkDrop(t *testing.T) {
	// drop around 10% messages
	sent := 1000
	droprate := 0.1
	nt := newRaftNetwork(1, 2)

	nt.drop(1, 2, droprate)

	go func() {
		for i := 0; i < sent; i++ {
			nt.send(raftpb.Message{From: 1, To: 2})
		}

		time.Sleep(time.Millisecond * 10)
		nt.stop()
	}()

	c := nt.recvFrom(2)

	received := 0
	ok := true
	for ok {
		if _, ok = <-c; ok {
			received++
		}
	}

	drop := sent - received
	assert.LessOrEqual(t, drop, int((droprate+0.1)*float64(sent)))
	assert.GreaterOrEqual(t, drop, int((droprate-0.1)*float64(sent)))
}

func TestNetworkDelay(t *testing.T) {
	sent := 1000
	d := time.Millisecond
	delayrate := 0.1
	nt := newRaftNetwork(1, 2)

	nt.delay(1, 2, delay{d, delayrate, true})

	go func() {
		for i := 0; i < sent; i++ {
			nt.send(raftpb.Message{From: 1, To: 2})
		}

		time.Sleep(time.Millisecond * 10)
		nt.stop()
	}()

	c := nt.recvFrom(2)
	s := time.Now()

	ok := true
	for ok {
		_, ok = <-c
	}

	total := time.Since(s)

	w := time.Duration(float64(sent)*delayrate/2) * d
	// there is some overhead in the send call since it generates random numbers.
	assert.GreaterOrEqual(t, total, w)
}

func TestMessageReordering(t *testing.T) {
	n := 1000
	d := time.Millisecond * 100
	delayrate := 0.1
	nt := newRaftNetwork(1, 2)

	nt.delay(1, 2, delay{d, delayrate, false})

	sent := []int{}
	go func() {
		for i := 0; i < n; i++ {
			nt.send(raftpb.Message{From: 1, To: 2, Index: uint64(i)})
			sent = append(sent, i)
		}

		// wait some time to send all messages.
		// Ideally we only see at most d delay with some extra time on internal loop.
		time.Sleep(d + time.Millisecond*10)
		nt.stop()
	}()

	c := nt.recvFrom(2)

	received := []int{}
	ok := true
	var m raftpb.Message
	for ok {
		if m, ok = <-c; ok {
			received = append(received, int(m.Index))
		}
	}

	assert.NotEqual(t, sent, received, "received messages shall be reordered")

	sort.Ints(received)
	assert.Equal(t, sent, received, "all messages shall be received")
}
