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
	"container/heap"
	"math/rand"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type msgItem struct {
	at    time.Time
	m     raftpb.Message
	index uint64
}

// priority queue that sort items by their at time.
type msgPriorityQueue []*msgItem

func (pq msgPriorityQueue) Len() int { return len(pq) }

func (pq msgPriorityQueue) Less(i, j int) bool {
	return pq[i].at.Before(pq[j].at)
}

func (pq msgPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *msgPriorityQueue) Push(x any) {
	item := x.(*msgItem)
	*pq = append(*pq, item)
}

func (pq *msgPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

type receiver struct {
	c       chan raftpb.Message
	stopc   chan struct{}
	pending sync.WaitGroup
}

func newReceiver() *receiver {
	return &receiver{
		c:       make(chan raftpb.Message),
		stopc:   make(chan struct{}),
		pending: sync.WaitGroup{},
	}
}

func (r *receiver) receive(m raftpb.Message) {
	r.pending.Add(1)
	go func() {
		select {
		case r.c <- m:
		case <-r.stopc:
		}
		r.pending.Done()
	}()
}

func (r *receiver) stop() {
	close(r.stopc)
	// wait all pending send goroutines
	r.pending.Wait()
	close(r.c)
}

type delayConf struct {
	from  uint64
	to    uint64
	delay delay
}

type dropConf struct {
	from uint64
	to   uint64
	p    float64
}

type faceConf struct {
	id  uint64
	add bool
}

type conn struct {
	from, to uint64
}

// messageQueue buffers messages in network in a priority queue.
// each message is associated with a time when the
// message shall be transmitted over the network.
type messageQueue struct {
	rand      *rand.Rand
	q         msgPriorityQueue
	sendc     chan raftpb.Message
	recvc     map[uint64]*receiver
	recvcLock sync.RWMutex
	stopc     chan struct{}
	delay     map[conn]delay
	drop      map[conn]float64
	ci        uint64

	delayc chan delayConf
	dropc  chan dropConf
	clearc chan struct{}
	facec  chan faceConf
}

func newMessageQueue() *messageQueue {
	mq := &messageQueue{
		rand:      rand.New(rand.NewSource(1)),
		q:         make(msgPriorityQueue, 0),
		sendc:     make(chan raftpb.Message),
		recvc:     map[uint64]*receiver{},
		recvcLock: sync.RWMutex{},
		stopc:     nil,
		delay:     make(map[conn]delay),
		drop:      make(map[conn]float64),
		delayc:    make(chan delayConf),
		dropc:     make(chan dropConf),
		clearc:    make(chan struct{}),
		facec:     make(chan faceConf),
		ci:        0,
	}

	mq.start()

	return mq
}

func (mq *messageQueue) start() {
	mq.stopc = make(chan struct{})
	go mq.msgLoop()
}
func (mq *messageQueue) stop() {
	mq.removeAll()
	close(mq.stopc)
}

func (mq *messageQueue) setDelay(from, to uint64, d delay) {
	mq.delayc <- delayConf{
		delay: d,
		from:  from,
		to:    to,
	}
}

func (mq *messageQueue) setDrop(from, to uint64, p float64) {
	mq.dropc <- dropConf{
		p:    p,
		from: from,
		to:   to,
	}
}

//nolint:golint,unused // TODO: remove the nolint directive when new tests leverage this. For now it is only used in trace validation.
func (mq *messageQueue) clearFault() {
	mq.clearc <- struct{}{}
}

func (mq *messageQueue) push(di *msgItem) {
	heap.Push(&mq.q, di)
}

func (mq *messageQueue) pop() *msgItem {
	return heap.Pop(&mq.q).(*msgItem)
}

func (mq *messageQueue) peek() *msgItem {
	n := mq.q.Len()
	if n <= 0 {
		return nil
	}

	return mq.q[n-1]
}

func (mq *messageQueue) popUntil(t time.Time) []*msgItem {
	result := []*msgItem{}

	for {
		if top := mq.peek(); top != nil && top.at.Before(t) {
			result = append(result, mq.pop())
		} else {
			break
		}
	}

	return result
}

func (mq *messageQueue) send(m raftpb.Message) {
	mq.sendc <- m
}

func (mq *messageQueue) recvFrom(from uint64) chan raftpb.Message {
	r := mq.getReceiver(from)
	if r != nil {
		return r.c
	}

	return nil
}

func (mq *messageQueue) changeFace(id uint64, add bool) {
	mq.facec <- faceConf{
		id:  id,
		add: add,
	}
}

func (mq *messageQueue) getReceiver(id uint64) *receiver {
	mq.recvcLock.RLock()
	defer mq.recvcLock.RUnlock()

	if r, ok := mq.recvc[id]; ok {
		return r
	}

	return nil
}

func (mq *messageQueue) getReceiverIDs() []uint64 {
	mq.recvcLock.RLock()
	defer mq.recvcLock.RUnlock()

	ids := []uint64{}
	for id := range mq.recvc {
		ids = append(ids, id)
	}

	return ids
}

func (mq *messageQueue) addReceiver(id uint64, r *receiver) *receiver {
	mq.recvcLock.Lock()
	defer mq.recvcLock.Unlock()

	old := mq.recvc[id]
	mq.recvc[id] = r
	return old
}

func (mq *messageQueue) removeReceiver(id uint64) *receiver {
	mq.recvcLock.Lock()
	defer mq.recvcLock.Unlock()

	if r, ok := mq.recvc[id]; ok {
		delete(mq.recvc, id)
		return r
	}

	return nil
}

func (mq *messageQueue) removeAll() {
	ids := mq.getReceiverIDs()

	for _, id := range ids {
		mq.changeFace(id, false)
	}
}

func (mq *messageQueue) msgLoop() {
	timer := time.NewTimer(time.Minute)
	var notifyc <-chan time.Time
	var lastIndex uint64
	for {

		first := mq.peek()
		if first != nil {
			if lastIndex != first.index {
				lastIndex = first.index
				// reset timer
				timer.Stop()
				select {
				case <-timer.C:
				default:
				}
				duration := time.Until(first.at)
				timer.Reset(duration)
				notifyc = timer.C
			}
		} else {
			notifyc = nil
		}

		select {
		case m := <-mq.sendc:
			if drop, exists := mq.drop[conn{m.From, m.To}]; exists && mq.rand.Float64() < drop {
				continue
			}
			var rd int64
			var nonBlockingDelay bool
			if delay, exists := mq.delay[conn{m.From, m.To}]; exists && mq.rand.Float64() < delay.rate {
				rd = mq.rand.Int63n(int64(delay.d))
				nonBlockingDelay = !delay.block
			}
			// use marshal/unmarshal to copy message to avoid data race.
			b, err := m.Marshal()
			if err != nil {
				panic(err)
			}
			var cm raftpb.Message
			err = cm.Unmarshal(b)
			if err != nil {
				panic(err)
			}
			if nonBlockingDelay {
				mq.push(&msgItem{
					at:    time.Now().Add(time.Duration(rd)),
					m:     cm,
					index: mq.ci,
				})
			} else {
				time.Sleep(time.Duration(rd))
				mq.push(&msgItem{
					at:    time.Now(),
					m:     cm,
					index: mq.ci,
				})
			}

			mq.ci++

		case <-notifyc:
			items := mq.popUntil(time.Now())
			for _, di := range items {
				if r := mq.getReceiver(di.m.To); r != nil {
					r.receive(di.m)
				}
			}
		case delayConf := <-mq.delayc:
			mq.delay[conn{delayConf.from, delayConf.to}] = delayConf.delay
		case dropConf := <-mq.dropc:
			mq.drop[conn{dropConf.from, dropConf.to}] = dropConf.p
		case <-mq.clearc:
			mq.delay = map[conn]delay{}
			mq.drop = map[conn]float64{}
		case faceConf := <-mq.facec:
			if faceConf.add {
				mq.addReceiver(faceConf.id, newReceiver())
			} else {
				if r := mq.removeReceiver(faceConf.id); r != nil {
					r.stop()
				}
			}
		case <-mq.stopc:
			timer.Stop()
			return
		}
	}
}
