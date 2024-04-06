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

//go:build with_tla
// +build with_tla

package rafttest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

type Fault int

const (
	None Fault = iota
	Delay
	Drop
	NetworkPartition
	NodeDown
	FaultCount
)

const (
	MaxDelayInMilliseconds int = 50 // 50ms as one election timeout
)

var testNameCleanRegex = regexp.MustCompile(`[^a-zA-Z0-9 \-_]+`)

func makeRandomFaults(c *cluster, n int) []func(*cluster) {
	var applier func(*cluster)
	appliers := []func(*cluster){}
	r := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		switch Fault(r.Intn(int(FaultCount))) {
		case Delay:
			p := r.Float64()
			d := time.Millisecond * time.Duration(r.Intn(MaxDelayInMilliseconds))
			dual := r.Float32() < 0.5
			applier = makeDelayFault(delay{d: d, rate: p}, dual)
		case Drop:
			p := r.Float64()
			dual := r.Float32() < 0.5
			applier = makeDropFault(p, dual)
		case NetworkPartition:
			applier = makeNetworkPartition()
		case NodeDown:
			applier = makeNodesDown()
		default:
			continue
		}

		appliers = append(appliers, applier)
	}

	return appliers
}

func runWithRandomFaults(clusterSize int, tl raft.TraceLogger, pReconf float64, durationInET int, faultIntervalInET int) {
	cluster := newCluster(clusterConfig{
		size:         clusterSize,
		traceLogger:  tl,
		tickInterval: 20 * time.Millisecond,
	})

	faults := makeRandomFaults(cluster, int(durationInET/faultIntervalInET))
	fi := 0

	faults[0](cluster)
	timer := time.NewTimer(time.Duration(faultIntervalInET*10) * cluster.tickInterval)

	nextID := uint64(clusterSize) + 1

	client := cluster.newClient()
	stopc := make(chan struct{})
	go func() {
		defer close(stopc)
		removes := 0
		for {
			select {
			case <-stopc:
				timer.Stop()
				return
			case <-timer.C:
				fi++
				faults[fi%len(faults)](cluster)
			default:
			}

			if rand.Float64() < pReconf {
				if rand.Float64() < 0.5 {
					client.addNode(context.TODO(), nextID)
					nextID++
				} else if removes < clusterSize-1 {
					id := client.getEndpoint().id
					client.removeNode(context.TODO(), id)
					removes++
				}
			} else {
				ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*100)
				defer cancel()
				client.propose(ctx, []byte("data"))
			}
			time.Sleep(time.Millisecond * 1)
		}
	}()

	time.Sleep(time.Duration(durationInET*10) * cluster.tickInterval)
	stopc <- struct{}{}
	<-stopc

	cluster.stop()
}

func makeTraceLogFilename(t *testing.T) string {
	filename := fmt.Sprintf("%s-%x.ndjson", testNameCleanRegex.ReplaceAllString(t.Name(), ""), time.Now().UnixNano())
	root := "."
	if v, enabled := os.LookupEnv("TEST_TRACE_LOG"); enabled && len(v) > 0 {
		root = v
	}

	return filepath.Join(root, filename)
}

func TestRunClustersWithRandomFaults(t *testing.T) {
	rounds := 5
	for i := 0; i < rounds; i++ {
		logPath := makeTraceLogFilename(t)
		tl := newTraceLogger(logPath)
		// run for 10 election timeouts, with fault interval of 3 election timeouts
		runWithRandomFaults(5, tl, 0.05, 10, 3)
		tl.flush()
	}
}
