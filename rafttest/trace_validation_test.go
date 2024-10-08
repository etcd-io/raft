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

//go:build with_tla

package rafttest

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

type Fault int

const (
	None Fault = iota
	Delay
	NonBlockingDelay
	Drop
	NetworkPartition
	NodeDown
	FaultCount
)

const (
	MaxDelayInMilliseconds int = 50 // 50ms as one election timeout
)

var testNameCleanRegex = regexp.MustCompile(`[^a-zA-Z0-9 \-_]+`)

func makeRandomFaults(n int) []func(*cluster) {
	var applier func(*cluster)
	appliers := []func(*cluster){}
	r := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		f := Fault(r.Intn(int(FaultCount)))
		switch f {
		case Delay, NonBlockingDelay:
			p := r.Float64()
			d := time.Millisecond * time.Duration(r.Intn(MaxDelayInMilliseconds))
			dual := r.Float32() < 0.5
			applier = makeDelayFault(delay{d: d, rate: p, block: f == Delay}, dual)
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
	const tickInterval = 20 * time.Millisecond
	cluster := newCluster(clusterConfig{
		size:         clusterSize,
		traceLogger:  tl,
		tickInterval: tickInterval,
	})

	faults := makeRandomFaults(durationInET / faultIntervalInET)
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
				cluster.faultc <- faults[fi%len(faults)]
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
			time.Sleep(tickInterval * 5)
		}
	}()

	time.Sleep(time.Duration(durationInET*10) * cluster.tickInterval)
	stopc <- struct{}{}
	<-stopc

	cluster.stop()
}

func TestTraceValidationWithRandomFaults(t *testing.T) {
	// working dir
	workDir := os.Getenv("WORKING_DIR")
	if workDir == "" {
		tmp, err := os.MkdirTemp("", "TestTraceValidationWithRandomFaults-")
		if err != nil {
			t.Fatalf("failed to create working directory")
		}
		workDir = tmp
	}

	t.Logf("Trace validation with random faults. Working directory: %s", workDir)

	// parallel
	nCPU := runtime.NumCPU()
	parallel := nCPU * 2 / 3
	if str := os.Getenv("PARALLEL"); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			t.Fatalf("failed to parse PARALLEL. PARALLEL: %s, Error: %s", str, err.Error())
		}
		if n > nCPU {
			n = nCPU
		}
		parallel = n
	}

	// trace count
	rounds := parallel
	if str := os.Getenv("TRACE_COUNT"); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			t.Fatalf("failed to parse TRACE_COUNT. TRACE_COUNT: %s, Error: %s", str, err.Error())
		}
		rounds = n
	}

	// Create cluster and run with random faults
	for i := 0; i < rounds; i++ {
		filename := fmt.Sprintf("%s-%d.ndjson", testNameCleanRegex.ReplaceAllString(t.Name(), ""), i)
		logPath := filepath.Join(workDir, filename)
		if err := os.Remove(logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed to clean up old log %s. Error: %s", logPath, err.Error())
		}
		tl := newTraceLogger(logPath)
		// run for 10 election timeouts, with fault interval of 2 election timeouts
		runWithRandomFaults(3, tl, 0.02, 10, 2)
		tl.flush()
	}

	// validate collected traces
	arguments := []string{
		"-p",
		fmt.Sprintf("%d", parallel),
		"-c",
		"../tla/Traceetcdraft.cfg",
		"-s",
		"../tla/Traceetcdraft.tla",
		"-w",
		workDir,
		fmt.Sprintf("%s/*.ndjson", workDir),
	}
	cmd := fmt.Sprintf("../tla/validate.sh %s", strings.Join(arguments, " "))
	if output, err := exec.Command("/bin/bash", "-c", cmd).Output(); err != nil {
		t.Errorf("trace validation failed. Output: %s", output)
	}
}
