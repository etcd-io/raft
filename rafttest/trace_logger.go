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
	"go.uber.org/zap"

	"go.etcd.io/raft/v3"
)

type traceLogger struct {
	lg *zap.Logger
}

func newTraceLogger(path string) *traceLogger {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{path}
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	cfg.Sampling = nil

	lg, _ := cfg.Build()
	return &traceLogger{
		lg: lg,
	}
}

func (t *traceLogger) TraceEvent(ev *raft.TracingEvent) {
	t.lg.Debug("trace", zap.String("tag", "trace"), zap.Any("event", ev))
}

func (t *traceLogger) flush() {
	t.lg.Sync()
}
