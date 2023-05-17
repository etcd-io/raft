// Copyright 2019 The etcd Authors
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
	"testing"

	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleTruncateAt(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	return env.TruncateAt(idx)
}

func (env *InteractionEnv) handleTruncate(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	return env.Truncate(idx)
}

func (env *InteractionEnv) TruncateAt(idx int) error {
	storage := env.Nodes[idx].Storage.(*snapOverrideStorage)
	if err := storage.TruncateAt(); err != nil {
		return err
	}
	return env.RaftLog(idx)
}

func (env *InteractionEnv) Truncate(idx int) error {
	storage := env.Nodes[idx].Storage.(*snapOverrideStorage)
	if err := storage.Truncate(); err != nil {
		return err
	}
	return env.RaftLog(idx)
}
