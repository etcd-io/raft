// Copyright 2021 The etcd Authors
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

package raftpb

import (
	"math/bits"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestProtoMemorySizes(t *testing.T) {
	if64Bit := func(yes, no uintptr) uintptr {
		if bits.UintSize == 64 {
			return yes
		}
		return no
	}

	var e Entry
	assert.Equal(t, if64Bit(48, 32), unsafe.Sizeof(e), "Entry size check")

	var sm SnapshotMetadata
	assert.Equal(t, if64Bit(120, 68), unsafe.Sizeof(sm), "SnapshotMetadata size check")

	var s Snapshot
	assert.Equal(t, if64Bit(144, 80), unsafe.Sizeof(s), "Snapshot size check")

	var m Message
	assert.Equal(t, if64Bit(160, 112), unsafe.Sizeof(m), "Message size check")

	var hs HardState
	assert.Equal(t, uintptr(24), unsafe.Sizeof(hs), "HardState size check")

	var cs ConfState
	assert.Equal(t, if64Bit(104, 52), unsafe.Sizeof(cs), "ConfState size check")

	var cc ConfChange
	assert.Equal(t, if64Bit(48, 32), unsafe.Sizeof(cc), "ConfChange size check")

	var ccs ConfChangeSingle
	assert.Equal(t, if64Bit(16, 12), unsafe.Sizeof(ccs), "ConfChangeSingle size check")

	var ccv2 ConfChangeV2
	assert.Equal(t, if64Bit(56, 28), unsafe.Sizeof(ccv2), "ConfChangeV2 size check")
}
