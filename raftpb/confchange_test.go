// Copyright 2026 The etcd Authors
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
	"reflect"
	"testing"
)

func TestLeaveJoint(t *testing.T) {
	// Verify that ConfChangeV2 has exactly the expected fields.
	// This ensures that if new fields are added to ConfChangeV2, this test will fail.
	ccv2Type := reflect.TypeOf((*ConfChangeV2)(nil)).Elem()
	expectedFieldCount := 3 // Transition, Changes, Context
	if ccv2Type.NumField() != expectedFieldCount {
		t.Fatalf("ConfChangeV2 has %d fields, expected %d. If new fields were added, please update this test.",
			ccv2Type.NumField(), expectedFieldCount)
	}

	tests := []struct {
		name     string
		cc       ConfChangeV2
		expected bool
	}{
		{
			name: "empty conf change (all zero values)",
			cc: ConfChangeV2{
				Transition: nil,
				Changes:    nil,
				Context:    nil,
			},
			expected: true,
		},
		{
			name: "empty conf change with auto transition explicitly set",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionAuto.Enum(),
				Changes:    nil,
				Context:    nil,
			},
			expected: true,
		},
		{
			name: "empty changes with context (context should be ignored)",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionAuto.Enum(),
				Changes:    nil,
				Context:    []byte("some context"),
			},
			expected: true,
		},

		{
			name: "empty changes slice with auto transition",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionAuto.Enum(),
				Changes:    []*ConfChangeSingle{},
				Context:    nil,
			},
			expected: true,
		},
		{
			name: "non-empty changes",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionAuto.Enum(),
				Changes: []*ConfChangeSingle{
					{
						Type:   ConfChangeAddNode.Enum(),
						NodeId: new(uint64),
					},
				},
				Context: nil,
			},
			expected: false,
		},
		{
			name: "joint implicit transition with empty changes",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionJointImplicit.Enum(),
				Changes:    nil,
				Context:    nil,
			},
			expected: false,
		},
		{
			name: "joint explicit transition with empty changes",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionJointExplicit.Enum(),
				Changes:    nil,
				Context:    nil,
			},
			expected: false,
		},
		{
			name: "auto transition with multiple changes",
			cc: ConfChangeV2{
				Transition: ConfChangeTransition_ConfChangeTransitionAuto.Enum(),
				Changes: []*ConfChangeSingle{
					{Type: ConfChangeAddNode.Enum(), NodeId: new(uint64)},
					{Type: ConfChangeRemoveNode.Enum(), NodeId: new(uint64)},
				},
				Context: nil,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cc.LeaveJoint()
			if got != tt.expected {
				t.Errorf("LeaveJoint() = %v, want %v", got, tt.expected)
			}
		})
	}
}
