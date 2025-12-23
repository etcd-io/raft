// Copyright 2025 The etcd Authors
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

package quorum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescribe(t *testing.T) {
	testCases := []struct {
		name           string
		c              MajorityConfig
		indexer        mapAckIndexer
		expectedResult string
	}{
		{
			name: "different commit index for each voter",
			c: MajorityConfig{
				1: struct{}{},
				2: struct{}{},
				3: struct{}{},
			},
			indexer: mapAckIndexer{
				1: 101,
				2: 102,
				3: 103,
			},
			expectedResult: `       idx
>      101    (id=1)
x>     102    (id=2)
xx>    103    (id=3)
`,
		},
		{
			name: "same commit index for all voters",
			c: MajorityConfig{
				1: struct{}{},
				2: struct{}{},
				3: struct{}{},
			},
			indexer: mapAckIndexer{
				1: 100,
				2: 100,
				3: 100,
			},
			expectedResult: `       idx
>      100    (id=1)
>      100    (id=2)
>      100    (id=3)
`,
		},
		{
			name: "same commit index for part of the voters",
			c: MajorityConfig{
				1: struct{}{},
				2: struct{}{},
				3: struct{}{},
			},
			indexer: mapAckIndexer{
				1: 100,
				2: 102,
				3: 102,
			},
			expectedResult: `       idx
>      100    (id=1)
x>     102    (id=2)
x>     102    (id=3)
`,
		},
		{
			name: "missing some commit indexes",
			c: MajorityConfig{
				1: struct{}{},
				2: struct{}{},
				3: struct{}{},
			},
			indexer: mapAckIndexer{
				1: 101,
				3: 103,
			},
			expectedResult: `       idx
x>     101    (id=1)
?        0    (id=2)
xx>    103    (id=3)
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualResult := tc.c.Describe(tc.indexer)
			require.Equal(t, tc.expectedResult, actualResult)
		})
	}

}
