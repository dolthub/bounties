// Copyright 2020 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package cellwise

import (
	"context"
	"fmt"
	"github.com/dolthub/bounties/go/payments/pkg/attteststate"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

const (
	testUsername = "Test User"
	testEmail    = "test@fake.horse"
)

func getTestEnv(ctx context.Context, t *testing.T) *env.DoltEnv {
	const (
		homeDir         = "/Users/madam"
		relativeTestDir = "databases/test"
	)

	testDir := filepath.Join(homeDir, relativeTestDir)
	hdp := func() (string, error) { return homeDir, nil }
	fs := filesys.NewInMemFS([]string{testDir}, nil, testDir)
	dEnv := env.Load(ctx, hdp, fs, doltdb.InMemDoltDB, "")
	require.NoError(t, dEnv.DocsLoadErr)
	require.NoError(t, dEnv.DBLoadError)
	require.NoError(t, dEnv.CfgLoadErr)
	require.Error(t, dEnv.RSLoadErr)

	err := dEnv.InitDBAndRepoState(ctx, types.Format_Default, testUsername, testEmail, time.Now())
	require.NoError(t, err)

	return dEnv
}

func getCellwiseExpected() [][]int {
	var expected [][]int

	// Expected Scoreboard:
	// Commit 1: 2*1000 new = 2000
	expected = append(expected, []int{2000})

	// Expected Scoreboard:
	// Commit 1: 2000 - (2*100 deleted + 1*100 col1 changes) = 1700
	// Commit 2: 3*100 new + (2 * 100 changed) = 500
	expected = append(expected, []int{1700, 500})

	// Expected Scoreboard:
	// Commit 1: 1700 - (1*100 deleted + 1*50 col1 changes) = 1550
	// Commit 2: 500 - (2*100 deleted + 2*50 col1/col2 changes) = 200
	// Commit 3: 4*100 new + (3 * 100 changed) = 700
	expected = append(expected, []int{1550, 200, 700})

	// Expected Scoreboard:
	// Commit 1: 1550 + 2*100 re-added = 1750
	// Commit 2: 200 + 1*100 re-added = 300
	// Commit 3: 700 - 200 col3 cells removed from schema = 500
	// Commit 4: 100 rows with col4 = 100
	expected = append(expected, []int{1750, 300, 500, 100})

	// Expected Scoreboard:
	// Commit 1: 1750 - 1*100 col1 changed = 1650
	// Commit 2: 300 + 1*100 col1 reverted to "b" = 400
	// Commit 3: 500
	// Commit 4: 100
	// Commit 5: 0
	expected = append(expected, []int{1650, 400, 500, 100, 0})

	return expected
}

func assertOnExpectedAttribution(_ context.Context, t *testing.T, expected []int, att *TableAttribution) {
	var expectedTotal int64
	for commitIdx, count := range att.CommitToCellCount {
		expectedTotal += int64(expected[commitIdx])
		assert.Equal(t, expected[commitIdx], count)
	}
	assert.Equal(t, expectedTotal, att.AttributedCells)
}

func createMeta(t *testing.T) [attteststate.NumCommits]*doltdb.CommitMeta {
	var meta [attteststate.NumCommits]*doltdb.CommitMeta
	for i := 0; i < attteststate.NumCommits; i++ {
		var err error
		meta[i], err = doltdb.NewCommitMeta(testUsername, testEmail, fmt.Sprintf("Commit %d", i))
		require.NoError(t, err)
	}

	return meta
}

func TestAttribution(t *testing.T) {
	ctx := context.Background()
	dEnv := getTestEnv(ctx, t)
	startOfBountyHash, cm, err := attteststate.GenTestCommitGraph(ctx, dEnv.DoltDB, createMeta(t))
	require.NoError(t, err)

	expected := getCellwiseExpected()

	commits, err := doltutils.GetMergeCommitsAfter(ctx, dEnv.DoltDB, cm, startOfBountyHash)
	require.NoError(t, err)
	att := NewDatabaseAttribution(startOfBountyHash)
	require.Equal(t, len(expected), len(commits))

	for i := 0; i < len(expected); i++ {
		expectedAtt := expected[i]
		commit := commits[len(commits)-(i+1)]
		commitHash, err := commit.HashOf()
		require.NoError(t, err)
		err = att.Update(ctx, dEnv.DoltDB, startOfBountyHash, commitHash)
		require.NoError(t, err)
		cellAtt, ok := att.NameToTableAttribution[attteststate.TestTableName]
		require.True(t, ok)
		assertOnExpectedAttribution(ctx, t, expectedAtt, cellAtt)

		bytes, err := Serialize(att)
		require.NoError(t, err)
		att, err = Deserialize(bytes)
		require.NoError(t, err)
	}
}
