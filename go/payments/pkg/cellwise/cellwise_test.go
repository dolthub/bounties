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

package cellwise

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/attteststate"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
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

func getCellwiseExpected() [][]uint64 {
	var expected [][]uint64

	// Expected Scoreboard:
	// Commit 1: 2*1000 new = 2000
	expected = append(expected, []uint64{2000})

	// Expected Scoreboard:
	// Commit 1: 2000 - (2*100 deleted + 1*100 col1 changes) = 1700
	// Commit 2: 3*100 new + (2 * 100 changed) = 500
	expected = append(expected, []uint64{1700, 500})

	// Expected Scoreboard:
	// Commit 1: 1700 - (1*100 deleted + 1*50 col1 changes) = 1550
	// Commit 2: 500 - (2*100 deleted + 2*50 col1/col2 changes) = 200
	// Commit 3: 4*100 new + (3 * 100 changed) = 700
	expected = append(expected, []uint64{1550, 200, 700})

	// Expected Scoreboard:
	// Commit 1: 1550 + 2*100 re-added = 1750
	// Commit 2: 200 + 1*100 re-added = 300
	// Commit 3: 700 - 200 col3 cells removed from schema = 500
	// Commit 4: 100 rows with col4 = 100
	expected = append(expected, []uint64{1750, 300, 500, 100})

	// Expected Scoreboard:
	// Commit 1: 1750 - 1*100 col1 changed = 1650
	// Commit 2: 300 + 1*100 col1 reverted to "b" = 400
	// Commit 3: 500
	// Commit 4: 100
	// Commit 5: 0
	expected = append(expected, []uint64{1650, 400, 500, 100, 0})

	return expected
}

func assertOnExpectedAttribution(t *testing.T, expected []uint64, summary CellwiseAttSummary) {
	fmt.Println(summary.CommitCounts)
	require.Equal(t, expected, summary.CommitCounts)
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
	tests := []struct {
		name        string
		shardParams CWAttShardParams
	}{
		{
			"Million rows per shard",
			CWAttShardParams{
				RowsPerShard: 1_000_000,
			},
		},
		{
			"100 rows per shard",
			CWAttShardParams{
				RowsPerShard: 100,
			},
		},
		{
			"31 rows per shard",
			CWAttShardParams{
				RowsPerShard: 31,
			},
		},
		{
			"31 rows per shard, min shard size 11",
			CWAttShardParams{
				RowsPerShard: 31,
				MinShardSize: 11,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			buildDir := os.TempDir()

			dEnv := getTestEnv(ctx, t)
			startOfBountyHash, cm, err := attteststate.GenTestCommitGraph(ctx, dEnv.DoltDB, createMeta(t))
			require.NoError(t, err)

			expected := getCellwiseExpected()

			commits, err := doltutils.GetMergeCommitsAfter(ctx, dEnv.DoltDB, cm, startOfBountyHash)
			require.NoError(t, err)
			shardStore, err := att.NewFilesysShardStore(buildDir)
			require.NoError(t, err)
			cwAtt := NewCWAtt(dEnv.DoltDB, startOfBountyHash.String(), startOfBountyHash, shardStore, test.shardParams)
			require.Equal(t, len(expected), len(commits))

			var summary att.Summary = emptySummary(startOfBountyHash)
			var prevCommit *doltdb.Commit
			for i := 0; i < len(expected); i++ {
				commit := commits[int16(len(commits)-(i+1))]
				require.NoError(t, err)

				shards, err := cwAtt.CollectShards(ctx, commit, prevCommit, summary)
				require.NoError(t, err)

				var results []att.ShardResult
				for _, shard := range shards {
					result, err := cwAtt.ProcessShard(ctx, int16(i), commit, prevCommit, shard)
					require.NoError(t, err)

					results = append(results, result)
				}

				commitHash, err := commit.HashOf()
				require.NoError(t, err)

				summary, err = cwAtt.ProcessResults(ctx, commitHash, summary, results)
				require.NoError(t, err)

				expectedAtt := expected[i]
				assertOnExpectedAttribution(t, expectedAtt, summary.(CellwiseAttSummary))

				h, err := commit.HashOf()
				require.NoError(t, err)
				err = cwAtt.WriteSummary(ctx, summary)
				require.NoError(t, err)
				summary, err = cwAtt.ReadSummary(ctx, h)
				require.NoError(t, err)

				prevCommit = commit
			}
		})
	}
}
