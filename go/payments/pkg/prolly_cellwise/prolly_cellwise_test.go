package prolly_cellwise

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/attteststate"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func getCellwiseExpected() [][]uint64 {
	var expected [][]uint64

	// Expected Scoreboard:
	// Commit A: 1000 new = 2000
	expected = append(expected, []uint64{1000})

	// Expected Scoreboard:
	// Commit A: 1000 - (1*100 deleted + 1*100 col1 changes) = 800
	// Commit B: 3*100 new + (2 * 100 changed) = 500
	expected = append(expected, []uint64{800, 500})

	// Expected Scoreboard:
	// Commit A: 800 - (1*50 col1 changes) = 750
	// Commit B: 500 - (2*100 deleted + 2*50 col1/col2 changes) = 200
	// Commit C: 4*100 new + (3 * 100 changed) = 700
	expected = append(expected, []uint64{750, 200, 700})

	// Expected Scoreboard:
	// Commit A: 750
	// Commit B: 200
	// Commit C: 700 - (3*50 col1/col2/col3 changes = 550
	// Commit D: 3*50 col1/col2/col3 changes in 1000-1050 + 1*50 col3 changes in 1050-1100 = 400
	expected = append(expected, []uint64{750, 200, 550, 200})

	return expected
}

func assertOnExpectedAttribution(t *testing.T, expected []uint64, summary ProllyAttSummary) {
	require.Equal(t, expected, summary.CommitCounts)
}

func TestProllyAttribution(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}

	tests := []struct {
		name        string
		shardParams ProllyAttShardParams
	}{
		{
			"Million rows per shard",
			ProllyAttShardParams{
				MaximumShardCardinality: 1_000_000,
			},
		},
		{
			"100 rows per shard",
			ProllyAttShardParams{
				MaximumShardCardinality: 100,
			},
		},
		{
			"31 rows per shard",
			ProllyAttShardParams{
				MaximumShardCardinality: 31,
			},
		},
		{
			"101 rows per shard",
			ProllyAttShardParams{
				MaximumShardCardinality: 101,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			buildDir := os.TempDir()

			dEnv := doltutils.GetTestEnv(ctx, t)
			startOfBountyHash, cm, err := attteststate.GenTestCommitGraph(ctx, dEnv.DoltDB, doltutils.CreateMeta(t))
			require.NoError(t, err)

			expected := getCellwiseExpected()

			commits, err := doltutils.GetMergeCommitsAfter(ctx, dEnv.DoltDB, cm, startOfBountyHash)
			require.NoError(t, err)
			shardStore, err := att.NewFilesysShardStore(filepath.Join(buildDir, startOfBountyHash.String()))
			require.NoError(t, err)
			logger, err := zap.NewDevelopment()
			require.NoError(t, err)
			method := NewMethod(logger, dEnv.DoltDB, startOfBountyHash, shardStore, test.shardParams)
			require.Equal(t, len(expected), len(commits))

			var prevSummary att.Summary = emptySummary(startOfBountyHash)
			var prevCommit *doltdb.Commit
			startCommit := commits[0]

			if startCommit.NumParents() != 0 {
				prevCommit, err = startCommit.GetParent(ctx, 0)
				require.NoError(t, err)
			}

			for i := 0; i < len(expected); i++ {
				commit := commits[i]
				require.NoError(t, err)

				shards, err := method.CollectShards(ctx, commit, prevCommit, prevSummary)
				require.NoError(t, err)

				for j := range shards {
					serialized, err := method.SerializeShardInfo(ctx, shards[j])
					require.NoError(t, err)
					deserialized, err := method.DeserializeShardInfo(ctx, serialized)
					require.NoError(t, err)

					unchanged, unchangedOK := shards[j].(UnchangedShard)

					if unchangedOK {
						deserializedUnchanged, ok := deserialized.(UnchangedShard)
						require.True(t, ok)
						require.True(t, unchanged.AttributionShard.Equals(deserializedUnchanged.AttributionShard))
					} else {
						require.True(t, shards[j].(AttributionShard).Equals(deserialized))
					}
				}

				var results []att.ShardResult
				for _, shard := range shards {
					result, err := method.ProcessShard(ctx, int16(i), commit, prevCommit, shard)
					require.NoError(t, err)
					require.NotNil(t, result)
					require.Len(t, result.([]AttributionShard), 1)
					shard := result.([]AttributionShard)[0]
					require.NotNil(t, shard.CommitCounts)

					results = append(results, result)
				}

				for j := range results {
					serialized, err := method.SerializeResults(ctx, results[j])
					require.NoError(t, err)
					deserialized, err := method.DeserializeResults(ctx, serialized)
					require.NoError(t, err)

					resShards := results[j].([]AttributionShard)
					deserResults := deserialized.([]AttributionShard)
					for k := 0; k < len(resShards); k++ {
						require.True(t, resShards[k].Equals(deserResults[k]))
					}
				}

				commitHash, err := commit.HashOf()
				require.NoError(t, err)

				newSummary, err := method.ProcessResults(ctx, commitHash, prevSummary, results)
				require.NoError(t, err)

				expectedAtt := expected[i]
				assertOnExpectedAttribution(t, expectedAtt, newSummary.(ProllyAttSummary))

				summaryKey, err := method.WriteSummary(ctx, newSummary)
				require.NoError(t, err)
				prevSummary, err = method.ReadSummary(ctx, summaryKey)
				require.NoError(t, err)

				t.Logf("============= COMMIT: %s DONE =============", commitHash.String())
				prevCommit = commit
			}
		})
	}
}
