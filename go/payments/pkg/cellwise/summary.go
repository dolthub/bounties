package cellwise

import (
	"context"

	"github.com/dolthub/bounties/go/payments/pkg/att"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type AttributionShard struct {
	Table          string      `noms:"table"`
	StartInclusive types.Value `noms:"start"`
	EndExclusive   types.Value `noms:"end"`
	Path           string      `noms:"path"`
	CommitCounts   []uint64    `noms:"commit_counts"`
}

func (as AttributionShard) inRangeFunc(nbf *types.NomsBinFormat) func(v types.Value) (bool, error) {
	if types.IsNull(as.EndExclusive) {
		return func(value types.Value) (bool, error) {
			return true, nil
		}
	}

	return func(value types.Value) (bool, error) {
		return value.Less(nbf, as.EndExclusive)
	}
}

type UnchangedShard struct {
	AttributionShard
}

var _ att.Summary = (*CellwiseAttSummary)(nil)

type CellwiseAttSummary struct {
	StartHash    hash.Hash                     `noms:"start_hash"`
	CommitHashes []hash.Hash                   `noms:"commit_hashes"`
	CommitCounts []uint64                      `noms:"commit_counts"`
	TableShards  map[string][]AttributionShard `noms:"table_to_shards"`
}

func emptySummary(startHash hash.Hash) CellwiseAttSummary {
	return CellwiseAttSummary{
		StartHash:    startHash,
		CommitHashes: []hash.Hash{},
		CommitCounts: []uint64{},
		TableShards:  make(map[string][]AttributionShard),
	}
}

func (c CellwiseAttSummary) NumCommits() int16 {
	return int16(len(c.CommitHashes))
}

func (c CellwiseAttSummary) CommitToCount(ctx context.Context) (map[hash.Hash]uint64, error) {
	commitToCount := make(map[hash.Hash]uint64)
	for i := range c.CommitHashes {
		commitToCount[c.CommitHashes[i]] = c.CommitCounts[i]
	}

	return commitToCount, nil
}

func (c CellwiseAttSummary) tableNames() []string {
	tableNames := make([]string, 0, len(c.TableShards))
	for name := range c.TableShards {
		tableNames = append(tableNames, name)
	}

	return tableNames
}
