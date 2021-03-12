// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/bounties/go/payments/pkg/att"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// AttributionShard is the ShardInfo object used by cellwise attribution to track a shard of.
type AttributionShard struct {
	// Table is the name of the table this shard is referring to
	Table string `noms:"table"`
	// StartInclusive is the primary key of the start of the shard.  All keys within this shard will be equal to or greater
	// than this value. If this value is nil then it starts from the beginning of the data.
	StartInclusive types.Value `noms:"start"`
	// EndExclusive is the end value for this shard.  All keys within this shard will be less than this value. If the value
	// is nil then it ends at the end of the data.
	EndExclusive types.Value `noms:"end"`
	// Path is the ShardStore key used for persisting / retrieving the attribution data for this shard
	Path string `noms:"path"`
	// CommitCounts is a slice of counts this shard has attributed to commits
	CommitCounts []uint64 `noms:"commit_counts"`
}

// inRangeFunc will return a function that validates a key against the end value
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

// UnchangedShard is used to mark shards which have not changed since they were last processed
type UnchangedShard struct {
	AttributionShard
}

var _ att.Summary = (*CellwiseAttSummary)(nil)

// CellwiseAttSummary is the Summary implementation used by cellwise attribution to summarize the attribution for a
// given commit and supports marshalling to and from noms
type CellwiseAttSummary struct {
	// StartHash is the commit hash at the start of attribution before the first attributed commit is merged
	StartHash hash.Hash `noms:"start_hash"`
	// CommitHashes is an ordered list of the commit hashes that have been processed. The current commit will be the
	// last element in the slice
	CommitHashes []hash.Hash `noms:"commit_hashes"`
	// CommitCounts provides the number of cellwise attribution changes that are attributed to each of the commits.
	CommitCounts []uint64 `noms:"commit_counts"`
	// TableShards tracks the AttributionShard for each table used to build attribution
	TableShards map[string][]AttributionShard `noms:"table_to_shards"`
}

// emptySummary returns an empty CellwiseAttSummary object
func emptySummary(startHash hash.Hash) CellwiseAttSummary {
	return CellwiseAttSummary{
		StartHash:    startHash,
		CommitHashes: []hash.Hash{},
		CommitCounts: []uint64{},
		TableShards:  make(map[string][]AttributionShard),
	}
}

// NumCommits returns the number of commits that were built and summarized by this object
func (c CellwiseAttSummary) NumCommits() int16 {
	return int16(len(c.CommitHashes))
}

// CommitToCount returns a map from commit hash to the number of changes attributed to them.
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
