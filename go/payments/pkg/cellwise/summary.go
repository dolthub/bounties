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
	"errors"
	"io"
	"reflect"

	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/valuefile"

	"github.com/dolthub/bounties/go/payments/pkg/att"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func deserializeShard(ctx context.Context, nbf *types.NomsBinFormat, rd io.Reader) (AttributionShard, error) {
	vf, err := valuefile.ReadFromReader(ctx, rd)
	if err != nil {
		return AttributionShard{}, err
	} else if len(vf.Values) != 1 {
		return AttributionShard{}, errors.New("corrupt shard info")
	}

	var shard AttributionShard
	err = marshal.Unmarshal(ctx, nbf, vf.Values[0], &shard)
	if err != nil {
		return AttributionShard{}, err
	}

	if _, ok := shard.StartInclusive.(types.Tuple); !ok {
		shard.StartInclusive = types.NullValue
	}

	if _, ok := shard.EndExclusive.(types.Tuple); !ok {
		shard.EndExclusive = types.NullValue
	}

	return shard, nil
}

// AttributionShard is the ShardInfo object used by cellwise attribution to track a shard of.
type AttributionShard struct {
	// Table is the name of the table this shard is referring to
	Table string `noms:"table" json:"table"`
	// StartInclusive is the primary key of the start of the shard.  All keys within this shard will be equal to or greater
	// than this value. If this value is nil then it starts from the beginning of the data.
	StartInclusive types.Value `noms:"start" json:"start"`
	// EndExclusive is the end value for this shard.  All keys within this shard will be less than this value. If the value
	// is nil then it ends at the end of the data.
	EndExclusive types.Value `noms:"end" json:"end"`
	// Path is the ShardStore key used for persisting / retrieving the attribution data for this shard
	Path string `noms:"path" json:"path"`
	// CommitCounts is a slice of counts this shard has attributed to commits
	CommitCounts []uint64 `noms:"commit_counts" json:"commit_counts"`
}

// inRangeFunc will return a function that validates a key against the end value
func (as AttributionShard) inRangeFunc(ctx context.Context, nbf *types.NomsBinFormat) func(ctx context.Context, v types.Value) (bool, bool, error) {
	if types.IsNull(as.EndExclusive) {
		return func(ctx context.Context, value types.Value) (bool, bool, error) {
			return true, false, nil
		}
	}

	return func(ctx context.Context, value types.Value) (bool, bool, error) {
		res, err := value.Less(nbf, as.EndExclusive)
		return res, false, err
	}
}

func (as AttributionShard) Equals(other interface{}) bool {
	otherShard, ok := other.(AttributionShard)

	if ok {
		return as.StartInclusive.Equals(otherShard.StartInclusive) &&
			as.EndExclusive.Equals(otherShard.EndExclusive) &&
			as.Table == otherShard.Table &&
			as.Path == otherShard.Path &&
			reflect.DeepEqual(as.CommitCounts, otherShard.CommitCounts)
	}

	return false
}

// Key returns a string based on the table, start and end values for the shard the format being table-<start-hash>_<end-hasd>.
// if the start or end keys are nil "" will be used in their place.  A shard named "table-_" will be the full range of values,
// "table-b03dtu0alc0piqlu8s5q7dibmt4kdn8_" would be a shard staring from the key that has hash b03dtu0alc0piqlu8s5q7dibmt4kdn8
// and all the rows that follow. "table-_b03dtu0alc0piqlu8s5q7dibmt4kdn8" would we all keys coming before the key with hash
// b03dtu0alc0piqlu8s5q7dibmt4kdn8.  "table-24pvcimjhuolbnr801nn1q8bq1f91u9j_s72v43bh8kiopalsldg1okgh96gpberv" would be all values
// between the keys with hashes 24pvcimjhuolbnr801nn1q8bq1f91u9j and s72v43bh8kiopalsldg1okgh96gpberv. The key is unique within a
// commit
func (as AttributionShard) Key(nbf *types.NomsBinFormat) string {
	return as.Table + "-" + hashValToString(as.StartInclusive, nbf) + "_" + hashValToString(as.EndExclusive, nbf)
}

func (as AttributionShard) serialize(ctx context.Context, nbf *types.NomsBinFormat, wr io.Writer) error {
	store, err := valuefile.NewFileValueStore(nbf)
	if err != nil {
		return err
	}

	v, err := marshal.Marshal(ctx, store, as)
	if err != nil {
		return err
	}

	_, err = store.WriteValue(ctx, v)
	if err != nil {
		return err
	}

	err = valuefile.WriteToWriter(ctx, wr, store, v)
	if err != nil {
		return err
	}

	return nil
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
	StartHash hash.Hash `noms:"start_hash" json:"start_hash"`
	// CommitHashes is an ordered list of the commit hashes that have been processed. The current commit will be the
	// last element in the slice
	CommitHashes []hash.Hash `noms:"commit_hashes" json:"commit_hashes"`
	// CommitCounts provides the number of cellwise attribution changes that are attributed to each of the commits.
	CommitCounts []uint64 `noms:"commit_counts" json:"commit_counts"`
	// TableShards tracks the AttributionShard for each table used to build attribution
	TableShards map[string][]AttributionShard `noms:"table_to_shards" json:"table_to_shard"`
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
