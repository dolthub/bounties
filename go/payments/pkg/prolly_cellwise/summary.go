package prolly_cellwise

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// AttributionShard is the ShardInfo object used by cellwise attribution to track a shard of.
type AttributionShard struct {
	// Table is the name of the table this shard is referring to
	Table string `json:"table"`
	// StartInclusive is the primary key of the start of the shard.  All keys within this shard will be equal to or greater
	// than this value. If this value is nil then it starts from the beginning of the data.
	StartInclusive []byte `json:"start"`
	// EndExclusive is the end value for this shard.  All keys within this shard will be less than this value. If the value
	// is nil then it ends at the end of the data.
	EndExclusive []byte `json:"end"`
	// Path is the ShardStore key used for persisting / retrieving the attribution data for this shard
	Path string `json:"path"`
	// CommitCounts is a slice of counts this shard has attributed to commits
	CommitCounts []uint64 `json:"commit_counts"`
}

var _ att.ShardInfo = (*AttributionShard)(nil)

// todo (dhruv): is this key okay?
func (as AttributionShard) Key(nbf *types.NomsBinFormat) string {
	return as.Table + "-" + hashOfTuple(as.StartInclusive) + "_" + hashOfTuple(as.EndExclusive)
}

func (as AttributionShard) DebugFormat(kd val.TupleDesc) string {
	return fmt.Sprintf("start: %s, end (exclusive): %s", kd.Format(as.StartInclusive), kd.Format(as.EndExclusive))
}

func (as AttributionShard) Equals(other interface{}) bool {
	otherShard, ok := other.(AttributionShard)
	if !ok {
		return false
	}

	return as.Table == otherShard.Table && bytes.Compare(as.StartInclusive, otherShard.StartInclusive) == 0 &&
		bytes.Compare(as.EndExclusive, otherShard.EndExclusive) == 0 && as.Path == otherShard.Path &&
		reflect.DeepEqual(as.CommitCounts, otherShard.CommitCounts)
}

func (as AttributionShard) serialize(wr io.Writer) error {
	enc := json.NewEncoder(wr)
	err := enc.Encode(as)
	if err != nil {
		return nil
	}
	return nil
}

func hashOfTuple(v val.Tuple) string {
	if len(v) == 0 {
		return ""
	}

	return hash.Of(v).String()
}

// ProllyAttSummary is the Summary implementation used by cellwise attribution to summarize the attribution for a
// given commit and supports marshalling to and from noms
type ProllyAttSummary struct {
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
func emptySummary(startHash hash.Hash) ProllyAttSummary {
	return ProllyAttSummary{
		StartHash:    startHash,
		CommitHashes: []hash.Hash{},
		CommitCounts: []uint64{},
		TableShards:  make(map[string][]AttributionShard),
	}
}

// NumCommits returns the number of commits that were built and summarized by this object
func (c ProllyAttSummary) NumCommits() int16 {
	return int16(len(c.CommitHashes))
}

// CommitToCount returns a map from commit hash to the number of changes attributed to them.
func (c ProllyAttSummary) CommitToCount(ctx context.Context) (map[hash.Hash]uint64, error) {
	commitToCount := make(map[hash.Hash]uint64)
	for i := range c.CommitHashes {
		commitToCount[c.CommitHashes[i]] = c.CommitCounts[i]
	}

	return commitToCount, nil
}

func (c ProllyAttSummary) tableNames() []string {
	tableNames := make([]string, 0, len(c.TableShards))
	for name := range c.TableShards {
		tableNames = append(tableNames, name)
	}

	return tableNames
}
