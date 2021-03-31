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

package att

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// ErrSummaryDoesntExist is the error returned when trying to read a summary for a given key that doesn't exist
var ErrSummaryDoesntExist = errors.New("summary doesn't exist")

// Summary is an interface which provides access to the attribution results
type Summary interface {
	// CommitToCount returns a map from commit hashes to the changes attributed to the commit
	CommitToCount(ctx context.Context) (map[hash.Hash]uint64, error)
}

// ShardInfo is an interface which contains data necessary for processing a single shard of data
type ShardInfo interface {
	Key(*types.NomsBinFormat) string
}

// ShardResult is an interface which contains the result of processing a shard of data
type ShardResult interface{}

// AttributionMethod is an interface for attributing changes to commits
type AttributionMethod interface {
	// EmptySummary returns an empty summary for the given method type
	EmptySummary(ctx context.Context) Summary

	// ReadSummary reads a summary for a commit hash
	ReadSummary(ctx context.Context, key string) (Summary, error)

	// CollectShards gathers all the shards that need to be processed
	CollectShards(ctx context.Context, commit, prevCommit *doltdb.Commit, summary Summary) ([]ShardInfo, error)

	// ProcessShard processes a single shard
	ProcessShard(ctx context.Context, commitIdx int16, cm, prevCm *doltdb.Commit, shardInfo ShardInfo) (ShardResult, error)

	// ProcessResults takes all the results from processing all the shards and returns a summary
	ProcessResults(ctx context.Context, commitHash hash.Hash, prevSummary Summary, results []ShardResult) (Summary, error)

	// WriteSummary persists a summary
	WriteSummary(ctx context.Context, summary Summary) (string, error)

	// SerializeShardInfo takes a ShardInfo object and serializes it
	SerializeShardInfo(ctx context.Context, info ShardInfo) ([]byte, error)

	// DeserializeShardInfo takes a []byte and deserializes it to a ShardInfo object
	DeserializeShardInfo(ctx context.Context, data []byte) (ShardInfo, error)

	// SerializeResults takes a ShardResult object and serializes it
	SerializeResults(ctx context.Context, results ShardResult) ([]byte, error)

	// DeserializeResults takes a []bite and deserializes it ta a ShardResult
	DeserializeResults(ctx context.Context, data []byte) (ShardResult, error)
}
