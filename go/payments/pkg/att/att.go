package att

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrSummaryDoesntExist = errors.New("summary doesn't exist")

type Summary interface {
	CommitToCount(ctx context.Context) (map[hash.Hash]uint64, error)
}

type ShardInfo interface{}
type ShardResult interface{}

type Method interface {
	EmptySummary(ctx context.Context) Summary
	ReadSummary(ctx context.Context, commitHash hash.Hash) (Summary, error)
	CollectShards(ctx context.Context, commit *doltdb.Commit, summary Summary) ([]ShardInfo, error)
	ProcessShard(ctx context.Context, commitIdx int16, cm, prevCm *doltdb.Commit, shardInfo ShardInfo) (ShardResult, error)
	ProcessResults(ctx context.Context, commitHash hash.Hash, prevSummary Summary, results []ShardResult) (Summary, error)
	WriteSummary(ctx context.Context, summary Summary) error
}
