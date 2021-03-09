package cellwise

import (
	"context"
	"errors"
	"strings"

	"github.com/dolthub/bounties/go/payments/pkg/att"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

// shardManager manages writing of the output shards
type shardManager struct {
	nbf           *types.NomsBinFormat
	shardBasePath string
	shardStore    att.ShardStore
	shardParams   CWAttShardParams
	table         string
	inputShard    AttributionShard
	numCommits    int
	rowAttBuff    *rowAttEncodingBuffers

	startKey        types.Value
	currStore       *valuefile.FileValueStore
	rowsInCurrShard uint64
	streamMapCh     chan types.Value
	outMap          *types.StreamingMap
	commitCounts    []uint64

	shards []AttributionShard
}

func NewShardManager(nbf *types.NomsBinFormat, numCommits int, inputShard AttributionShard, table, shardBasePath string, shardParams CWAttShardParams, shardStore att.ShardStore) *shardManager {
	return &shardManager{
		nbf:           nbf,
		table:         table,
		inputShard:    inputShard,
		numCommits:    numCommits,
		rowAttBuff:    NewRowAttEncodingBuffers(),
		shardBasePath: shardBasePath,
		shardStore:    shardStore,
		shardParams:   shardParams,
	}
}

func (sm *shardManager) getShards() []AttributionShard {
	return sm.shards
}

func (sm *shardManager) addRowAtt(ctx context.Context, key types.Value, ra rowAtt, raVal types.Value) error {
	// check and shard
	if sm.rowsInCurrShard >= sm.shardParams.RowsPerShard {
		err := sm.closeCurrentShard(ctx, key)

		if err != nil {
			return err
		}
	}

	if sm.currStore == nil {
		err := sm.openNewShard(ctx, key)

		if err != nil {
			return err
		}
	}

	if raVal == nil {
		var err error
		raVal, err = ra.AsValue(sm.nbf, sm.rowAttBuff)
		if err != nil {
			return err
		}
	}

	sm.streamMapCh <- key
	sm.streamMapCh <- raVal
	sm.rowsInCurrShard++

	for _, ca := range ra {
		if ca.CurrentOwner != -1 {
			sm.commitCounts[ca.CurrentOwner]++
		}
	}

	return nil
}

func (sm *shardManager) close(ctx context.Context) error {
	return sm.closeCurrentShard(ctx, sm.inputShard.EndExclusive)
}

func (sm *shardManager) closeCurrentShard(ctx context.Context, end types.Value) error {
	if sm.rowsInCurrShard > 0 {
		close(sm.streamMapCh)
		m, err := sm.outMap.Wait()

		if err != nil {
			return err
		}

		_, err = sm.currStore.WriteValue(ctx, m)

		if err != nil {
			return err
		}

		startKey := sm.startKey
		if len(sm.shards) == 0 {
			startKey = sm.inputShard.StartInclusive
		}

		path := sm.shardStore.Join(sm.shardBasePath, shardName(sm.nbf, startKey, end))
		err = sm.shardStore.WriteShard(ctx, path, sm.currStore, m)

		if err != nil {
			return err
		}

		sm.shards = append(sm.shards, AttributionShard{
			Table:          sm.table,
			StartInclusive: startKey,
			EndExclusive:   end,
			Path:           path,
			CommitCounts:   sm.commitCounts,
		})

		sm.startKey = nil
		sm.currStore = nil
		sm.rowsInCurrShard = 0
		sm.streamMapCh = nil
		sm.outMap = nil
		sm.commitCounts = nil
	}

	return nil
}

func (sm *shardManager) openNewShard(ctx context.Context, startKey types.Value) error {
	if sm.rowsInCurrShard != 0 {
		return errors.New("opening new shard while previous shard not closed")
	}

	var err error
	sm.currStore, err = valuefile.NewFileValueStore(sm.nbf)

	if err != nil {
		return err
	}

	sm.streamMapCh = make(chan types.Value, 128)
	sm.outMap = types.NewStreamingMap(ctx, sm.currStore, sm.streamMapCh)
	sm.startKey = startKey
	sm.commitCounts = make([]uint64, sm.numCommits)

	return nil
}

func shardName(nbf *types.NomsBinFormat, startInclusive, endExclusive types.Value) string {
	return strings.Join([]string{hashValToString(startInclusive, nbf), hashValToString(endExclusive, nbf)}, "_")
}

func hashValToString(v types.Value, nbf *types.NomsBinFormat) string {
	if types.IsNull(v) {
		return ""
	}

	h, err := v.Hash(nbf)

	if err != nil {
		panic(err)
	}

	return h.String()
}
