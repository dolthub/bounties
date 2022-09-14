// Copyright 2022 Dolthub, Inc.
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

package prolly_cellwise

import (
	"context"
	"time"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/dolt/go/store/valuefile"
	"go.uber.org/zap"
)

// prollyShardManager manages writing of output shards
type prollyShardManager struct {
	logger        *zap.Logger
	nbf           *types.NomsBinFormat
	shardBasePath string
	kb            *val.TupleBuilder
	attVb         *val.TupleBuilder
	pool          pool.BuffPool
	shardStore    att.ShardStore
	shardParams   ProllyAttShardParams
	table         string
	inputShard    AttributionShard
	numCommits    int

	currStore    *valuefile.FileValueStore
	nodeStore    tree.NodeStore
	mut          *prolly.MutableMap
	commitCounts []uint64

	seen int

	lastAdded val.Tuple
}

// NewProllyShardManager takes an input shard, a ShardStore, some ShardParams and some other metadata and returns a prollyShardManager
// which is used to manage dynamic sharding, and persisting of shard data
func NewProllyShardManager(logger *zap.Logger, nbf *types.NomsBinFormat, numCommits int, inputShard AttributionShard, table, shardBasePath string, tableSch schema.Schema, shardParams ProllyAttShardParams, shardStore att.ShardStore) *prollyShardManager {
	_, vd := getAttribDescriptorsFromTblSchema(tableSch)
	attVb := val.NewTupleBuilder(vd)
	return &prollyShardManager{
		logger:        logger,
		nbf:           nbf,
		table:         table,
		inputShard:    inputShard,
		numCommits:    numCommits,
		shardBasePath: shardBasePath,
		kb:            val.NewTupleBuilder(tableSch.GetKeyDescriptor()),
		pool:          pool.NewBuffPool(),
		attVb:         attVb,
		shardStore:    shardStore,
		shardParams:   shardParams,
	}
}

// add an attributed row to the current shard being built
func (sm *prollyShardManager) addRowAtt(ctx context.Context, key val.Tuple, ra prollyRowAtt, raVal val.Tuple) error {
	// if necessary build a val.Tuple
	if raVal == nil {
		raVal = ra.asTuple(sm.attVb, sm.pool)
	}

	for _, currOwner := range ra {
		if currOwner != nil {
			sm.commitCounts[*currOwner]++
		}
	}

	err := sm.mut.Put(ctx, key, raVal)
	if err != nil {
		return err
	}

	return nil
}

// called when all attribution work is done for the input shard
func (sm *prollyShardManager) close(ctx context.Context) (AttributionShard, error) {
	start := time.Now()
	sm.logger.Info("closing and persisting shard")
	defer func() {
		sm.logger.Info("closed shard", zap.Duration("took", time.Since(start)))
	}()

	shard := AttributionShard{
		Table:          sm.table,
		StartInclusive: sm.inputShard.StartInclusive,
		EndExclusive:   sm.inputShard.EndExclusive,
		CommitCounts:   sm.commitCounts,
	}

	m, err := sm.mut.Map(ctx)
	if err != nil {
		return AttributionShard{}, err
	}
	v := shim.ValueFromMap(m)

	vrw := types.NewValueStore(sm.currStore)
	nootNodeRef, err := vrw.WriteValue(ctx, v)
	if err != nil {
		return AttributionShard{}, nil
	}

	// persist to shard store
	path := sm.shardStore.Join(sm.shardBasePath, shard.Key(sm.nbf))
	err = sm.shardStore.WriteShard(ctx, path, sm.currStore, nootNodeRef)
	if err != nil {
		return AttributionShard{}, err
	}
	shard.Path = path

	sm.currStore = nil
	sm.commitCounts = nil

	return shard, nil
}

func (sm *prollyShardManager) openShard(ctx context.Context) error {
	var err error
	sm.currStore, err = valuefile.NewFileValueStore(sm.nbf)
	if err != nil {
		return err
	}
	sm.nodeStore = tree.NewNodeStore(sm.currStore)

	m, err := prolly.NewMapFromTuples(ctx, sm.nodeStore, sm.kb.Desc, sm.attVb.Desc)
	if err != nil {
		return err
	}

	sm.mut = m.Mutate()
	sm.commitCounts = make([]uint64, sm.numCommits)

	return nil
}
