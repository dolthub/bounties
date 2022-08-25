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

package prolly_cellwise

import (
	"context"
	"errors"
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

type prollyAddRowParams struct {
	key   val.Tuple
	raVal val.Tuple
	ra    prollyRowAtt
}

type prollyAttQueue struct {
	attVals []prollyAddRowParams
	head    int
	tail    int
	size    int
	maxSize int
}

func newprollyAttQueue(maxSize int) *prollyAttQueue {
	return &prollyAttQueue{make([]prollyAddRowParams, maxSize), 0, 0, 0, maxSize}
}

func (q *prollyAttQueue) Size() int {
	return q.size
}

func (q *prollyAttQueue) Full() bool {
	return q.size == q.maxSize
}

func (q *prollyAttQueue) Empty() bool {
	return q.size == 0
}

func (q *prollyAttQueue) Push(key, val val.Tuple, ra prollyRowAtt) {
	if q.Full() {
		panic("full")
	}

	q.attVals[q.head] = prollyAddRowParams{key: key, raVal: val, ra: ra}
	q.head = (q.head + 1) % q.maxSize
	q.size++
}

func (q *prollyAttQueue) Pop() *prollyAddRowParams {
	if q.Empty() {
		return nil
	}

	arp := q.attVals[q.tail]
	q.tail = (q.tail + 1) % q.maxSize
	q.size--

	return &arp
}

func (q *prollyAttQueue) PeekKey() val.Tuple {
	if q.Empty() {
		return nil
	}

	return q.attVals[q.tail].key
}

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

	startKey      val.Tuple
	currStore     *valuefile.FileValueStore
	nodeStore     tree.NodeStore
	rowsBufferred int
	aQ            *prollyAttQueue
	mut           prolly.MutableMap
	commitCounts  []uint64

	shards []AttributionShard
	seen   int
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
		aQ:            newprollyAttQueue(shardParams.RowsPerShard),
	}
}

// returns the output shards
func (sm *prollyShardManager) getShards() []AttributionShard {
	return sm.shards
}

// add an attributed row to the current shard being built
func (sm *prollyShardManager) addRowAtt(ctx context.Context, key val.Tuple, ra prollyRowAtt, raVal val.Tuple) error {
	// if necessary encode the row attribution data as a noms value
	if raVal == nil {
		raVal = ra.asTuple(sm.attVb, sm.pool)
	}

	sm.aQ.Push(key, raVal, ra)
	nextKey := sm.aQ.PeekKey()

	// check and shard if beyond the configured number of rows per shard
	if sm.rowsBufferred >= sm.shardParams.RowsPerShard {
		err := sm.closeCurrentShard(ctx, nextKey)
		if err != nil {
			return err
		}
	}

	// open a new shard if not actively writing a shard
	if sm.currStore == nil {
		err := sm.openNewShard(ctx, nextKey)
		if err != nil {
			return err
		}
	}

	toMove := (sm.aQ.Size() - sm.rowsBufferred + 1) / 2
	err := sm.popQueuedRowsToShard(ctx, toMove)
	if err != nil {
		return err
	}

	return nil
}

func (sm *prollyShardManager) popQueuedRowsToShard(ctx context.Context, toMove int) error {
	for i := 0; i < toMove; i++ {
		arp := sm.aQ.Pop()
		err := sm.mut.Put(ctx, arp.key, arp.raVal)
		if err != nil {
			return err
		}

		for _, currOwner := range arp.ra {
			if currOwner != nil {
				sm.commitCounts[*currOwner]++
			}
		}
	}

	sm.rowsBufferred += toMove
	return nil
}

// called when all attribution is done for the input shard
func (sm *prollyShardManager) close(ctx context.Context) error {
	remaining := sm.aQ.Size() + sm.rowsBufferred

	// If the total number of rows between the buffered shard and the queue is greater than the shard cut off then cut the
	// shard now and open a new one which we stream all the queued rows to before closing.  This gives two shards with an
	// equal row count.
	//
	// If what remains is less than the cutoff just stream the queue into the current shard and close it out.
	if remaining > sm.shardParams.RowsPerShard {
		nextKey := sm.aQ.PeekKey()
		err := sm.closeCurrentShard(ctx, nextKey)
		if err != nil {
			return err
		}

		err = sm.openNewShard(ctx, nextKey)
		if err != nil {
			return err
		}
	}

	err := sm.popQueuedRowsToShard(ctx, sm.aQ.Size())
	if err != nil {
		return err
	}

	return sm.closeCurrentShard(ctx, sm.inputShard.EndExclusive)
}

// finalizes an output shard and persists it.
func (sm *prollyShardManager) closeCurrentShard(ctx context.Context, end val.Tuple) error {
	if sm.rowsBufferred > 0 {
		start := time.Now()
		sm.logger.Info("closing and persisting shard")
		defer func() {
			var startK, endK string
			if len(sm.startKey) > 0 {
				startK = sm.kb.Desc.Format(sm.startKey)
			}
			if len(end) > 0 {
				endK = sm.kb.Desc.Format(end)
			}

			sm.logger.Info("closed shard", zap.String("start", startK), zap.String("end", endK), zap.Duration("took", time.Since(start)))
		}()

		// if this is the first output shard, maintain the starting key from the input shard.
		startKey := sm.startKey
		if len(sm.shards) == 0 {
			startKey = sm.inputShard.StartInclusive
		}

		// TODO (dhruv): Should |startKey| and |end| be copied here? There was a
		// comment describing on how the types.Values needed to copied to allow
		// garbage collection on large objects.

		shard := AttributionShard{
			Table:          sm.table,
			StartInclusive: startKey,
			EndExclusive:   end,
			CommitCounts:   sm.commitCounts,
		}

		m, err := sm.mut.Map(ctx)
		if err != nil {
			return err
		}
		v := shim.ValueFromMap(m)

		vrw := types.NewValueStore(sm.currStore)
		nootNodeRef, err := vrw.WriteValue(ctx, v)
		if err != nil {
			return nil
		}

		// persist to shard store
		path := sm.shardStore.Join(sm.shardBasePath, shard.Key(sm.nbf))
		err = sm.shardStore.WriteShard(ctx, path, sm.currStore, nootNodeRef)
		if err != nil {
			return err
		}
		shard.Path = path

		// resulting metadata on the output shard
		sm.shards = append(sm.shards, shard)
		sm.startKey = nil
		sm.currStore = nil
		sm.rowsBufferred = 0
		sm.commitCounts = nil
	}

	return nil
}

// create a new chunk store and streaming map in order to be able to stream row attribution values to the map containing
// sharded attribution ddata
func (sm *prollyShardManager) openNewShard(ctx context.Context, startKey val.Tuple) error {
	if sm.rowsBufferred != 0 {
		return errors.New("opening new shard while previous shard not closed")
	}

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
	sm.startKey = startKey
	sm.commitCounts = make([]uint64, sm.numCommits)

	return nil
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
