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
	"strings"

	"github.com/dolthub/bounties/go/payments/pkg/att"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

// shardManager manages writing of output shards
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

// NewShardManager takes an input shard, a ShardStore, some ShardParams and some other metadata and returns a shardManager
// which is used to manage dynamic sharding, and persisting of shard data
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

// returns the output shards
func (sm *shardManager) getShards() []AttributionShard {
	return sm.shards
}

// add an attributed row to the current shard being built
func (sm *shardManager) addRowAtt(ctx context.Context, key types.Value, ra rowAtt, raVal types.Value) error {
	// check and shard if beyond the configured number of rows per shard
	if sm.rowsInCurrShard >= sm.shardParams.RowsPerShard {
		err := sm.closeCurrentShard(ctx, key)
		if err != nil {
			return err
		}
	}

	// open a new shard if not actively writing a shard
	if sm.currStore == nil {
		err := sm.openNewShard(ctx, key)
		if err != nil {
			return err
		}
	}

	// if necessary encode the row attribution data as a noms value
	if raVal == nil {
		var err error
		raVal, err = ra.AsValue(sm.nbf, sm.rowAttBuff)
		if err != nil {
			return err
		}
	}

	// stream in key and value (keys are sorted, and streamMapCh must receive key, value, key, value in interleaved manner)
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

// called when all attribution is done for the input shard
func (sm *shardManager) close(ctx context.Context) error {
	return sm.closeCurrentShard(ctx, sm.inputShard.EndExclusive)
}

// finalizes an output shard and persists it.
func (sm *shardManager) closeCurrentShard(ctx context.Context, end types.Value) error {
	if sm.rowsInCurrShard > 0 {
		// close the streaming map and get the types.Map which we will persist
		close(sm.streamMapCh)
		m, err := sm.outMap.Wait()
		if err != nil {
			return err
		}

		_, err = sm.currStore.WriteValue(ctx, m)
		if err != nil {
			return err
		}

		// if this is the first output shard, maintain the starting key from the input shard.
		startKey := sm.startKey
		if len(sm.shards) == 0 {
			startKey = sm.inputShard.StartInclusive
		}

		// Create copies of start and end key tuples so larger objects are able to be cleaned up.
		if !types.IsNull(startKey) {
			startKey = startKey.(types.Tuple).CopyOf(nil)
		}

		if !types.IsNull(end) {
			end = end.(types.Tuple).CopyOf(nil)
		}

		// persist to shard store
		path := sm.shardStore.Join(sm.shardBasePath, shardName(sm.nbf, startKey, end))
		err = sm.shardStore.WriteShard(ctx, path, sm.currStore, m)
		if err != nil {
			return err
		}

		// resulting metadata on the output shard
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

// create a new chunk store and streaming map in order to be able to stream row attribution values to the map containing
// sharded attribution ddata
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

// shardName returns a string based on the start and end values for the shard the format being <start-hash>_<end-hasd>.
// if the start or end keys are nil "" will be used in their place.  A shard named "_" will be the full range of values,
// "b03dtu0alc0piqlu8s5q7dibmt4kdn8_" would be a shard staring from the key that has hash b03dtu0alc0piqlu8s5q7dibmt4kdn8
// and all the rows that follow. "_b03dtu0alc0piqlu8s5q7dibmt4kdn8" would we all keys coming before the key with hash
// b03dtu0alc0piqlu8s5q7dibmt4kdn8.  "24pvcimjhuolbnr801nn1q8bq1f91u9j_s72v43bh8kiopalsldg1okgh96gpberv" would be all values
// between the keys with hashes 24pvcimjhuolbnr801nn1q8bq1f91u9j and s72v43bh8kiopalsldg1okgh96gpberv.
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
