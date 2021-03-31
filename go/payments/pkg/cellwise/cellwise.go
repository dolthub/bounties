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
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils/differs"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	diff2 "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

// CWAttShardParams control the dynamic sharding behavior
type CWAttShardParams struct {
	// RowsPerShard define the count at which point a shard is cut and a new one starts
	RowsPerShard uint64
	// MinShardSize uint64 need to implement
}

var _ att.AttributionMethod = CWAttribution{}

// CWAttribution implements att.AttributionMethod and provides cellwise attribution
type CWAttribution struct {
	ddb         *doltdb.DoltDB
	startHash   hash.Hash
	shardParams CWAttShardParams
	shardStore  att.ShardStore
}

// NewCWAtt returns a new CWAttribution object
func NewCWAtt(ddb *doltdb.DoltDB, startHash hash.Hash, shardStore att.ShardStore, params CWAttShardParams) CWAttribution {
	return CWAttribution{
		ddb:         ddb,
		startHash:   startHash,
		shardStore:  shardStore,
		shardParams: params,
	}
}

// EmptySummary returns an empty CellwiseAttSummary object
func (cwa CWAttribution) EmptySummary(ctx context.Context) att.Summary {
	return emptySummary(cwa.startHash)
}

// SerializeResults takes a ShardResult object and serializes it
func (cwa CWAttribution) SerializeResults(ctx context.Context, results att.ShardResult) ([]byte, error) {
	attShards, ok := results.([]AttributionShard)
	if !ok {
		return nil, errors.New("unexpected result type")
	}

	store, err := valuefile.NewFileValueStore(cwa.ddb.Format())
	if err != nil {
		return nil, err
	}

	val, err := marshal.Marshal(ctx, store, attShards)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = valuefile.WriteToWriter(ctx, &buf, store, val)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeResults takes a []bite and deserializes it ta a ShardResult
func (cwa CWAttribution) DeserializeResults(ctx context.Context, data []byte) (att.ShardResult, error) {
	buf := bytes.NewBuffer(data)
	vals, err := valuefile.ReadFromReader(ctx, buf)
	if err != nil {
		return AttributionShard{}, err
	} else if len(vals) != 1 {
		return AttributionShard{}, errors.New("corrupt shard info")
	}

	var results []AttributionShard
	err = marshal.Unmarshal(ctx, cwa.ddb.Format(), vals[0], &results)
	if err != nil {
		return nil, err
	}

	for i := range results {
		if _, ok := results[i].StartInclusive.(types.Tuple); !ok {
			results[i].StartInclusive = types.NullValue
		}

		if _, ok := results[i].EndExclusive.(types.Tuple); !ok {
			results[i].EndExclusive = types.NullValue
		}
	}

	return results, nil
}

// SerializeShardInfo takes a ShardInfo object and serializes it
func (cwa CWAttribution) SerializeShardInfo(ctx context.Context, info att.ShardInfo) ([]byte, error) {
	var buf bytes.Buffer

	unchanged, ok := info.(UnchangedShard)

	if ok {
		buf.WriteByte(0)
		err := unchanged.serialize(ctx, cwa.ddb.Format(), &buf)
		if err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	cellwiseShard, ok := info.(AttributionShard)

	if ok {
		buf.WriteByte(1)
		err := cellwiseShard.serialize(ctx, cwa.ddb.Format(), &buf)
		if err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	return nil, errors.New("Unsupported shard type for cellwise attribution")
}

// DeserializeShardInfo takes a []byte and deserializes it to a ShardInfo object
func (cwa CWAttribution) DeserializeShardInfo(ctx context.Context, data []byte) (att.ShardInfo, error) {
	buf := bytes.NewBuffer(data)
	shardType, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	if shardType < 0 || shardType > 1 {
		return nil, errors.New("corrupt shard info")
	}

	shardInfo, err := deserializeShard(ctx, cwa.ddb.Format(), buf)
	if err != nil {
		return nil, err
	}

	if shardType == 0 {
		return UnchangedShard{AttributionShard: shardInfo}, nil
	} else {
		return shardInfo, nil
	}
}

// WriteSummary persists a summary
func (cwa CWAttribution) WriteSummary(ctx context.Context, summary att.Summary) (string, error) {
	cws := summary.(CellwiseAttSummary)
	commitHash := cws.CommitHashes[cws.NumCommits()-1]
	commitHashStr := commitHash.String()

	store, err := valuefile.NewFileValueStore(cwa.ddb.Format())
	if err != nil {
		return "", err
	}

	v, err := marshal.Marshal(ctx, store, summary)
	if err != nil {
		return "", err
	}

	_, err = store.WriteValue(ctx, v)
	if err != nil {
		return "", err
	}

	key := commitHashStr + ".summary"
	err = cwa.shardStore.WriteShard(ctx, key, store, v)
	if err != nil {
		return "", err
	}

	return key, err
}

// ReadSummary reads a summary for a commit hash
func (cwa CWAttribution) ReadSummary(ctx context.Context, key string) (att.Summary, error) {
	val, err := cwa.shardStore.ReadShard(ctx, key)
	if err != nil {
		return nil, err
	}

	var summary CellwiseAttSummary
	err = marshal.Unmarshal(ctx, cwa.ddb.Format(), val, &summary)
	if err != nil {
		return nil, err
	}

	// total hack.  need to look at noms serialization.  It's encoding types.NullValue as a types.Float with value 0.
	// When I tried encoding a value of nil it panicked.
	for tableName := range summary.TableShards {
		for i := 0; i < len(summary.TableShards[tableName]); i++ {
			if _, ok := summary.TableShards[tableName][i].StartInclusive.(types.Tuple); !ok {
				summary.TableShards[tableName][i].StartInclusive = types.NullValue
			}

			if _, ok := summary.TableShards[tableName][i].EndExclusive.(types.Tuple); !ok {
				summary.TableShards[tableName][i].EndExclusive = types.NullValue
			}
		}
	}

	return summary, nil
}

// CollectShards gathers all the shards that need to be processed
func (cwa CWAttribution) CollectShards(ctx context.Context, commit, prevCommit *doltdb.Commit, summary att.Summary) ([]att.ShardInfo, error) {
	root, err := commit.GetRootValue()
	if err != nil {
		return nil, err
	}

	var prevRoot *doltdb.RootValue
	if prevCommit != nil {
		prevRoot, err = prevCommit.GetRootValue()
		if err != nil {
			return nil, err
		}
	}

	var cws CellwiseAttSummary
	if summary == nil {
		cws = emptySummary(cwa.startHash)
	} else {
		cws = summary.(CellwiseAttSummary)
	}

	return cwa.collectShards(ctx, cws, root, prevRoot)
}

func (cwa CWAttribution) collectShards(ctx context.Context, summary CellwiseAttSummary, root, prevRoot *doltdb.RootValue) ([]att.ShardInfo, error) {
	tables, err := cwa.getScoredTables(ctx, summary, root)

	if err != nil {
		return nil, err
	}

	allShards := make([]att.ShardInfo, 0, len(tables)*16)

	// loop over the tables and look at the shards from the previous summary
	for _, table := range tables {
		shards, ok := summary.TableShards[table]

		if !ok {
			allShards = append(allShards, AttributionShard{Table: table, StartInclusive: types.NullValue, EndExclusive: types.NullValue})
			continue
		}

		// gets a []bool which tells which shards have differences that need to be attributed, and which do not
		hasDiffs, err := cwa.shardsHaveDiffs(ctx, shards, table, root, prevRoot)
		if err != nil {
			return nil, err
		}

		// append to allShards creating a special UnchangedShard object for shards that have not changed
		for i := range shards {
			if hasDiffs[i] {
				allShards = append(allShards, shards[i])
			} else {
				allShards = append(allShards, UnchangedShard{shards[i]})
			}
		}
	}

	return allShards, nil
}

// looks at whether the table has changed.  If it has changed, it then looks to see if their are diffs that touch the
// corresponding shards
func (cwa CWAttribution) shardsHaveDiffs(ctx context.Context, shards []AttributionShard, table string, root, prevRoot *doltdb.RootValue) ([]bool, error) {
	var tblHash hash.Hash
	var prevTblHash hash.Hash

	tbl, ok, err := root.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}

	if ok {
		tblHash, err = tbl.HashOf()
		if err != nil {
			return nil, err
		}
	}

	prevTbl, ok, err := prevRoot.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}

	if ok {
		prevTblHash, err = prevTbl.HashOf()
		if err != nil {
			return nil, err
		}
	}

	hasDiffs := make([]bool, len(shards))
	if !tblHash.Equal(prevTblHash) {
		for i, shard := range shards {
			_, differ, err := cwa.getDiffer(ctx, shard, tbl, prevTbl)
			if err != nil {
				return nil, err
			}

			diffs, _, err := differ.GetDiffs(1, -1)
			if err != nil {
				return nil, err
			}

			hasDiffs[i] = len(diffs) != 0
		}
	}

	return hasDiffs, err
}

// gets the lists of tables that are scored.  This filters out any tables with the prefix dolt_
func (cwa CWAttribution) getScoredTables(ctx context.Context, summary CellwiseAttSummary, root *doltdb.RootValue) ([]string, error) {
	tableNames, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	unique := set.NewStrSet(tableNames)
	summaryTableNames := summary.tableNames()
	unique.Add(summaryTableNames...)

	scoredTables := make([]string, 0, unique.Size())
	unique.Iterate(func(s string) (cont bool) {
		if !strings.HasPrefix(strings.ToLower(s), "dolt_") {
			scoredTables = append(scoredTables, s)
		}

		return true
	})

	return scoredTables, nil
}

// ProcessShard processes a single shard
func (cwa CWAttribution) ProcessShard(ctx context.Context, commitIdx int16, cm, prevCm *doltdb.Commit, shardInfo att.ShardInfo) (att.ShardResult, error) {
	commitHash, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return nil, err
	}

	var prevRoot *doltdb.RootValue
	if prevCm != nil {
		prevRoot, err = prevCm.GetRootValue()
		if err != nil {
			return nil, err
		}
	}

	if unchangedShard, ok := shardInfo.(UnchangedShard); ok {
		return processUnchangedShard(ctx, unchangedShard.AttributionShard)
	}

	shard := shardInfo.(AttributionShard)
	tableName := shard.Table
	tbl, _, err := root.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	var prevTbl *doltdb.Table
	if prevRoot != nil {
		prevTbl, _, err = prevRoot.GetTable(ctx, tableName)

		if err != nil {
			return nil, err
		}
	}

	nbf := cwa.ddb.Format()
	tableShardPath := cwa.shardStore.Join(commitHash.String(), tableName)

	sch, differ, err := cwa.getDiffer(ctx, shard, tbl, prevTbl)
	if err != nil {
		return nil, err
	}

	var attribData *types.Map
	if len(shard.Path) > 0 {
		ad, err := cwa.readShardFile(ctx, shard)
		if err != nil {
			return nil, err
		}
		attribData = &ad
	}

	shardMgr := NewShardManager(nbf, int(commitIdx)+1, shard, shard.Table, tableShardPath, cwa.shardParams, cwa.shardStore)
	err = cwa.attributeDiffs(ctx, commitIdx, shardMgr, sch, attribData, differ)
	if err != nil && err != io.EOF {
		return nil, err
	}

	err = shardMgr.close(ctx)
	if err != nil {
		return nil, err
	}

	return shardMgr.getShards(), nil
}

func processUnchangedShard(ctx context.Context, shard AttributionShard) (att.ShardResult, error) {
	// updated counts are the same as previous, but have an extra 0 for this commit
	updatedCounts := make([]uint64, len(shard.CommitCounts)+1)
	copy(updatedCounts, shard.CommitCounts)

	return []AttributionShard{{
		Table:          shard.Table,
		StartInclusive: shard.StartInclusive,
		EndExclusive:   shard.EndExclusive,
		Path:           shard.Path,
		CommitCounts:   updatedCounts,
	}}, nil
}

// ProcessResults takes all the results from processing all the shards and returns a summary
func (cwa CWAttribution) ProcessResults(ctx context.Context, commitHash hash.Hash, prevSummary att.Summary, results []att.ShardResult) (att.Summary, error) {
	// nil summary not allowed.  pass emptySummary when there is no prev summary
	ps := prevSummary.(CellwiseAttSummary)

	commitIdx := len(ps.CommitHashes)
	commitCounts := make([]uint64, commitIdx+1)
	commitHashes := make([]hash.Hash, commitIdx+1)

	copy(commitHashes, ps.CommitHashes)
	commitHashes[commitIdx] = commitHash

	tableShards := make(map[string][]AttributionShard)
	for _, shard := range results {
		cwShards := shard.([]AttributionShard)
		for _, cwShard := range cwShards {
			tableShards[cwShard.Table] = append(tableShards[cwShard.Table], cwShard)
			for idx, count := range cwShard.CommitCounts {
				commitCounts[idx] += count
			}
		}
	}

	return CellwiseAttSummary{
		StartHash:    ps.StartHash,
		CommitHashes: commitHashes,
		CommitCounts: commitCounts,
		TableShards:  tableShards,
	}, nil
}

func (cwa CWAttribution) readShardFile(ctx context.Context, shard AttributionShard) (types.Map, error) {
	val, err := cwa.shardStore.ReadShard(ctx, shard.Path)
	if err != nil {
		return types.Map{}, err
	}

	attribData, ok := val.(types.Map)
	if !ok {
		return types.Map{}, valuefile.ErrCorruptNVF // Not the type of value we expect
	}

	return attribData, nil
}

// getDiffer returns a differs.Differ implementation which encapsulates all the changes.
func (cwa CWAttribution) getDiffer(ctx context.Context, shard AttributionShard, tbl, prevTbl *doltdb.Table) (schema.Schema, differs.Differ, error) {
	if prevTbl == nil && tbl == nil {
		panic("how")
	}

	var err error
	var prevSch schema.Schema
	var prevRowData types.Map
	var sch schema.Schema
	var rowData types.Map

	if tbl != nil {
		rowData, err = tbl.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}

		sch, err = tbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if prevTbl != nil {
		prevRowData, err = prevTbl.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}

		prevSch, err = prevTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if prevTbl == nil {
		return sch, differs.NewMapRowsAsDiffs(ctx, cwa.ddb.Format(), types.DiffChangeAdded, rowData, shard.StartInclusive, shard.EndExclusive), nil
	} else if tbl == nil {
		return nil, differs.NewMapRowsAsDiffs(ctx, cwa.ddb.Format(), types.DiffChangeRemoved, prevRowData, shard.StartInclusive, shard.EndExclusive), nil
	}

	eqSchemas := schema.SchemasAreEqual(sch, prevSch)
	inRangeFunc := shard.inRangeFunc(cwa.ddb.Format())

	if eqSchemas {
		differ := diff.NewAsyncDiffer(32)
		differ.StartWithRange(ctx, prevRowData, rowData, shard.StartInclusive, inRangeFunc)
		return sch, differ, nil
	} else {
		differ := &differs.DualMapIter{}
		differ.Start(ctx, prevRowData, rowData, shard.StartInclusive, inRangeFunc)
		return sch, differ, nil
	}
}

// attributeDiffs loops over the previous attribution and the diffs updating the attribution and sending it to the shard
// manager to be persisted.
func (cwa CWAttribution) attributeDiffs(ctx context.Context, commitIdx int16, shardMgr *shardManager, sch schema.Schema, attribData *types.Map, differ differs.Differ) error {
	var attItr types.MapIterator
	var err error

	// get an iterator for iterating over the existing attribution for the shard.  If there is no attribution gets an empty iterator
	if attribData != nil {
		attItr, err = attribData.Iterator(ctx)

		if err != nil {
			return err
		}
	} else {
		attItr = types.EmptyMapIterator{}
	}

	nbf := cwa.ddb.Format()

	var attKey types.Value
	var attVal types.Value
	var diffs []*diff2.Difference
	for {
		if attKey == nil {
			// gets existing attribution data for the next previously attributed row
			attKey, attVal, err = attItr.Next(ctx)

			if err != nil && err != io.EOF {
				return err
			}
		}

		if diffs == nil {
			// get diff data for the next row that has changed
			diffs, _, err = differ.GetDiffs(1, -1)

			if err != nil && err != io.EOF {
				return err
			}
		}

		// if there is no more attribution data, and no more diffs to be processed then exit
		if attKey == nil && len(diffs) == 0 {
			break
		}

		if attKey == nil { // no existing attribution data.  This is a new row
			err = cwa.processDiffWithNoPrevAtt(ctx, shardMgr, sch, commitIdx, diffs[0])
			diffs = nil
		} else if len(diffs) == 0 { // have attribution data but no diff.  attribution should stay untouched
			err = cwa.processUnchangedAttribution(ctx, shardMgr, attKey, attVal)
			attKey = nil
			attVal = nil
		} else {
			// have an existing attributed row, and a row that has changed
			diffKey := diffs[0].KeyValue
			isLess, err := attKey.Less(nbf, diffKey)

			if err != nil {
				return err
			}

			if isLess {
				// the attributed row comes before the changed row.  attribution stays untouched
				err = cwa.processUnchangedAttribution(ctx, shardMgr, attKey, attVal)
				attKey = nil
				attVal = nil
			} else if attKey.Equals(diffKey) {
				// existing attributed row matches the row that has changed.  Update attribution
				err = cwa.updateAttFromDiff(ctx, shardMgr, sch, commitIdx, attKey, attVal, diffs[0])
				attKey = nil
				attVal = nil
				diffs = nil
			} else {
				// the changed row is less than the attributed row.  This row is new.  add new attribution
				err = cwa.processDiffWithNoPrevAtt(ctx, shardMgr, sch, commitIdx, diffs[0])
				diffs = nil
			}
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (cwa CWAttribution) processDiffWithNoPrevAtt(ctx context.Context, shardMgr *shardManager, sch schema.Schema, commitIdx int16, difference *diff2.Difference) error {
	ra := newRowAtt()
	err := ra.updateFromDiff(cwa.ddb.Format(), sch, commitIdx, difference)

	if err != nil {
		return err
	}

	return shardMgr.addRowAtt(ctx, difference.KeyValue, ra, nil)
}

func (cwa CWAttribution) processUnchangedAttribution(ctx context.Context, shardMgr *shardManager, key types.Value, val types.Value) error {
	ra, err := rowAttFromValue(val)
	if err != nil {
		return err
	}

	return shardMgr.addRowAtt(ctx, key, ra, val)
}

func (cwa CWAttribution) updateAttFromDiff(ctx context.Context, shardMgr *shardManager, sch schema.Schema, commitIdx int16, key types.Value, val types.Value, difference *diff2.Difference) error {
	ra, err := rowAttFromValue(val)
	if err != nil {
		return err
	}

	err = ra.updateFromDiff(cwa.ddb.Format(), sch, commitIdx, difference)

	if err != nil {
		return err
	}

	return shardMgr.addRowAtt(ctx, key, ra, nil)
}
