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
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"go.uber.org/zap"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils/differs"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	diff2 "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

// CWAttShardParams control the dynamic sharding behavior
type CWAttShardParams struct {
	// RowsPerShard define the count at which point a shard is cut and a new one starts
	RowsPerShard int
	// MinShardSize uint64 need to implement

	SubdivideDiffsSize int64
}

var _ att.AttributionMethod = CWAttribution{}

// CWAttribution implements att.AttributionMethod and provides cellwise attribution
type CWAttribution struct {
	ddb         *doltdb.DoltDB
	logger      *zap.Logger
	startHash   hash.Hash
	shardParams CWAttShardParams
	shardStore  att.ShardStore
}

// NewCWAtt returns a new CWAttribution object
func NewCWAtt(logger *zap.Logger, ddb *doltdb.DoltDB, startHash hash.Hash, shardStore att.ShardStore, params CWAttShardParams) CWAttribution {
	return CWAttribution{
		logger:      logger,
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

// DeserializeResults takes a []byte and deserializes it ta a ShardResult
func (cwa CWAttribution) DeserializeResults(ctx context.Context, data []byte) (att.ShardResult, error) {
	buf := bytes.NewBuffer(data)
	vf, err := valuefile.ReadFromReader(ctx, buf)
	if err != nil {
		return AttributionShard{}, err
	} else if len(vf.Values) != 1 {
		return AttributionShard{}, errors.New("corrupt shard info")
	}

	var results []AttributionShard
	err = marshal.Unmarshal(ctx, cwa.ddb.Format(), vf.Values[0], &results)
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
	memShard, err := cwa.shardStore.ReadShard(ctx, key)
	if err != nil {
		return nil, err
	}

	var summary CellwiseAttSummary
	err = marshal.Unmarshal(ctx, cwa.ddb.Format(), memShard.Value, &summary)
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
	root, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	var prevRoot *doltdb.RootValue
	if prevCommit != nil {
		prevRoot, err = prevCommit.GetRootValue(ctx)
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
	if jsonData, err := json.Marshal(summary); err == nil {
		cwa.logger.Info("collecting shards", zap.String("summary", string(jsonData)))
	}

	tables, err := doltutils.GetScoredTables(ctx, summary.tableNames(), root)
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
				subDivided, err := cwa.subdivideShard(ctx, shards[i], table, root, prevRoot)
				if err != nil {
					return nil, err
				}

				allShards = append(allShards, subDivided...)
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
			if err != nil {
				return nil, err
			}

			_, differ, err := getDiffer(ctx, shard, tbl, prevTbl)
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

// ProcessShard processes a single shard
func (cwa CWAttribution) ProcessShard(ctx context.Context, commitIdx int16, cm, prevCm *doltdb.Commit, shardInfo att.ShardInfo) (att.ShardResult, error) {
	shardKey := shardInfo.Key(cwa.ddb.Format())

	if infoJson, err := json.Marshal(shardInfo); err == nil {
		start := time.Now()
		cwa.logger.Info("Processing Shard Start", zap.String("shard_key", shardKey), zap.String("shard_info", string(infoJson)))
		defer func() {
			cwa.logger.Info("Processing Shard End", zap.String("shard_key", shardKey), zap.String("shard_info", string(infoJson)), zap.Duration("took", time.Since(start)))
		}()
	}

	commitHash, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	var prevRoot *doltdb.RootValue
	if prevCm != nil {
		prevRoot, err = prevCm.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}
	}

	if unchangedShard, ok := shardInfo.(UnchangedShard); ok {
		cwa.logger.Info("Shard has no changes", zap.String("shard_key", shardKey))
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

	sch, differ, err := getDiffer(ctx, shard, tbl, prevTbl)
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

	basePath := commitHash.String()
	shardMgr := NewShardManager(cwa.logger, nbf, int(commitIdx)+1, shard, shard.Table, basePath, cwa.shardParams, cwa.shardStore)
	err = cwa.attributeDiffs(ctx, commitIdx, shardMgr, shard, sch, attribData, differ)
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
	start := time.Now()
	shardKey := shard.Key(cwa.ddb.Format())
	cwa.logger.Info("Reading shard", zap.String("shard_key", shardKey))
	defer func() {
		cwa.logger.Info("Reading shard", zap.String("shard_key", shardKey), zap.Duration("took", time.Since(start)))
	}()

	memShard, err := cwa.shardStore.ReadShard(ctx, shard.Path)
	if err != nil {
		return types.Map{}, err
	}

	attribData, ok := memShard.Value.(types.Map)
	if !ok {
		return types.Map{}, valuefile.ErrCorruptNVF // Not the type of value we expect
	}

	return attribData, nil
}

func getRangeSize(ctx context.Context, rowData types.Map, startInc, endExc types.Value) (int64, error) {
	if rowData.Empty() {
		return 0, nil
	}

	var startIdx int64
	var endIdx int64
	var err error

	if !types.IsNull(startInc) {
		startIdx, err = rowData.IndexForKey(ctx, startInc)
		if err != nil {
			return 0, err
		}
	} else {
		startIdx = 0
	}

	if !types.IsNull(endExc) {
		endIdx, err = rowData.IndexForKey(ctx, endExc)
		if err != nil {
			return 0, err
		}
	} else {
		endIdx = int64(rowData.Len())
	}

	return endIdx - startIdx, nil
}

// getDiffer returns a differs.Differ implementation which encapsulates all the changes.
func getDiffer(ctx context.Context, shard AttributionShard, tbl, prevTbl *doltdb.Table) (schema.Schema, differs.Differ, error) {
	if prevTbl == nil && tbl == nil {
		panic("how")
	}

	var err error
	var prevSch schema.Schema
	var prevRowData types.Map
	var sch schema.Schema
	var rowData types.Map
	var format *types.NomsBinFormat

	if tbl != nil {
		rowData, err = tbl.GetNomsRowData(ctx)
		if err != nil {
			return nil, nil, err
		}

		sch, err = tbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}

		format = rowData.Format()
	}

	if prevTbl != nil {
		prevRowData, err = prevTbl.GetNomsRowData(ctx)
		if err != nil {
			return nil, nil, err
		}

		prevSch, err = prevTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}

		format = rowData.Format()
	}

	if prevTbl == nil {
		return sch, differs.NewMapRowsAsDiffs(ctx, tbl.ValueReadWriter(), sch, rowData.Format(), types.DiffChangeAdded, rowData, shard.StartInclusive, shard.EndExclusive), nil
	} else if tbl == nil {
		return nil, differs.NewMapRowsAsDiffs(ctx, prevTbl.ValueReadWriter(), prevSch, prevRowData.Format(), types.DiffChangeRemoved, prevRowData, shard.StartInclusive, shard.EndExclusive), nil
	}

	eqSchemas := schema.SchemasAreEqual(sch, prevSch)
	inRangeFunc := shard.inRangeFunc(ctx, format)

	if eqSchemas {
		differ := diff.NewAsyncDiffer(32)
		differ.StartWithRange(ctx, prevRowData, rowData, shard.StartInclusive, inRangeFunc)
		return sch, differ, nil
	} else {
		differ := &differs.DualMapIter{}
		differ.Start(ctx, prevTbl.ValueReadWriter(), tbl.ValueReadWriter(), prevSch, sch, prevRowData, rowData, shard.StartInclusive, inRangeFunc)
		return sch, differ, nil
	}
}

// attributeDiffs loops over the previous attribution and the diffs updating the attribution and sending it to the shard
// manager to be persisted.
func (cwa CWAttribution) attributeDiffs(ctx context.Context, commitIdx int16, shardMgr *shardManager, shard AttributionShard, sch schema.Schema, attribData *types.Map, differ differs.Differ) error {
	var attItr types.MapTupleIterator
	var err error

	// get an iterator for iterating over the existing attribution for the shard.  If there is no attribution gets an empty iterator
	if attribData != nil {
		var rangeStart int64
		rangeEnd := int64(attribData.Len())

		if !types.IsNull(shard.StartInclusive) {
			rangeStart, err = attribData.IndexForKey(ctx, shard.StartInclusive)
			if err != nil {
				return err
			}
		}

		if !types.IsNull(shard.EndExclusive) {
			rangeEnd, err = attribData.IndexForKey(ctx, shard.EndExclusive)
		}

		attItr, err = attribData.RangeIterator(ctx, uint64(rangeStart), uint64(rangeEnd))

		if err != nil {
			return err
		}
	} else {
		attItr = types.EmptyMapIterator{}
	}

	nbf := cwa.ddb.Format()

	var newData int
	var unchangedData int
	var modifiedData int
	var total int
	incAndLog := func(p *int) {
		if p != nil {
			total++
			*p = *p + 1
		}

		if p == nil || total%50_000 == 0 {
			cwa.logger.Info("attribution update", zap.Int("new", newData), zap.Int("unchanged", unchangedData), zap.Int("modified", modifiedData))
		}
	}
	defer incAndLog(nil)

	var attKey types.Value
	var attVal types.Value
	var diffs []*diff2.Difference
	for {
		if attKey == nil {
			// gets existing attribution data for the next previously attributed row
			attKeyTpl, attValTpl, err := attItr.NextTuple(ctx)

			if err != nil {
				if err != io.EOF {
					return err
				}
				// in the case of io.EOF attKey and attVal are not set
			} else {
				attKey = attKeyTpl
				attVal = attValTpl
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
			incAndLog(&newData)

			err = cwa.processDiffWithNoPrevAtt(ctx, shardMgr, sch, commitIdx, diffs[0])
			diffs = nil
		} else if len(diffs) == 0 { // have attribution data but no diff.  attribution should stay untouched
			incAndLog(&unchangedData)

			err = cwa.processUnchangedAttribution(ctx, shardMgr, attKey, attVal)
			attKey = nil
			attVal = nil
		} else {
			// have an existing attributed row, and a row that has changed
			diffKey := diffs[0].KeyValue
			isLess, err := attKey.Less(ctx, nbf, diffKey)

			if err != nil {
				return err
			}

			if isLess {
				incAndLog(&unchangedData)
				// the attributed row comes before the changed row.  attribution stays untouched
				err = cwa.processUnchangedAttribution(ctx, shardMgr, attKey, attVal)
				attKey = nil
				attVal = nil
			} else if attKey.Equals(diffKey) {
				incAndLog(&modifiedData)
				// existing attributed row matches the row that has changed.  Update attribution
				err = cwa.updateAttFromDiff(ctx, shardMgr, sch, commitIdx, attKey, attVal, diffs[0])
				attKey = nil
				attVal = nil
				diffs = nil
			} else {
				incAndLog(&newData)
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

func getRowData(ctx context.Context, table string, root *doltdb.RootValue) (types.Map, error) {
	tbl, ok, err := root.GetTable(ctx, table)

	if err != nil {
		return types.Map{}, err
	} else if !ok {
		return types.Map{}, doltdb.ErrTableNotFound
	}

	return tbl.GetNomsRowData(ctx)
}

func (cwa CWAttribution) subdivideShard(ctx context.Context, shard AttributionShard, table string, root *doltdb.RootValue, prevRoot *doltdb.RootValue) ([]att.ShardInfo, error) {
	if cwa.shardParams.SubdivideDiffsSize <= 0 {
		return []att.ShardInfo{shard}, nil
	}

	rowData, err := getRowData(ctx, table, root)
	if err != nil {
		return nil, err
	}

	prevRowData, err := getRowData(ctx, table, prevRoot)
	if err != nil {
		return nil, err
	}

	size, err := getRangeSize(ctx, rowData, shard.StartInclusive, shard.EndExclusive)
	if err != nil {
		return nil, err
	}

	prevSize, err := getRangeSize(ctx, prevRowData, shard.StartInclusive, shard.EndExclusive)
	if err != nil {
		return nil, err
	}

	delta := size - prevSize
	if delta < 0 {
		delta = -delta
	}

	if delta <= cwa.shardParams.SubdivideDiffsSize {
		return []att.ShardInfo{shard}, nil
	} else {
		var subDivisions []att.ShardInfo
		subDivideRows := rowData

		if prevSize > size {
			subDivideRows = prevRowData
		}

		numSubs := (delta / cwa.shardParams.SubdivideDiffsSize) + 1
		subDivisionStep := delta / numSubs

		var startIdx int64
		if !types.IsNull(shard.StartInclusive) {
			startIdx, err = subDivideRows.IndexForKey(ctx, shard.StartInclusive)
			if err != nil {
				return nil, err
			}
		}

		start := shard.StartInclusive
		var subdivisionKeys []string
		for i := int64(0); i < numSubs-1; i++ {
			k, _, err := subDivideRows.At(ctx, uint64(startIdx+subDivisionStep))
			if err != nil {
				return nil, err
			}

			newSub := AttributionShard{
				Table:          shard.Table,
				Path:           shard.Path,
				StartInclusive: start,
				EndExclusive:   k,
			}
			subDivisions = append(subDivisions, newSub)
			subdivisionKeys = append(subdivisionKeys, newSub.Key(cwa.ddb.Format()))

			start = k
			startIdx += subDivisionStep
		}

		lastSub := AttributionShard{
			Table:          shard.Table,
			Path:           shard.Path,
			StartInclusive: start,
			EndExclusive:   shard.EndExclusive,
		}
		subDivisions = append(subDivisions, lastSub)
		subdivisionKeys = append(subdivisionKeys, lastSub.Key(cwa.ddb.Format()))

		cwa.logger.Info("Subdividing Shard", zap.String("shard_key", shard.Key(cwa.ddb.Format())), zap.Int64("num_subdivisions", numSubs), zap.Int64("rowdata_range_size_delta", delta), zap.Strings("subdivisions", subdivisionKeys))
		return subDivisions, nil
	}
}
