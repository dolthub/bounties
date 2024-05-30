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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/dolt/go/store/valuefile"
	"go.uber.org/zap"
)

// ProllyAttShardParams control the dynamic sharding behavior
type ProllyAttShardParams struct {
	// MaximumShardCardinality controls the maximum cardinality of a shard's key
	// range.
	MaximumShardCardinality int
}

// Method implements att.AttributionMethod
type Method struct {
	ddb         *doltdb.DoltDB
	logger      *zap.Logger
	shardParams ProllyAttShardParams
	startHash   hash.Hash
	shardStore  att.ShardStore
}

var _ att.AttributionMethod = (*Method)(nil)

// NewMethod returns a new Method object
func NewMethod(logger *zap.Logger, ddb *doltdb.DoltDB, startHash hash.Hash, shardStore att.ShardStore, params ProllyAttShardParams) Method {
	return Method{
		logger:      logger,
		ddb:         ddb,
		startHash:   startHash,
		shardStore:  shardStore,
		shardParams: params,
	}
}

func (m Method) EmptySummary(ctx context.Context) att.Summary {
	return emptySummary(m.startHash)
}

func (m Method) ReadSummary(ctx context.Context, key string) (att.Summary, error) {
	memShard, err := m.shardStore.ReadShard(ctx, key)
	if err != nil {
		return nil, err
	}

	var summary ProllyAttSummary
	err = marshal.Unmarshal(ctx, m.ddb.Format(), memShard.Value, &summary)
	if err != nil {
		return nil, err
	}

	return summary, nil
}

type CellwiseAttSummary struct{}

func (m Method) CollectShards(ctx context.Context, commit, prevCommit *doltdb.Commit, summary att.Summary) ([]att.ShardInfo, error) {
	root, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	var prevRoot doltdb.RootValue
	if prevCommit != nil {
		prevRoot, err = prevCommit.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}
	}

	var pas ProllyAttSummary
	if summary == nil {
		pas = emptySummary(m.startHash)
	} else {
		pas = summary.(ProllyAttSummary)
	}

	return m.collectShards(ctx, pas, root, prevRoot)
}

func (m Method) collectShards(ctx context.Context, summary ProllyAttSummary, root, prevRoot doltdb.RootValue) ([]att.ShardInfo, error) {
	if jsonData, err := json.Marshal(summary); err == nil {
		m.logger.Info("collecting shards", zap.String("summary", string(jsonData)))
	}

	tables, err := doltutils.GetScoredTables(ctx, summary.tableNames(), root)
	if err != nil {
		return nil, err
	}

	allShards := make([]att.ShardInfo, 0, len(tables)*16)

	for _, table := range tables {
		shards, ok := summary.TableShards[table]
		if !ok {
			shards = []AttributionShard{{Table: table}}
		}

		hasDiffs, err := m.shardsHaveDiffs(ctx, shards, table, root, prevRoot)
		if err != nil {
			return nil, err
		}

		for i := range shards {
			if hasDiffs[i] {
				subDivided, err := m.subdivideShard(ctx, shards[i], table, root, prevRoot)
				if err != nil {
					return nil, err
				}

				// If we subdivide a shard, we have to do work to redistribute
				// the commit counts. So we can't check if they have diffs and
				// exclude them.
				allShards = append(allShards, subDivided...)
			} else {
				m.logger.Info(fmt.Sprintf("Shard %d has no diffs", i))
				allShards = append(allShards, UnchangedShard{shards[i]})
			}
		}
	}

	return allShards, nil
}

func (m Method) subdivideShard(ctx context.Context, shard AttributionShard, table string, root doltdb.RootValue, prevRoot doltdb.RootValue) ([]att.ShardInfo, error) {
	rowData, err := getRowData(ctx, table, root)
	if err != nil {
		return nil, err
	}

	size, err := rowData.GetKeyRangeCardinality(ctx, shard.StartInclusive, shard.EndExclusive)
	if err != nil {
		return nil, err
	}

	var prevSize uint64
	var prevRowData prolly.Map
	if prevRoot != nil {
		prevRowData, err = getRowData(ctx, table, prevRoot)
		if err != nil {
			return nil, err
		}

		prevSize, err = prevRowData.GetKeyRangeCardinality(ctx, shard.StartInclusive, shard.EndExclusive)
		if err != nil {
			return nil, err
		}
	}

	subDivideRows := rowData
	shardCardinality := size
	if prevSize > size {
		shardCardinality = prevSize
		subDivideRows = prevRowData
	}
	shard.Cardinality = shardCardinality

	if shardCardinality <= uint64(m.shardParams.MaximumShardCardinality) {
		m.logger.Info("not going to subdivide shard. Shard cardinality <= MaximumShardCardinality.", zap.Uint64("cardinality", shardCardinality))
		return []att.ShardInfo{shard}, nil
	}

	var subDivisions []att.ShardInfo

	numSubs := (shardCardinality / uint64(m.shardParams.MaximumShardCardinality)) + 1
	subDivisionStep := shardCardinality / numSubs

	var startOrd uint64
	if len(shard.StartInclusive) > 0 {
		startOrd, err = subDivideRows.GetOrdinalForKey(ctx, shard.StartInclusive)
		if err != nil {
			return nil, err
		}
	}

	start := shard.StartInclusive
	for i := uint64(0); i < numSubs-1; i++ {
		endOrd := startOrd + subDivisionStep
		m.logger.Info("Creating subshard",
			zap.Uint64("start_ord", startOrd),
			zap.Uint64("end_ord", endOrd),
			zap.Uint64("cardinality", endOrd-startOrd))
		itr, err := subDivideRows.IterOrdinalRange(ctx, endOrd, endOrd+1)
		if err != nil {
			return nil, err
		}
		end, _, err := itr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				err = fmt.Errorf("expected to find key at ordinal range [%d, %d)", endOrd, endOrd+1)
			}
			return nil, err
		}

		newSub := AttributionShard{
			Table:          shard.Table,
			Path:           shard.Path,
			StartInclusive: start,
			EndExclusive:   end,
			Cardinality:    endOrd - startOrd,
		}
		subDivisions = append(subDivisions, newSub)

		start = end
		startOrd += subDivisionStep
	}

	var endOrd uint64
	var cardinality uint64
	if len(shard.EndExclusive) > 0 {
		endOrd, err = subDivideRows.GetOrdinalForKey(ctx, shard.EndExclusive)
		if err != nil {
			return nil, err
		}
		cardinality = endOrd - startOrd
	}

	lastSub := AttributionShard{
		Table:          shard.Table,
		Path:           shard.Path,
		StartInclusive: start,
		EndExclusive:   shard.EndExclusive,
		Cardinality:    cardinality,
	}

	m.logger.Info("Creating end shard",
		zap.Uint64("start_ord", startOrd),
		zap.Uint64("end_ord", endOrd),
		zap.Uint64("cardinality", cardinality))
	subDivisions = append(subDivisions, lastSub)

	m.logger.Info("Subdivided Shard", zap.String("shard_key", shard.Key(m.ddb.Format())), zap.Uint64("num_subdivisions", numSubs), zap.Uint64("sub_division_size", subDivisionStep))
	return subDivisions, nil
}

func getRowData(ctx context.Context, table string, root doltdb.RootValue) (prolly.Map, error) {
	tbl, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: table})
	if err != nil {
		return prolly.Map{}, err
	}
	if !ok {
		return prolly.Map{}, fmt.Errorf("could not find table %s in root", table)
	}
	idx, err := tbl.GetRowData(ctx)
	if err != nil {
		return prolly.Map{}, err
	}
	return durable.ProllyMapFromIndex(idx), nil
}

// looks at whether the table has changed.  If it has changed, it then looks to see if their are diffs that touch the
// corresponding shards
func (m Method) shardsHaveDiffs(ctx context.Context, shards []AttributionShard, table string, root, prevRoot doltdb.RootValue) ([]bool, error) {
	var tblHash hash.Hash
	var prevTblHash hash.Hash

	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: table})
	if err != nil {
		return nil, err
	}

	if ok {
		tblHash, err = tbl.HashOf()
		if err != nil {
			return nil, err
		}
	}

	var prevTbl *doltdb.Table
	if prevRoot != nil {
		prevTbl, ok, err = prevRoot.GetTable(ctx, doltdb.TableName{Name: table})
		if err != nil {
			return nil, err
		}

		if ok {
			prevTblHash, err = prevTbl.HashOf()
			if err != nil {
				return nil, err
			}
		}
	}

	hasDiffs := make([]bool, len(shards))
	if !tblHash.Equal(prevTblHash) {
		for i, shard := range shards {
			if err != nil {
				return nil, err
			}

			diffIter, err := getDiffer(ctx, shard, tbl, prevTbl)
			if err != nil {
				return nil, err
			}

			_, err = diffIter.Next(ctx)
			if err != nil && err != io.EOF {
				return nil, err
			}

			hasDiffs[i] = err != io.EOF
		}
	}

	return hasDiffs, err
}

func getDiffer(ctx context.Context, shard AttributionShard, tbl, prevTbl *doltdb.Table) (doltutils.ProllyDiffIter, error) {
	if tbl == nil && prevTbl == nil {
		panic("both |tbl| and |prevTbl| were nil")
	}

	var toSch schema.Schema
	var toM prolly.Map
	var fromSch schema.Schema
	var fromM prolly.Map
	var err error

	if tbl == nil {
		toSch, err = prevTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		idx, err := durable.NewEmptyIndex(ctx, prevTbl.ValueReadWriter(), prevTbl.NodeStore(), toSch)
		if err != nil {
			return nil, err
		}
		toM = durable.ProllyMapFromIndex(idx)
	} else {
		toSch, err = tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		idx, err := tbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		toM = durable.ProllyMapFromIndex(idx)
	}

	// If a table doesn't exist pretend that it is empty instead.
	if prevTbl == nil {
		fromSch, err = tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		idx, err := durable.NewEmptyIndex(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), fromSch)
		if err != nil {
			return nil, err
		}
		fromM = durable.ProllyMapFromIndex(idx)
	} else {
		fromSch, err = prevTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		idx, err := prevTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		fromM = durable.ProllyMapFromIndex(idx)
	}

	err = canDiffSchemas(tbl.Format(), toSch, fromSch)
	if err != nil {
		return nil, err
	}

	return doltutils.NewDiffIterKeyRange(ctx, fromM, toM, shard.StartInclusive, shard.EndExclusive)
}

func canDiffSchemas(format *types.NomsBinFormat, toSch, fromSch schema.Schema) error {
	// Allow renames and equivalent type changes for primary keys.
	if !schema.ArePrimaryKeySetsDiffable(format, fromSch, toSch) {
		return fmt.Errorf("primary key sets are not diffable:\nfrom: %s\nto: %s", fromSch.GetPKCols().GetColumnNames(), toSch.GetPKCols().GetColumnNames())
	}

	// Don't allow columns to be dropped or added.
	if fromSch.GetNonPKCols().Size() != toSch.GetNonPKCols().Size() {
		return fmt.Errorf("a column was dropped or added during the bounty:\nfrom: %s\nto: %s", fromSch.GetNonPKCols().GetColumnNames(), toSch.GetPKCols().GetColumnNames())
	}

	// Tags of non-pk columns must be in the same order between tables.
	// TODO (dhruv): Support equivalent column types like varchar(100) and varchar(150)?
	n := fromSch.GetNonPKCols().Size()
	for i := 0; i < n; i++ {
		fromCol := fromSch.GetNonPKCols().GetByIndex(i)
		toCol := toSch.GetNonPKCols().GetByIndex(i)
		if fromCol.Tag != toCol.Tag {
			return fmt.Errorf("column %s (%d) in from schema has different tag than column %s (%d) in to schema", fromCol.Name, fromCol.Tag, toCol.Name, toCol.Tag)
		}
	}

	return nil
}

func (m Method) ProcessShard(ctx context.Context, commitIdx int16, cm, prevCm *doltdb.Commit, shardInfo att.ShardInfo) (att.ShardResult, error) {
	shardKey := shardInfo.Key(m.ddb.Format())

	if infoJson, err := json.Marshal(shardInfo); err == nil {
		start := time.Now()
		m.logger.Info("Processing Shard Start", zap.String("shard_key", shardKey), zap.String("shard_info", string(infoJson)))
		defer func() {
			m.logger.Info("Processing Shard End", zap.String("shard_key", shardKey), zap.String("shard_info", string(infoJson)), zap.Duration("took", time.Since(start)))
		}()
	} else {
		m.logger.Error("failed to marshall shard info", zap.Error(err))
	}

	commitHash, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	var prevRoot doltdb.RootValue
	if prevCm != nil {
		prevRoot, err = prevCm.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}
	}

	if unchangedShard, ok := shardInfo.(UnchangedShard); ok {
		m.logger.Info("Shard has no changes", zap.String("shard_key", shardKey))
		return processUnchangedShard(ctx, unchangedShard.AttributionShard)
	}

	shard := shardInfo.(AttributionShard)
	tableName := shard.Table
	tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return nil, err
	}

	currSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	var prevTbl *doltdb.Table
	if prevRoot != nil {
		prevTbl, _, err = prevRoot.GetTable(ctx, doltdb.TableName{Name: tableName})

		if err != nil {
			return nil, err
		}
	}

	var prevAttribData prolly.Map
	if len(shard.Path) > 0 {
		prevAttribData, err = m.readShardFile(ctx, shard, currSch)
		if err != nil {
			return nil, err
		}
	} else {
		prevAttribData, err = m.makeEmptyAttribData(ctx, currSch)
		if err != nil {
			return nil, err
		}
	}

	diffIter, err := getDiffer(ctx, shard, tbl, prevTbl)
	if err != nil {
		return nil, err
	}

	basePath := commitHash.String()
	shardMgr := NewProllyShardManager(m.logger, m.ddb.Format(), int(commitIdx)+1, shard, shard.Table, basePath, currSch, m.shardParams, m.shardStore)
	dA, err := newDiffAttributor(ctx, tbl, prevTbl, commitIdx, shardMgr, shard, m.logger)
	if err != nil {
		return nil, err
	}

	attribIter, err := prevAttribData.IterKeyRange(ctx, shard.StartInclusive, shard.EndExclusive)
	if err != nil {
		return nil, err
	}

	err = shardMgr.openShard(ctx)
	if err != nil {
		return nil, err
	}

	// Walk iters in parallel
	err = dA.attributeDiffs(ctx, diffIter, attribIter)
	if err != nil {
		return nil, err
	}

	outShard, err := shardMgr.close(ctx)
	if err != nil {
		return nil, err
	}

	kd := currSch.GetKeyDescriptor()

	m.logger.Info("Shard work stats",
		zap.Int("attributions", dA.total),
		zap.Uint64("cardinality", shard.Cardinality),
		zap.String("range_debug", shard.DebugFormat(kd)))

	return []AttributionShard{outShard}, nil
}

func getAttribDescriptorsFromTblSchema(tblSch schema.Schema) (val.TupleDesc, val.TupleDesc) {
	kd := tblSch.GetKeyDescriptor()

	tt := make([]val.Type, tblSch.GetAllCols().Size())
	for i := range tt {
		tt[i] = val.Type{
			Enc:      val.Int16Enc,
			Nullable: true,
		}
	}
	vd := val.NewTupleDescriptor(tt...)

	return kd, vd
}

func (m Method) readShardFile(ctx context.Context, shard AttributionShard, tblSch schema.Schema) (prolly.Map, error) {
	start := time.Now()
	shardKey := shard.Key(m.ddb.Format())
	m.logger.Info("Reading shard", zap.String("shard_key", shardKey))
	defer func() {
		m.logger.Info("Done reading shard", zap.String("shard_key", shardKey), zap.Duration("took", time.Since(start)))
	}()

	memShard, err := m.shardStore.ReadShard(ctx, shard.Path)
	if err != nil {
		return prolly.Map{}, err
	}

	rootNodeRef, ok := memShard.Value.(types.Ref)
	if !ok {
		return prolly.Map{}, fmt.Errorf("expected shard value to be a ref")
	}

	rootNodeVal, err := memShard.Vrw.ReadValue(ctx, rootNodeRef.TargetHash())
	if err != nil {
		return prolly.Map{}, err
	}

	kd, vd := getAttribDescriptorsFromTblSchema(tblSch)
	rootNode, err := shim.NodeFromValue(rootNodeVal)
	if err != nil {
		return prolly.Map{}, err
	}
	pm := prolly.NewMap(rootNode, memShard.Ns, kd, vd)

	return pm, nil
}

func (m Method) makeEmptyAttribData(ctx context.Context, tblSch schema.Schema) (prolly.Map, error) {
	kd, vd := getAttribDescriptorsFromTblSchema(tblSch)
	store, err := valuefile.NewFileValueStore(m.ddb.Format())
	if err != nil {
		return prolly.Map{}, err
	}
	ns := tree.NewNodeStore(store)
	return prolly.NewMapFromTuples(ctx, ns, kd, vd)
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

func (m Method) ProcessResults(ctx context.Context, commitHash hash.Hash, prevSummary att.Summary, results []att.ShardResult) (att.Summary, error) {
	// nil summary not allowed.  pass emptySummary when there is no prev summary
	ps := prevSummary.(ProllyAttSummary)

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

	return ProllyAttSummary{
		StartHash:    ps.StartHash,
		CommitHashes: commitHashes,
		CommitCounts: commitCounts,
		TableShards:  tableShards,
	}, nil
}

func (m Method) WriteSummary(ctx context.Context, summary att.Summary) (string, error) {
	cws := summary.(ProllyAttSummary)
	commitHash := cws.CommitHashes[cws.NumCommits()-1]
	commitHashStr := commitHash.String()

	store, err := valuefile.NewFileValueStore(m.ddb.Format())
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
	err = m.shardStore.WriteShard(ctx, key, store, v)
	if err != nil {
		return "", err
	}

	return key, err
}

func (m Method) SerializeShardInfo(ctx context.Context, info att.ShardInfo) ([]byte, error) {
	var buf bytes.Buffer

	unchanged, ok := info.(UnchangedShard)
	if ok {
		buf.WriteByte(0)
		err := unchanged.serialize(&buf)
		if err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	cellWiseShard, ok := info.(AttributionShard)
	if ok {
		buf.WriteByte(1)
		err := cellWiseShard.serialize(&buf)
		if err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	return nil, errors.New("Unsupported shard type for cellwise attribution")
}

func (m Method) DeserializeShardInfo(ctx context.Context, data []byte) (att.ShardInfo, error) {
	buf := bytes.NewBuffer(data)
	shardType, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	if shardType < 0 || shardType > 1 {
		return nil, errors.New("corrupt shard info")
	}

	shardInfo, err := deserializeShard(buf)
	if err != nil {
		return nil, err
	}

	if shardType == 0 {
		return UnchangedShard{AttributionShard: shardInfo}, nil
	} else {
		return shardInfo, nil
	}
}

func deserializeShard(rd io.Reader) (AttributionShard, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return AttributionShard{}, err
	}

	var a AttributionShard
	err = json.Unmarshal(data, &a)
	if err != nil {
		return AttributionShard{}, err
	}

	return a, nil
}

func (m Method) SerializeResults(ctx context.Context, results att.ShardResult) ([]byte, error) {
	attShards, ok := results.([]AttributionShard)
	if !ok {
		return nil, errors.New("unexpected result type")
	}

	return json.Marshal(attShards)
}

func (m Method) DeserializeResults(ctx context.Context, data []byte) (att.ShardResult, error) {
	var results []AttributionShard
	err := json.Unmarshal(data, &results)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// UnchangedShard is used to mark shards which have not changed since they were last processed
type UnchangedShard struct {
	AttributionShard
}
