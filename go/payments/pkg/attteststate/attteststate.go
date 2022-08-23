// Copyright 2020 Dolthub, Inc.
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

package attteststate

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	TestTableName = "test"
	NumCommits    = 5
)

type tableState struct {
	rowData durable.Index
	sch     schema.Schema
}

const (
	pkTag uint64 = iota
	col1Tag
	col2Tag
	col3Tag
	col4Tag
)

var AttSch schema.Schema
var UpdatedSch schema.Schema

func init() {
	cols := []schema.Column{
		schema.NewColumn("pk", pkTag, types.UintKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("col1", col1Tag, types.StringKind, false),
		schema.NewColumn("col2", col2Tag, types.StringKind, false),
		schema.NewColumn("col3", col3Tag, types.StringKind, false),
	}

	attColColl := schema.NewColCollection(cols...)

	columns := make([]schema.Column, len(cols))
	copy(columns, cols)

	columns[3] = schema.NewColumn("col4", col4Tag, types.StringKind, false)

	updatedColColl := schema.NewColCollection(columns...)

	var err error
	AttSch, err = schema.SchemaFromCols(attColColl)
	if err != nil {
		panic(err)
	}

	UpdatedSch, err = schema.SchemaFromCols(updatedColColl)
	if err != nil {
		panic(err)
	}
}

var sharedPool = pool.NewBuffPool()

func prollyGenTableState(ctx context.Context, ns tree.NodeStore) ([]tableState, error) {
	var states []tableState

	kd, vd := AttSch.GetMapDescriptors()
	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
	if err != nil {
		return nil, err
	}
	kb := val.NewTupleBuilder(kd)
	vb := val.NewTupleBuilder(vd)

	// ========== STATE 1 ==========
	mut := m.Mutate()
	// * Inserts 1000 rows with 2 valid columns (pk, col1)
	for i := uint64(0); i < 1000; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)
		vb.PutString(0, "a")
		v := vb.Build(sharedPool)

		err = mut.Put(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	m, err = mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromProllyMap(m), sch: AttSch})

	// ========== STATE 2 ==========
	mut = m.Mutate()
	// Adds 100 new rows with 3 columns (pk, col1, col2)
	for i := uint64(1000); i < 1100; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)
		vb.PutString(0, "b")
		vb.PutString(1, "b")
		v := vb.Build(sharedPool)

		err = mut.Put(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	// Delete 100 rows (pks 0 - 99)
	for i := uint64(0); i < 100; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)

		if err != nil {
			return nil, err
		}

		err = mut.Delete(ctx, k)
		if err != nil {
			return nil, err
		}
	}

	// Modify col1 of 100 old rows and add col2 (pks 100 - 199)
	for i := uint64(100); i < 200; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)
		vb.PutString(0, "b")
		vb.PutString(1, "b")
		v := vb.Build(sharedPool)

		err = mut.Put(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	m, err = mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromProllyMap(m), sch: AttSch})

	// ========== STATE 3 ==========
	mut = m.Mutate()
	// Adds 100 new rows with 4 columns (pk, col1, col2, col3)
	for i := uint64(1100); i < 1200; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)
		vb.PutString(0, "c")
		vb.PutString(1, "c")
		vb.PutString(2, "c")
		v := vb.Build(sharedPool)

		err = mut.Put(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	// Delete 100 rows (pks 100 - 199)
	for i := uint64(100); i < 200; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)

		if err != nil {
			return nil, err
		}

		err = mut.Delete(ctx, k)
		if err != nil {
			return nil, err
		}
	}

	// Modify 100 rows.  50 created in commit 1, 50 created in commit 2 (pks 950 to 1049)
	for i := uint64(950); i < 1050; i++ {
		kb.PutUint64(0, i)
		k := kb.Build(sharedPool)
		vb.PutString(0, "c")
		vb.PutString(1, "c")
		vb.PutString(2, "c")
		v := vb.Build(sharedPool)

		err = mut.Put(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	m, err = mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromProllyMap(m), sch: AttSch})

	// ========== STATE 4 ==========
	// *** schema updated
	kd2, vd2 := UpdatedSch.GetMapDescriptors()
	kb2, vb2 := val.NewTupleBuilder(kd), val.NewTupleBuilder(vd)

	// create new map with old values from col1 and col2
	var tups []val.Tuple
	itr, err := m.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	var k, v val.Tuple
	for k, v, err = itr.Next(ctx); err == nil; k, v, err = itr.Next(ctx) {
		kb2.PutRaw(0, k.GetField(0))
		k2 := kb2.Build(sharedPool)
		tups = append(tups, k2)

		vb2.PutRaw(0, v.GetField(0))
		vb2.PutRaw(1, v.GetField(1))
		v2 := vb2.Build(sharedPool)
		tups = append(tups, v2)
	}
	if err != io.EOF {
		return nil, err
	}

	m2, err := prolly.NewMapFromTuples(ctx, ns, kd2, vd2, tups...)
	if err != nil {
		return nil, err
	}

	mut2 := m2.Mutate()
	// Adds 100 rows that have existed before with 4 columns (pk, col1, col2, col4), pk, col1 and col2 values from previous commits
	for i := uint64(100); i < 200; i++ {
		kb2.PutUint64(0, i)
		k2 := kb2.Build(sharedPool)
		vb2.PutString(0, "a")
		vb2.PutString(1, "b")
		vb2.PutString(2, "d")
		v2 := vb2.Build(sharedPool)

		err = mut.Put(ctx, k2, v2)
		if err != nil {
			return nil, err
		}
	}

	m2, err = mut2.Map(ctx)
	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromProllyMap(m2), sch: AttSch})

	// ========== STATE 5 ==========
	mut2 = m2.Mutate()
	// edit col1 to be back the the value set in commit2
	for i := uint64(100); i < 200; i++ {
		kb2.PutUint64(0, i)
		k2 := kb2.Build(sharedPool)
		vb2.PutString(0, "b")
		vb2.PutString(1, "b")
		vb2.PutString(2, "d")
		v2 := vb2.Build(sharedPool)

		err = mut.Put(ctx, k2, v2)
		if err != nil {
			return nil, err
		}
	}

	m2, err = mut2.Map(ctx)
	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromProllyMap(m2), sch: AttSch})

	return states, nil
}

func nomsGenTableState(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore) ([]tableState, error) {
	var states []tableState

	nbf := vrw.Format()
	m, err := types.NewMap(ctx, vrw)

	if err != nil {
		return nil, err
	}

	// ========== STATE 1 ==========
	me := m.Edit()
	// * Inserts 1000 rows with 2 valid columns (pk, col1)
	for i := uint64(0); i < 1000; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("a"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(AttSch), r.NomsMapValue(AttSch))
	}

	m, err = me.Map(ctx)

	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromNomsMap(m, vrw, ns), sch: AttSch})

	// ========== STATE 2 ==========
	me = m.Edit()
	// Adds 100 new rows with 3 columns (pk, col1, col2)
	for i := uint64(1000); i < 1100; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("b"),
			col2Tag: types.String("b"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(AttSch), r.NomsMapValue(AttSch))
	}

	// Delete 100 rows (pks 0 - 99)
	for i := uint64(0); i < 100; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{pkTag: types.Uint(i)})

		if err != nil {
			return nil, err
		}

		me.Remove(r.NomsMapKey(AttSch))
	}

	// Modify col1 of 100 old rows and add col2 (pks 100 - 199)
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("b"),
			col2Tag: types.String("b"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(AttSch), r.NomsMapValue(AttSch))
	}

	m, err = me.Map(ctx)

	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromNomsMap(m, vrw, ns), sch: AttSch})

	// ========== STATE 3 ==========
	me = m.Edit()
	// Adds 100 new rows with 4 columns (pk, col1, col2, col3)
	for i := uint64(1100); i < 1200; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("c"),
			col2Tag: types.String("c"),
			col3Tag: types.String("c"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(AttSch), r.NomsMapValue(AttSch))
	}

	// Delete 100 rows (pks 100 - 199)
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{pkTag: types.Uint(i)})

		if err != nil {
			return nil, err
		}

		me.Remove(r.NomsMapKey(AttSch))
	}

	// Modify 100 rows.  50 created in commit 1, 50 created in commit 2 (pks 950 to 1049)
	for i := uint64(950); i < 1050; i++ {
		r, err := row.New(nbf, AttSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("c"),
			col2Tag: types.String("c"),
			col3Tag: types.String("c"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(AttSch), r.NomsMapValue(AttSch))
	}

	m, err = me.Map(ctx)

	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromNomsMap(m, vrw, ns), sch: AttSch})

	// ========== STATE 4 ==========
	me = m.Edit()
	// *** schema updated
	// Adds 100 rows that have existed before with 4 columns (pk, col1, col2, col4), pk, col1 and col2 values from previous commits
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, UpdatedSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("a"),
			col2Tag: types.String("b"),
			col4Tag: types.String("d"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(UpdatedSch), r.NomsMapValue(UpdatedSch))
	}

	m, err = me.Map(ctx)

	if err != nil {
		return nil, err
	}

	states = append(states, tableState{rowData: durable.IndexFromNomsMap(m, vrw, ns), sch: UpdatedSch})

	// ========== STATE 5 ==========
	me = m.Edit()
	// edit col1 to be back the the value set in commit2
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, UpdatedSch, row.TaggedValues{
			pkTag:   types.Uint(i),
			col1Tag: types.String("b"),
			col2Tag: types.String("b"),
			col4Tag: types.String("d"),
		})

		if err != nil {
			return nil, err
		}

		me.Set(r.NomsMapKey(UpdatedSch), r.NomsMapValue(UpdatedSch))
	}

	m, err = me.Map(ctx)

	if err != nil {
		return nil, err
	}
	states = append(states, tableState{rowData: durable.IndexFromNomsMap(m, vrw, ns), sch: UpdatedSch})

	return states, nil
}

func createTable(ctx context.Context, ddb *doltdb.DoltDB, state tableState) (*doltdb.Table, error) {
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()
	tbl, err := doltdb.NewTable(ctx, vrw, ns, state.sch, state.rowData, nil, nil)

	if err != nil {
		return nil, err
	}

	return tbl, nil
}

func createCommit(ctx context.Context, ddb *doltdb.DoltDB, root *doltdb.RootValue, tbl *doltdb.Table, parents []*doltdb.Commit, meta *datas.CommitMeta) (*doltdb.RootValue, *doltdb.Commit, error) {
	var err error
	if tbl != nil {
		root, err = root.PutTable(ctx, TestTableName, tbl)

		if err != nil {
			return nil, nil, err
		}
	}

	r, h, err := ddb.WriteRootValue(ctx, root)

	if err != nil {
		return nil, nil, err
	}
	root = r

	cm, err := ddb.CommitDanglingWithParentCommits(ctx, h, parents, meta)

	if err != nil {
		return nil, nil, err
	}

	return root, cm, nil
}

func GenTestCommitGraph(ctx context.Context, ddb *doltdb.DoltDB, meta [NumCommits]*datas.CommitMeta) (hash.Hash, *doltdb.Commit, error) {
	var states []tableState
	var err error
	if types.IsFormat_DOLT(ddb.Format()) {
		states, err = prollyGenTableState(ctx, ddb.NodeStore())
	} else {
		states, err = nomsGenTableState(ctx, ddb.ValueReadWriter(), ddb.NodeStore())
	}

	if err != nil {
		return hash.Hash{}, nil, err
	}

	initialCommit, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("main"))

	if err != nil {
		return hash.Hash{}, nil, err
	}

	root, err := initialCommit.GetRootValue(ctx)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	idx, err := durable.NewEmptyIndex(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), AttSch)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	tbl, err := createTable(ctx, ddb, tableState{rowData: idx, sch: AttSch})

	if err != nil {
		return hash.Hash{}, nil, err
	}

	createCmMeta, err := datas.NewCommitMeta("__user__", "user@fake.horse", "state at start of bounty")

	if err != nil {
		return hash.Hash{}, nil, err
	}

	root, bountyStart, err := createCommit(ctx, ddb, root, tbl, []*doltdb.Commit{initialCommit}, createCmMeta)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	head := bountyStart

	for i, state := range states {
		tbl, err := createTable(ctx, ddb, state)

		if err != nil {
			return hash.Hash{}, nil, err
		}

		var cm *doltdb.Commit
		root, cm, err = createCommit(ctx, ddb, root, tbl, []*doltdb.Commit{initialCommit}, meta[i])
		root, head, err = createCommit(ctx, ddb, root, tbl, []*doltdb.Commit{head, cm}, meta[i])
	}

	err = ddb.SetHeadToCommit(ctx, ref.NewBranchRef("main"), head)
	if err != nil {
		return hash.Hash{}, nil, err
	}

	h, err := bountyStart.HashOf()
	if err != nil {
		return hash.Hash{}, nil, err
	}

	return h, head, nil
}
