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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	TestTableName = "test"
	NumCommits    = 5
)

type tableState struct {
	rowData types.Map
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

func genTableState(ctx context.Context, vrw types.ValueReadWriter) ([]tableState, error) {
	var states []tableState

	nbf := vrw.Format()
	m, err := types.NewMap(ctx, vrw)

	if err != nil {
		return nil, err
	}

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
	states = append(states, tableState{rowData: m, sch: AttSch})

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
	states = append(states, tableState{rowData: m, sch: AttSch})

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
	states = append(states, tableState{rowData: m, sch: AttSch})

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

	states = append(states, tableState{rowData: m, sch: UpdatedSch})

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
	states = append(states, tableState{rowData: m, sch: UpdatedSch})

	return states, nil
}

func createTable(ctx context.Context, ddb *doltdb.DoltDB, state tableState) (*doltdb.Table, error) {
	vrw := ddb.ValueReadWriter()
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, state.sch)

	if err != nil {
		return nil, err
	}

	m, err := types.NewMap(ctx, ddb.ValueReadWriter())

	if err != nil {
		return nil, err
	}

	tbl, err := doltdb.NewTable(ctx, vrw, schVal, state.rowData, m, nil)

	if err != nil {
		return nil, err
	}

	return tbl, nil
}

func createCommit(ctx context.Context, ddb *doltdb.DoltDB, root *doltdb.RootValue, tbl *doltdb.Table, parents []*doltdb.Commit, meta *doltdb.CommitMeta) (*doltdb.RootValue, *doltdb.Commit, error) {
	var err error
	if tbl != nil {
		root, err = root.PutTable(ctx, TestTableName, tbl)

		if err != nil {
			return nil, nil, err
		}
	}

	h, err := ddb.WriteRootValue(ctx, root)

	if err != nil {
		return nil, nil, err
	}

	cm, err := ddb.CommitWithParentCommits(ctx, h, ref.NewBranchRef("master"), parents, meta)

	if err != nil {
		return nil, nil, err
	}

	return root, cm, nil
}

func GenTestCommitGraph(ctx context.Context, ddb *doltdb.DoltDB, meta [NumCommits]*doltdb.CommitMeta) (hash.Hash, *doltdb.Commit, error) {
	states, err := genTableState(ctx, ddb.ValueReadWriter())

	if err != nil {
		return hash.Hash{}, nil, err
	}

	initialCommit, err := ddb.ResolveRef(ctx, ref.NewBranchRef("master"))

	if err != nil {
		return hash.Hash{}, nil, err
	}

	root, err := initialCommit.GetRootValue()

	if err != nil {
		return hash.Hash{}, nil, err
	}

	m, err := types.NewMap(ctx, ddb.ValueReadWriter())

	if err != nil {
		return hash.Hash{}, nil, err
	}

	tbl, err := createTable(ctx, ddb, tableState{rowData: m, sch: AttSch})

	if err != nil {
		return hash.Hash{}, nil, err
	}

	createCmMeta, err := doltdb.NewCommitMeta("__user__", "user@fake.horse", "state at start of bounty")

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

	h, err := bountyStart.HashOf()

	if err != nil {
		return hash.Hash{}, nil, err
	}

	return h, head, nil
}
