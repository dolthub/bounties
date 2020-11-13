package cellwise

import (
	"context"
	"fmt"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

const (
	testTableName = "test"
	testUsername = "Test User"
	testEmail = "test@fake.horse"
)

type tableState struct {
	rowData types.Map
	sch schema.Schema
}

const (
	pkTag uint64 = iota
	col1Tag
	col2Tag
	col3Tag
	col4Tag
)

var attSch schema.Schema
var updatedSch schema.Schema
var superSch *schema.SuperSchema

func init() {
	cols := []schema.Column{
		schema.NewColumn("pk", pkTag, types.UintKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("col1", col1Tag, types.StringKind, false),
		schema.NewColumn("col2", col2Tag, types.StringKind, false),
		schema.NewColumn("col3", col3Tag, types.StringKind, false),
	}

	attColColl, err := schema.NewColCollection(cols...)

	if err != nil {
		panic(err)
	}

	cols[3] = schema.NewColumn("col4", col4Tag, types.StringKind, false)

	updatedColColl, err := schema.NewColCollection(cols...)

	if err != nil {
		panic(err)
	}

	attSch = schema.SchemaFromCols(attColColl)
	updatedSch = schema.SchemaFromCols(updatedColColl)

	superSch, err = schema.NewSuperSchema(attSch, updatedSch)

	if err != nil {
		panic(err)
	}
}

func getTestEnv(ctx context.Context, t *testing.T) *env.DoltEnv {
	const (
		homeDir = "/Users/madam"
		relativeTestDir = "databases/test"
	)

	testDir := filepath.Join(homeDir, relativeTestDir)
	hdp := func() (string, error) { return homeDir, nil }
	fs := filesys.NewInMemFS([]string{testDir}, nil, testDir)
	dEnv := env.Load(ctx, hdp, fs, doltdb.InMemDoltDB, "")
	require.NoError(t, dEnv.DocsLoadErr)
	require.NoError(t, dEnv.DBLoadError)
	require.NoError(t, dEnv.CfgLoadErr)
	require.Error(t, dEnv.RSLoadErr)

	err := dEnv.InitDBAndRepoState(ctx, types.Format_Default, testUsername, testEmail, time.Now())
	require.NoError(t, err)

	return dEnv
}

func genTableState(ctx context.Context, t *testing.T, vrw types.ValueReadWriter) ([][]int, []tableState) {
	var states []tableState
	var expected [][]int

	nbf := vrw.Format()
	m, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)

	me := m.Edit()
	// * Inserts 1000 rows with 2 valid columns (pk, col1)
	for i := uint64(0); i < 1000; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("a"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(attSch), r.NomsMapValue(attSch))
	}

	// Expected Scoreboard:
	// Commit 1: 2*1000 new = 2000
	expected = append(expected,[]int{2000})

	m, err = me.Map(ctx)
	require.NoError(t, err)
	states = append(states, tableState{ rowData: m, sch: attSch })

	me = m.Edit()
	// Adds 100 new rows with 3 columns (pk, col1, col2)
	for i := uint64(1000); i < 1100; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("b"),
			col2Tag: types.String("b"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(attSch), r.NomsMapValue(attSch))
	}

	// Delete 100 rows (pks 0 - 99)
	for i := uint64(0); i < 100; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{pkTag: types.Uint(i)})
		require.NoError(t, err)

		me.Remove(r.NomsMapKey(attSch))
	}

	// Modify col1 of 100 old rows and add col2 (pks 100 - 199)
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("b"),
			col2Tag: types.String("b"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(attSch), r.NomsMapValue(attSch))
	}

	// Expected Scoreboard:
	// Commit 1: 2000 - (2*100 deleted + 1*100 col1 changes) = 1700
	// Commit 2: 3*100 new + (2 * 100 changed) = 500
	expected = append(expected,[]int{1700, 500})

	m, err = me.Map(ctx)
	require.NoError(t, err)
	states = append(states, tableState{ rowData: m, sch: attSch })

	me = m.Edit()
	// Adds 100 new rows with 4 columns (pk, col1, col2, col3)
	for i := uint64(1100); i < 1200; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("c"),
			col2Tag: types.String("c"),
			col3Tag: types.String("c"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(attSch), r.NomsMapValue(attSch))
	}

	// Delete 100 rows (pks 100 - 199)
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{pkTag: types.Uint(i)})
		require.NoError(t, err)

		me.Remove(r.NomsMapKey(attSch))
	}

	// Modify 100 rows.  50 created in commit 1, 50 created in commit 2 (pks 950 to 1049)
	for i := uint64(950); i < 1050; i++ {
		r, err := row.New(nbf, attSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("c"),
			col2Tag: types.String("c"),
			col3Tag: types.String("c"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(attSch), r.NomsMapValue(attSch))
	}

	// Expected Scoreboard:
	// Commit 1: 1700 - (1*100 deleted + 1*50 col1 changes) = 1550
	// Commit 2: 500 - (2*100 deleted + 2*50 col1/col2 changes) = 200
	// Commit 3: 4*100 new + (3 * 100 changed) = 700
	expected = append(expected,[]int{1550, 200, 700})

	m, err = me.Map(ctx)
	require.NoError(t, err)
	states = append(states, tableState{ rowData: m, sch: attSch })

	me = m.Edit()
	// *** schema updated
	// Adds 100 rows that have existed before with 4 columns (pk, col1, col2, col4), pk, col1 and col2 values from p`revious commits
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, updatedSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("a"),
			col2Tag: types.String("b"),
			col4Tag: types.String("d"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(updatedSch), r.NomsMapValue(updatedSch))
	}

	// Expected Scoreboard:
	// Commit 1: 1550 + 2*100 re-added = 1750
	// Commit 2: 200 + 1*100 re-added = 300
	// Commit 3: 700 - 200 col3 cells removed from schema = 500
	// Commit 4: 100 rows with col4 = 100
	expected = append(expected,[]int{1750, 300, 500, 100})

	m, err = me.Map(ctx)
	require.NoError(t, err)
	states = append(states, tableState{ rowData: m, sch: updatedSch })

	me = m.Edit()
	// edit col1 to be back the the value set in commit2
	for i := uint64(100); i < 200; i++ {
		r, err := row.New(nbf, updatedSch, row.TaggedValues{
			pkTag: types.Uint(i),
			col1Tag: types.String("b"),
			col2Tag: types.String("b"),
			col4Tag: types.String("d"),
		})
		require.NoError(t, err)

		me.Set(r.NomsMapKey(updatedSch), r.NomsMapValue(updatedSch))
	}

	// Expected Scoreboard:
	// Commit 1: 1750 - 1*100 col1 changed = 1650
	// Commit 2: 300 + 1*100 col1 reverted to "b" = 400
	// Commit 3: 500
	// Commit 4: 100
	// Commit 5: 0
	expected = append(expected,[]int{1650, 400, 500, 100, 0})

	m, err = me.Map(ctx)
	require.NoError(t, err)
	states = append(states, tableState{ rowData: m, sch: updatedSch })

	// Clear everything out.  Make sure we are back where we expected
	m, err = types.NewMap(ctx, vrw)
	require.NoError(t, err)

	expected = append(expected,[]int{0,0,0,0,0})
	states = append(states, tableState{ rowData: m, sch: updatedSch })

	return expected, states
}

func createTable(ctx context.Context, t *testing.T, dEnv *env.DoltEnv, state tableState) *doltdb.Table {
	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err  := encoding.MarshalSchemaAsNomsValue(ctx, vrw, state.sch)
	tbl, err := doltdb.NewTable(ctx, vrw, schVal, state.rowData, nil)
	require.NoError(t, err)

	return tbl
}

func createCommit(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, root *doltdb.RootValue, commitMsg string, tbl *doltdb.Table, parents []*doltdb.Commit) (*doltdb.RootValue, *doltdb.Commit) {
	var err error
	if tbl != nil {
		root, err = root.PutTable(ctx, testTableName, tbl)
		require.NoError(t, err)
	}

	h, err := ddb.WriteRootValue(ctx , root)
	require.NoError(t, err)
	meta, err := doltdb.NewCommitMeta(testUsername, testEmail, commitMsg)
	require.NoError(t, err)
	cm, err := ddb.CommitWithParentCommits(ctx, h, ref.NewBranchRef("master"), parents, meta)
	require.NoError(t, err)
	return root, cm
}

func genTestCommitGraph(ctx context.Context, t *testing.T, dEnv *env.DoltEnv, states []tableState) (hash.Hash, *doltdb.Commit) {
	initialCommit, err := dEnv.DoltDB.ResolveRef(ctx, ref.NewBranchRef("master"))
	require.NoError(t, err)
	root, err := initialCommit.GetRootValue()
	require.NoError(t, err)

	m, err := types.NewMap(ctx, dEnv.DoltDB.ValueReadWriter())
	require.NoError(t, err)
	tbl := createTable(ctx, t, dEnv, tableState{ rowData: m, sch: attSch })
	root, bountyStart := createCommit(ctx, t, dEnv.DoltDB, root, "bounty start", tbl, []*doltdb.Commit{initialCommit})

	head := bountyStart

	for i, state := range states {
		tbl := createTable(ctx, t, dEnv, state)
		commitMsg := fmt.Sprintf("Commit %d", i)

		var cm *doltdb.Commit
		root, cm = createCommit(ctx, t, dEnv.DoltDB, root, commitMsg, tbl, []*doltdb.Commit{initialCommit})
		root, head = createCommit(ctx, t, dEnv.DoltDB, root, commitMsg, tbl, []*doltdb.Commit{head, cm})
	}

	h, err := bountyStart.HashOf()
	require.NoError(t, err)

	return h, head
}

func assertOnExpectedAttribution(_ context.Context, t *testing.T, expected []int, att *TableAttribution) {
	var expectedTotal int64
	for commitIdx, count := range att.CommitToCellCount {
		expectedTotal += int64(expected[commitIdx])
		assert.Equal(t, expected[commitIdx], count)
	}
	assert.Equal(t, expectedTotal, att.AttributedCells)
}

func TestAttribution(t *testing.T) {
	ctx := context.Background()
	dEnv := getTestEnv(ctx, t)
	expected, states := genTableState(ctx, t, dEnv.DoltDB.ValueReadWriter())
	startOfBountyHash, cm := genTestCommitGraph(ctx, t, dEnv, states)

	commits, err := doltutils.GetMergeCommitsAfter(ctx, dEnv.DoltDB, cm, startOfBountyHash)
	require.NoError(t, err)
	att := NewDatabaseAttribution(startOfBountyHash)
	require.Equal(t, len(expected), len(commits))

	for i := 0; i < len(expected); i++ {
		expectedAtt := expected[i]
		commit := commits[len(commits) - (i + 1)]
		commitHash, err := commit.HashOf()
		require.NoError(t, err)
		err = att.Update(ctx, dEnv.DoltDB, startOfBountyHash, commitHash)
		require.NoError(t, err)
		cellAtt, ok := att.NameToTableAttribution[testTableName]
		require.True(t, ok)
		assertOnExpectedAttribution(ctx, t, expectedAtt, cellAtt)

		bytes, err := Serialize(att)
		require.NoError(t, err)
		att, err = Deserialize(bytes)
		require.NoError(t, err)
	}
}


