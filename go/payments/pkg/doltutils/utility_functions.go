package doltutils

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func GetRows(ctx context.Context, root *doltdb.RootValue, tableName string) (types.Map, schema.Schema, error) {
	tbl, ok, err := root.GetTable(ctx, tableName)

	if err != nil {
		return types.EmptyMap, nil, err
	} else if !ok {
		return types.EmptyMap, nil, nil
	}

	rowData, err := tbl.GetRowData(ctx)

	if err != nil {
		return types.EmptyMap, nil, err
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return types.EmptyMap, nil, err
	}

	return rowData, sch, err
}

// GetMergeCommitsAfter iterates through the commit graph backwards until it finds `afterHash`, only ever following a
// a commits first parent. Each merge commit is appended to a slice which is ordered from newest commit to oldest commit
func GetMergeCommitsAfter(ctx context.Context, db *doltdb.DoltDB, current *doltdb.Commit, afterHash hash.Hash) ([]*doltdb.Commit, error) {
	var mergeCommits []*doltdb.Commit

	// I don't think you should be able to create this kind of cycle unless you wrote code to be malicious
	processed := make(map[hash.Hash]bool)

	for {
		currHash, err := current.HashOf()

		if err != nil {
			return nil, err
		}

		if currHash == afterHash || processed[currHash] {
			break
		}

		processed[currHash] = true
		parentHashes, err := current.ParentHashes(ctx)

		if err != nil {
			return nil, err
		}

		if len(parentHashes) == 0 {
			break
		} else if len(parentHashes) == 2 {
			mergeCommits = append(mergeCommits, current)
		}

		cs, err := doltdb.NewCommitSpec(parentHashes[0].String())

		if err != nil {
			return nil, err
		}

		current, err = db.Resolve(ctx, cs, nil)

		if err != nil {
			return nil, err
		}
	}

	return mergeCommits, nil
}

func GetMergeCommitsBetween(ctx context.Context, db *doltdb.DoltDB, start, end hash.Hash) ([]*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(end.String())

	if err != nil {
		return nil, err
	}

	cm, err := db.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, err
	}

	return GetMergeCommitsAfter(ctx, db, cm, start)
}
