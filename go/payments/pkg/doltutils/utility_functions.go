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

package doltutils

import (
	"context"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// GetRows gets a tables row data and schema from a rootValue and table name
func GetRows(ctx context.Context, root *doltdb.RootValue, tableName string) (types.Map, schema.Schema, error) {
	tbl, ok, err := root.GetTable(ctx, tableName)

	if err != nil {
		return types.EmptyMap, nil, err
	} else if !ok {
		m, err := types.NewMap(ctx, root.VRW())

		if err != nil {
			return types.EmptyMap, nil, err
		}

		return m, nil, nil
	}

	rowData, err := tbl.GetNomsRowData(ctx)

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
// a commits first parent. The merge commits are returned in order from the oldest to the newest
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

		optCmt, err := db.Resolve(ctx, cs, nil)
		if err != nil {
			return nil, err
		}
		ok := false
		current, ok = optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitRuntimeFailure
		}
	}

	for i, j := 0, len(mergeCommits)-1; i < j; i, j = i+1, j-1 {
		mergeCommits[i], mergeCommits[j] = mergeCommits[j], mergeCommits[i]
	}

	return mergeCommits, nil
}

// GetMergeCommitsBetween walks the commit log and returns ordered merge commits from the commit after start, to
// the commit equal to end.
func GetMergeCommitsBetween(ctx context.Context, db *doltdb.DoltDB, start, end hash.Hash) ([]*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(end.String())

	if err != nil {
		return nil, err
	}

	optCmt, err := db.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitRuntimeFailure
	}

	return GetMergeCommitsAfter(ctx, db, cm, start)
}

// gets the lists of tables that are scored.  This filters out any tables with the prefix dolt_
func GetScoredTables(ctx context.Context, additonalNames []string, root *doltdb.RootValue) ([]string, error) {
	tableNames, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	unique := set.NewStrSet(tableNames)
	unique.Add(additonalNames...)

	scoredTables := make([]string, 0, unique.Size())
	unique.Iterate(func(s string) (cont bool) {
		if !strings.HasPrefix(strings.ToLower(s), "dolt_") {
			scoredTables = append(scoredTables, s)
		}

		return true
	})

	return scoredTables, nil
}
