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

package cellwise

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
)

// DatabaseAttribution manages attribution of multiple tables across many commits.  Each call to update mutates the object
// adding a new commit and updating the attribution for the commit
type DatabaseAttribution struct {
	// AttribStartPoint is the commit hash at which point we began attribution
	AttribStartPoint hash.Hash
	// Commits are the ordered list of commits who are having their changes attributed
	Commits []hash.Hash
	// NameToTableAttribution is a map of table names to TableAttribution objects
	NameToTableAttribution map[string]*TableAttribution
}

// NewDatabaseAttribution returns a new DatabaseAttribution object
func NewDatabaseAttribution(start hash.Hash) *DatabaseAttribution {
	return &DatabaseAttribution{
		AttribStartPoint:       start,
		NameToTableAttribution: make(map[string]*TableAttribution),
	}
}

// Update mutates the DatabaseAttribution objects with new data based on the changes in the commit pointed to by the
// supplied commit hash and it's parent[0]
func (dbAtt *DatabaseAttribution) Update(ctx context.Context, ddb *doltdb.DoltDB, start, commitHash hash.Hash) error {
	if dbAtt.AttribStartPoint != start {
		return errors.New("The starting point for attribution has changed.  This needs to be re-run")
	}

	cm, parent, err := getCommitAndParentCommit(ctx, ddb, commitHash)

	if err != nil {
		return err
	}

	root, err := cm.GetRootValue()

	if err != nil {
		return err
	}

	parentRoot, err := parent.GetRootValue()

	if err != nil {
		return err
	}

	tables, err := allTables(ctx, root, parentRoot)

	if err != nil {
		return err
	}

	dbAtt.Commits = append(dbAtt.Commits, commitHash)
	commitIdx := int16(len(dbAtt.Commits) - 1)

	for _, table := range tables {
		tableAtt := dbAtt.NameToTableAttribution[table]
		if tableAtt == nil {
			tableAtt = newTableCellAttribution()
		}

		err := tableAtt.updateAttribution(ctx, parentRoot, root, table, commitIdx)
		if err != nil {
			return err
		}

		dbAtt.NameToTableAttribution[table] = tableAtt
	}

	return nil
}

// GetTableCounts returns a map of table name to map of commit hash to the number of changes that are attributed to that hash
func (dbAtt *DatabaseAttribution) GetTableCounts() map[string]map[hash.Hash]int {
	nameToCommitCounts := make(map[string]map[hash.Hash]int)
	for tbl, att := range dbAtt.NameToTableAttribution {
		commitToCounts := make(map[hash.Hash]int)
		for commitIdx, count := range att.CommitToCellCount {
			commitToCounts[dbAtt.Commits[commitIdx]] = count
		}

		nameToCommitCounts[tbl] = commitToCounts
	}

	return nameToCommitCounts
}

//GetCounts returns a map of commit hash to the number of changes that are attributed to that hash across all tables
func (dbAtt *DatabaseAttribution) GetCounts() map[hash.Hash]int {
	commitToCounts := make(map[hash.Hash]int)
	for _, att := range dbAtt.NameToTableAttribution {
		for commitIdx, count := range att.CommitToCellCount {
			commitToCounts[dbAtt.Commits[commitIdx]] += count
		}
	}

	for _, cm := range dbAtt.Commits {
		if _, ok := commitToCounts[cm]; !ok {
			commitToCounts[cm] = 0
		}
	}

	return commitToCounts
}

func getCommitAndParentCommit(ctx context.Context, ddb *doltdb.DoltDB, h hash.Hash) (*doltdb.Commit, *doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(h.String())

	if err != nil {
		return nil, nil, err
	}

	cm, err := ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, nil, err
	}

	parentHashes, err := cm.ParentHashes(ctx)

	if err != nil {
		return nil, nil, err
	}

	cs, err = doltdb.NewCommitSpec(parentHashes[0].String())

	if err != nil {
		return nil, nil, err
	}

	parent, err := ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, nil, err
	}

	fmt.Printf("commit: %s, parent: %s\n", h.String(), parentHashes[0].String())
	return cm, parent, nil
}

func allTables(ctx context.Context, roots ...*doltdb.RootValue) ([]string, error) {
	tables := set.NewStrSet(nil)
	for _, root := range roots {
		tablesForRoot, err := root.GetTableNames(ctx)

		if err != nil {
			return nil, err
		}

		tables.Add(tablesForRoot...)
	}

	return tables.AsSlice(), nil
}
