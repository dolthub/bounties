package cellwise

import (
	"context"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"time"
)

// TableAttribution stores metrics and ownership details for every change made during the attribution period for a single
// table.  The object is mutated by the updateAttribution method which takes two root values and updates attribution
// based on the differences in the table
type TableAttribution struct {
	pkHashToTagToCellAtt map[hash.Hash]map[uint64]*cellAtt
	// CommitToCellCount stores the number of cell changes attributed to a specific commit index
	CommitToCellCount    map[int16]int
	// AttributedCells is the total number of cell changes that have occurred during the attribution period
	AttributedCells      int64
}

func newTableCellAttribution() *TableAttribution {
	return &TableAttribution{
		pkHashToTagToCellAtt: make(map[hash.Hash]map[uint64]*cellAtt),
		CommitToCellCount:    make(map[int16]int),
		AttributedCells:      0,
	}
}

// updateAttribution mutates the TableAttribution object updating it's state based on the changes that have occurred
// between parentRoot and root
func (tblAtt *TableAttribution) updateAttribution(ctx context.Context, parentRoot, root *doltdb.RootValue, table string, commitIdx int16) error {
	// Get table rows from parent and current table
	parentRows, parentSch, err := doltutils.GetRows(ctx, parentRoot, table)

	if err != nil {
		return err
	}

	rows, sch, err := doltutils.GetRows(ctx, root, table)

	if err != nil {
		return err
	}

	nbf := parentRoot.VRW().Format()
	eqSchemas, err := schema.SchemasAreEqual(parentSch, sch)

	if err != nil {
		return err
	}

	// iterate over the diffs
	var differ Differ
	if eqSchemas {
		differ = diff.NewAsyncDiffer(32)
	} else {
		// If the schema has changed this will iterate over all rows
		differ = &dualMapIter{}
	}

	differ.Start(ctx, parentRows, rows)
	defer differ.Close()

	for {
		diffs, err := differ.GetDiffs(1, time.Second)

		if err != nil {
			return err
		}

		if differ.IsDone() {
			break
		}

		d := diffs[0]
		pkHash, err := d.KeyValue.Hash(rows.Format())

		if err != nil {
			return err
		}

		var currRow row.Row
		var parentRow row.Row
		if d.OldValue != nil {
			parentRow, err = row.FromNoms(parentSch, d.KeyValue.(types.Tuple), d.OldValue.(types.Tuple))

			if err != nil {
				return err
			}
		}

		if d.NewValue != nil {
			currRow, err = row.FromNoms(sch, d.KeyValue.(types.Tuple), d.NewValue.(types.Tuple))

			if err != nil {
				return err
			}
		}

		if currRow == nil {
			err = tblAtt.processDeletion(nbf, pkHash, parentRow)
		} else {
			err = tblAtt.processInsertOrUpdate(nbf, commitIdx, pkHash, currRow, parentRow)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (tblAtt *TableAttribution) processInsertOrUpdate(nbf *types.NomsBinFormat, commitIdx int16, pkHash hash.Hash, currRow, oldRow row.Row) error {
	tagToCellAtt := tblAtt.pkHashToTagToCellAtt[pkHash]

	if tagToCellAtt == nil {
		tagToCellAtt = make(map[uint64]*cellAtt)
	}

	// iterate over the values in the new row and update attribution for each cell
	handled := make(map[uint64]bool)
	_, err := currRow.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		handled[tag] = true
		if !types.IsNull(val) {
			var oldVal types.Value
			if oldRow != nil {
				oldVal, _ = oldRow.GetColVal(tag)
			}

			if val.Equals(oldVal) {
				return false, nil
			}

			attCommitIdx := commitIdx
			prevCommitIdx := int16(-1)
			cellState, ok := tagToCellAtt[tag]
			if !ok {
				cellState = newCellAtt(commitIdx)
			} else {
				var err error

				attCommitIdx, prevCommitIdx, err = cellState.Update(nbf, commitIdx, oldVal, val)

				if err != nil {
					return false, err
				}
			}

			if prevCommitIdx == -1 {
				tblAtt.AttributedCells++
			}

			if attCommitIdx != prevCommitIdx {
				if attCommitIdx != -1 {
					tblAtt.CommitToCellCount[attCommitIdx]++
				}

				if prevCommitIdx != -1 {
					tblAtt.CommitToCellCount[prevCommitIdx]--
				}
			}

			tagToCellAtt[tag] = cellState
		}

		return false, nil
	})

	if err != nil {
		return err
	}

	if oldRow != nil {
		// now iterate over all the values in the old row and call CellAtt.Delete on each column that wasn't in the new row
		_, err := oldRow.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
			if !handled[tag] && !types.IsNull(val) {
				cellState, ok := tagToCellAtt[tag]
				if ok {
					tblAtt.AttributedCells--
					tblAtt.CommitToCellCount[cellState.CurrentOwner]--
					err := cellState.Delete(nbf, val)

					if err != nil {
						return false, err
					}

					tagToCellAtt[tag] = cellState
				}
			}

			return false, nil
		})

		if err != nil {
			return err
		}
	}

	tblAtt.pkHashToTagToCellAtt[pkHash] = tagToCellAtt
	return nil
}

func (tblAtt *TableAttribution) processDeletion(nbf *types.NomsBinFormat, pkHash hash.Hash, r row.Row) error {
	tagToCellAtt, ok := tblAtt.pkHashToTagToCellAtt[pkHash]

	if ok {
		_, err := r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
			cellAtt, ok := tagToCellAtt[tag]
			if ok && cellAtt.CurrentOwner != -1 {
				tblAtt.AttributedCells--
				tblAtt.CommitToCellCount[cellAtt.CurrentOwner]--
				err := cellAtt.Delete(nbf, val)

				if err != nil {
					return false, err
				}
			}

			return false, nil
		})

		if err != nil {
			return  err
		}
	}

	return nil
}
