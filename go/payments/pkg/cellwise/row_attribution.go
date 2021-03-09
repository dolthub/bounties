package cellwise

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/bounties/go/payments/pkg/doltutils/differs"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	initialTags          = 2048
	maxPastValuesInitial = 1024
)

type rowAttEncodingBuffers struct {
	tags     []uint64
	rowVals  []types.Value
	cellVals []types.Value
}

func NewRowAttEncodingBuffers() *rowAttEncodingBuffers {
	buffs := &rowAttEncodingBuffers{
		tags:     make([]uint64, initialTags),
		rowVals:  make([]types.Value, initialTags*2),
		cellVals: make([]types.Value, (2*maxPastValuesInitial)+1),
	}

	buffs.reset()
	return buffs
}

func (buffs *rowAttEncodingBuffers) reset() {
	buffs.tags = buffs.tags[:0]
	buffs.rowVals = buffs.rowVals[:0]
	buffs.cellVals = buffs.cellVals[:0]
}

type rowAtt map[uint64]*cellAtt

func newRowAtt() rowAtt {
	return make(rowAtt)
}

func (ra rowAtt) AsValue(nbf *types.NomsBinFormat, buffs *rowAttEncodingBuffers) (types.Value, error) {
	for tag := range ra {
		buffs.tags = append(buffs.tags, tag)
	}

	sort.Slice(buffs.tags, func(i, j int) bool {
		return buffs.tags[i] < buffs.tags[j]
	})

	for _, tag := range buffs.tags {
		buffs.rowVals = append(buffs.rowVals, types.Uint(tag))
		cellVal, err := ra[tag].AsValue(nbf, buffs)

		if err != nil {
			return nil, err
		}

		buffs.rowVals = append(buffs.rowVals, cellVal)
	}

	t, err := types.NewTuple(nbf, buffs.rowVals...)

	if err != nil {
		return nil, err
	}

	buffs.reset()
	return t, nil
}

func rowAttFromValue(v types.Value) (rowAtt, error) {
	t := v.(types.Tuple)
	itr, err := t.Iterator()

	if err != nil {
		return nil, err
	}

	ra := make(rowAtt)
	for itr.HasMore() {
		_, tag, err := itr.NextUint64()

		if err != nil {
			return nil, err
		}

		_, val, err := itr.Next()

		if err != nil {
			return nil, err
		}

		ca, err := cellAttFromValue(val)

		if err != nil {
			return nil, err
		}

		ra[tag] = ca
	}

	return ra, nil
}

func (ra rowAtt) updateFromDiff(nbf *types.NomsBinFormat, sch schema.Schema, commitIdx int16, difference *diff.Difference) error {
	tvd, err := differs.NewTaggedValDiff(sch, difference)

	if err != nil {
		return err
	}

	tvd.FilterEqual()
	err = tvd.IterDiffs(func(tag uint64, old, new types.Value) error {
		if types.IsNull(new) {
			att, ok := ra[tag]
			if ok {
				prevOwner := att.CurrentOwner

				if prevOwner == -1 {
					return nil
				} else {
					return att.Delete(nbf, old)
				}
			}
		} else {
			att, ok := ra[tag]

			if !ok {
				ra[tag] = newCellAtt(commitIdx)
			} else {
				_, _, err := att.Update(nbf, commitIdx, old, new)

				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

func (ra rowAtt) String() string {
	tags := make([]uint64, 0, len(ra))
	for tag := range ra {
		tags = append(tags, tag)
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i] < tags[j]
	})

	sb := strings.Builder{}

	for _, tag := range tags {
		sb.WriteString(fmt.Sprintf("%d: %d, ", tag, ra[tag].CurrentOwner))
	}

	return sb.String()
}
