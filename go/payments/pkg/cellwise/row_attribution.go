package cellwise

import (
	"fmt"
	"github.com/dolthub/dolt/go/store/hash"
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
	tags    []uint64
	rowVals []types.Value
}

func NewRowAttEncodingBuffers() *rowAttEncodingBuffers {
	buffs := &rowAttEncodingBuffers{
		tags:    make([]uint64, initialTags),
		rowVals: make([]types.Value, initialTags*8),
	}

	buffs.reset()
	return buffs
}

func (buffs *rowAttEncodingBuffers) reset() {
	buffs.tags = buffs.tags[:0]
	buffs.rowVals = buffs.rowVals[:0]
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
		ca := ra[tag]

		buffs.rowVals = append(buffs.rowVals, types.Uint(tag))
		buffs.rowVals = append(buffs.rowVals, types.Int(ca.CurrentOwner))
		buffs.rowVals = append(buffs.rowVals, types.Uint(len(ca.PastValues)))

		for h, n := range ca.PastValues {
			hCopy := h
			buffs.rowVals = append(buffs.rowVals, types.InlineBlob(hCopy[:]))
			buffs.rowVals = append(buffs.rowVals, types.Int(n))
		}
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

		_, currOwnerVal, err := itr.Next()
		if err != nil {
			return nil, err
		}

		_, pastValCount, err := itr.NextUint64()
		if err != nil {
			return nil, err
		}

		var pastVals map[hash.Hash]int16
		if pastValCount > 0 {
			pastVals = make(map[hash.Hash]int16, pastValCount)
			for i := uint64(0); i < pastValCount; i++ {
				_, hashBlobVal, err := itr.Next()

				if err != nil {
					return nil, err
				}

				hashBlob := hashBlobVal.(types.InlineBlob)
				h := hash.New(hashBlob)

				_, prevOwnerVal, err := itr.Next()

				if err != nil {
					return nil, err
				}

				pastVals[h] = int16(prevOwnerVal.(types.Int))
			}
		}

		ra[tag] = &cellAtt{
			CurrentOwner: int16(currOwnerVal.(types.Int)),
			PastValues:   pastVals,
		}
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
