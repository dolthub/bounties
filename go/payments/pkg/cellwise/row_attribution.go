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

type rowAtt map[uint64]*cellAtt

func newRowAtt() rowAtt {
	return make(rowAtt)
}

func (ra rowAtt) AsValue(nbf *types.NomsBinFormat) (types.Value, error) {
	tags := make([]uint64, 0, len(ra))
	for tag := range ra {
		tags = append(tags, tag)
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i] < tags[j]
	})

	var err error
	vals := make([]types.Value, len(ra)*2)
	for i, tag := range tags {
		vals[i*2] = types.Uint(tag)
		vals[i*2+1], err = ra[tag].AsValue(nbf)

		if err != nil {
			return nil, err
		}
	}

	return types.NewTuple(nbf, vals...)
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
