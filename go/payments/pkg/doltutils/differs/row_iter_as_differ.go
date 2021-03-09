package differs

import (
	"context"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

var _ Differ = (*MapRowsAsDiffs)(nil)

type MapRowsAsDiffs struct {
	ctx        context.Context
	changeType types.DiffChangeType
	rd         *noms.NomsRangeReader
}

func NewMapRowsAsDiffs(ctx context.Context, nbf *types.NomsBinFormat, changeType types.DiffChangeType, m types.Map, startInclusive, endExclusive types.Value) *MapRowsAsDiffs {
	var readRange *noms.ReadRange
	if !types.IsNull(startInclusive) && !types.IsNull(endExclusive) {
		readRange = noms.NewRangeStartingAt(startInclusive.(types.Tuple), func(tuple types.Tuple) (bool, error) {
			return tuple.Less(nbf, endExclusive)
		})
	} else if !types.IsNull(startInclusive) {
		readRange = noms.NewRangeStartingAt(startInclusive.(types.Tuple), func(tuple types.Tuple) (bool, error) {
			return true, nil
		})
	} else if !types.IsNull(endExclusive) {
		readRange = noms.NewRangeEndingBefore(endExclusive.(types.Tuple), func(tuple types.Tuple) (bool, error) {
			return true, nil
		})
	} else {
		// range of all values.
		readRange = &noms.ReadRange{
			Start:     types.EmptyTuple(nbf),
			Inclusive: true,
			Reverse:   false,
			Check: func(tuple types.Tuple) (bool, error) {
				return true, nil
			},
		}
	}

	rd := noms.NewNomsRangeReader(nil, m, []*noms.ReadRange{readRange})
	return &MapRowsAsDiffs{
		ctx:        ctx,
		changeType: changeType,
		rd:         rd,
	}
}

func (m *MapRowsAsDiffs) Close() error {
	return m.rd.Close(m.ctx)
}

func (m *MapRowsAsDiffs) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error) {
	diffs := make([]*diff.Difference, 0, numDiffs)

	for i := 0; i < numDiffs; i++ {
		k, v, err := m.rd.ReadKV(m.ctx)

		if err != nil {
			return nil, false, err
		}

		if m.changeType == types.DiffChangeAdded {
			diffs = append(diffs, &diff.Difference{
				ChangeType: m.changeType,
				NewValue:   v,
				KeyValue:   k,
			})
		} else {
			diffs = append(diffs, &diff.Difference{
				ChangeType: m.changeType,
				OldValue:   v,
				KeyValue:   k,
			})
		}
	}

	return diffs, len(diffs) > 0, nil
}
