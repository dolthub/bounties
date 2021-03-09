package differs

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	OldValIndex = 0
	NewValIndex = 1
)

func filteredCBFunc(sch schema.Schema, f func(tag uint64, val types.Value) error) func(tag uint64, val types.Value) error {
	schTags := sch.GetAllCols().TagToIdx
	return func(tag uint64, val types.Value) error {
		if _, ok := schTags[tag]; ok {
			return f(tag, val)
		}
		return nil
	}
}

type taggedValDiff map[uint64][2]types.Value

func NewTaggedValDiff(sch schema.Schema, difference *diff.Difference) (taggedValDiff, error) {
	tv := make(taggedValDiff)

	switch difference.ChangeType {
	case types.DiffChangeRemoved:
		err := row.IterDoltTuple(difference.KeyValue.(types.Tuple), tv.setOld)
		if err != nil {
			return nil, err
		}

		err = row.IterDoltTuple(difference.OldValue.(types.Tuple), tv.setOld)
		if err != nil {
			return nil, err
		}

	case types.DiffChangeAdded:
		filteredSetNew := filteredCBFunc(sch, tv.setNew)
		err := row.IterDoltTuple(difference.KeyValue.(types.Tuple), filteredSetNew)
		if err != nil {
			return nil, err
		}

		err = row.IterDoltTuple(difference.NewValue.(types.Tuple), filteredSetNew)
		if err != nil {
			return nil, err
		}

	case types.DiffChangeModified:
		err := row.IterDoltTuple(difference.KeyValue.(types.Tuple), tv.setOldAndNew)
		if err != nil {
			return nil, err
		}

		err = row.IterDoltTuple(difference.OldValue.(types.Tuple), tv.setOld)
		if err != nil {
			return nil, err
		}

		filteredSetNew := filteredCBFunc(sch, tv.setNew)
		err = row.IterDoltTuple(difference.NewValue.(types.Tuple), filteredSetNew)
		if err != nil {
			return nil, err
		}
	}

	return tv, nil
}

func (tvd taggedValDiff) FilterEqual() {
	toRemove := make([]uint64, 0, len(tvd))
	for tag, oldAndNew := range tvd {
		oldVal := oldAndNew[OldValIndex]
		newVal := oldAndNew[NewValIndex]

		var equal bool
		if types.IsNull(oldVal) {
			equal = types.IsNull(newVal)
		} else {
			equal = oldVal.Equals(newVal)
		}

		if equal {
			toRemove = append(toRemove, tag)
		}
	}

	for _, tag := range toRemove {
		delete(tvd, tag)
	}
}

func (tvd taggedValDiff) setOld(tag uint64, val types.Value) error {
	if oldAndNew, ok := tvd[tag]; ok {
		oldAndNew[OldValIndex] = val
		tvd[tag] = oldAndNew
	} else {
		tvd[tag] = [2]types.Value{val, nil}
	}

	return nil
}

func (tvd taggedValDiff) setNew(tag uint64, val types.Value) error {
	if oldAndNew, ok := tvd[tag]; ok {
		oldAndNew[NewValIndex] = val
		tvd[tag] = oldAndNew
	} else {
		tvd[tag] = [2]types.Value{nil, val}
	}

	return nil
}

func (tvd taggedValDiff) setOldAndNew(tag uint64, val types.Value) error {
	tvd[tag] = [2]types.Value{val, val}
	return nil
}

func (tvd taggedValDiff) IterDiffs(cb func(tag uint64, old, new types.Value) error) error {
	for tag, oldAndNew := range tvd {
		err := cb(tag, oldAndNew[OldValIndex], oldAndNew[NewValIndex])

		if err != nil {
			return err
		}
	}

	return nil
}
