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

// TaggedValDiff is a map from tags to the old and new values of the cells with those tags.
type TaggedValDiff map[uint64][2]types.Value

// NewTaggedValDiff takes a Difference object and returns a TaggedValDiff object
func NewTaggedValDiff(sch schema.Schema, difference *diff.Difference) (TaggedValDiff, error) {
	tv := make(TaggedValDiff)

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

// FilterEquals filters out tags where the old and new values are equal
func (tvd TaggedValDiff) FilterEqual() {
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

func (tvd TaggedValDiff) setOld(tag uint64, val types.Value) error {
	if oldAndNew, ok := tvd[tag]; ok {
		oldAndNew[OldValIndex] = val
		tvd[tag] = oldAndNew
	} else {
		tvd[tag] = [2]types.Value{val, nil}
	}

	return nil
}

func (tvd TaggedValDiff) setNew(tag uint64, val types.Value) error {
	if oldAndNew, ok := tvd[tag]; ok {
		oldAndNew[NewValIndex] = val
		tvd[tag] = oldAndNew
	} else {
		tvd[tag] = [2]types.Value{nil, val}
	}

	return nil
}

func (tvd TaggedValDiff) setOldAndNew(tag uint64, val types.Value) error {
	tvd[tag] = [2]types.Value{val, val}
	return nil
}

// IterDiffs iterates over a TaggedValDiff object calling the cb for each tag and paassing in the tag along with the
// old and new values for each cell.
func (tvd TaggedValDiff) IterDiffs(cb func(tag uint64, old, new types.Value) error) error {
	for tag, oldAndNew := range tvd {
		err := cb(tag, oldAndNew[OldValIndex], oldAndNew[NewValIndex])

		if err != nil {
			return err
		}
	}

	return nil
}
