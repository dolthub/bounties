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
	"context"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

var _ Differ = (*MapRowsAsDiffs)(nil)

// MapRowsAsDiffs takes a map of rows and provides a Differ interface to it's data.  Where each row returned is either
// returned as an added row, or a deleted row
type MapRowsAsDiffs struct {
	ctx        context.Context
	changeType types.DiffChangeType
	rd         *noms.NomsRangeReader
}

// NewMapRowsAsDiffs returns a MapRowsAsDiffs object
func NewMapRowsAsDiffs(ctx context.Context, vr types.ValueReader, sch schema.Schema, nbf *types.NomsBinFormat, changeType types.DiffChangeType, m types.Map, startInclusive, endExclusive types.Value) *MapRowsAsDiffs {
	var readRange *noms.ReadRange
	if !types.IsNull(startInclusive) && !types.IsNull(endExclusive) {
		readRange = noms.NewRangeStartingAt(startInclusive.(types.Tuple), inRangeCheckLess{endExclusive})
	} else if !types.IsNull(startInclusive) {
		readRange = noms.NewRangeStartingAt(startInclusive.(types.Tuple), noms.InRangeCheckAlways{})
	} else if !types.IsNull(endExclusive) {
		readRange = noms.NewRangeEndingBefore(endExclusive.(types.Tuple), noms.InRangeCheckAlways{})
	} else {
		// range of all values.
		readRange = &noms.ReadRange{
			Start:     types.EmptyTuple(nbf),
			Inclusive: true,
			Reverse:   false,
			Check:     noms.InRangeCheckAlways{},
		}
	}

	rd := noms.NewNomsRangeReader(vr, sch, m, []*noms.ReadRange{readRange})
	return &MapRowsAsDiffs{
		ctx:        ctx,
		changeType: changeType,
		rd:         rd,
	}
}

// Close cleans up any resources
func (m *MapRowsAsDiffs) Close() error {
	return m.rd.Close(m.ctx)
}

// GetDiffs gets up to the specified number of diffs.  A timeout can be specified and if the requested number of diffs
// are not available it will return what is available.  A timeout of 0 returns what is immediately available without waiting.
// a timeout of -1 will wait indefinitely until the number of diffs are available, or it can return all remaining diffs
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

// inRangeCheckLess check that a given tuple is in range by comparing it to the enclosed value.
type inRangeCheckLess struct {
	val types.Value
}

var _ noms.InRangeCheck = inRangeCheckLess{}

// Check implements the interface noms.InRangeCheck.
func (i inRangeCheckLess) Check(ctx context.Context, vr types.ValueReader, tuple types.Tuple) (valid bool, skip bool, err error) {
	ok, err := tuple.Less(ctx, vr.Format(), i.val)
	return ok, false, err
}
