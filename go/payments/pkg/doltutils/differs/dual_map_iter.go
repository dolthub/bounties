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

package differs

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

type kv struct {
	key   types.Tuple
	value types.Tuple
}

func kvDiff(from, to *kv) (*diff.Difference, error) {
	if to == nil {
		return &diff.Difference{
			ChangeType: types.DiffChangeRemoved,
			OldValue:   from.value,
			KeyValue:   from.key,
		}, nil
	}

	if from == nil {
		return &diff.Difference{
			ChangeType: types.DiffChangeAdded,
			NewValue:   to.value,
			KeyValue:   to.key,
		}, nil
	}

	return &diff.Difference{
		ChangeType: types.DiffChangeModified,
		OldValue:   from.value,
		NewValue:   to.value,
		KeyValue:   from.key,
	}, nil
}

// DualMapIter is a Differ implementation which will returns every row.  We use this to iterate over every row in the
// case of a schema change
type DualMapIter struct {
	ctx     context.Context
	nbf     *types.NomsBinFormat
	isDone  bool
	mRdrs   [2]*noms.NomsRangeReader
	current [2]*kv
}

// Start initializes the iterator to iterate over 2 maps
func (itr *DualMapIter) Start(ctx context.Context, fromVR, toVR types.ValueReader, fromSch, toSch schema.Schema, from, to types.Map, start types.Value, inRange func(context.Context, types.Value) (bool, bool, error)) {
	itr.ctx = ctx
	itr.nbf = from.Format()

	startTuple := types.EmptyTuple(itr.nbf)
	if !types.IsNull(start) {
		startTuple = start.(types.Tuple)
	}

	readRange := &noms.ReadRange{
		Start:     startTuple,
		Inclusive: true,
		Reverse:   false,
		Check:     inRangeWrapper(inRange),
	}

	fromReader := noms.NewNomsRangeReader(fromVR, fromSch, from, []*noms.ReadRange{readRange})
	toReader := noms.NewNomsRangeReader(toVR, toSch, to, []*noms.ReadRange{readRange})

	itr.mRdrs = [2]*noms.NomsRangeReader{fromReader, toReader}
}

// Close cleans up any resources
func (itr *DualMapIter) Close() error {
	return nil
}

// GetDiffs gets up to the specified number of diffs.  A timeout can be specified and if the requested number of diffs
// are not available it will return what is available.  A timeout of 0 returns what is immediately available without waiting.
// a timeout of -1 will wait indefinitely until the number of diffs are available, or it can return all remaining diffs
func (itr *DualMapIter) GetDiffs(numDiffs int, _ time.Duration) ([]*diff.Difference, bool, error) {
	if numDiffs == 0 {
		numDiffs = math.MaxInt32
	}

	var results []*diff.Difference
	for i := 0; i < numDiffs; i++ {
		diff, err := itr.getDiff(itr.ctx)

		if err != nil {
			return nil, false, err
		}

		if diff == nil {
			break
		}

		results = append(results, diff)
	}

	return results, len(results) > 0, nil
}

func (itr *DualMapIter) getDiff(ctx context.Context) (*diff.Difference, error) {
	// update the our current key / value pairs when necessary
	for i := 0; i < 2; i++ {
		if itr.current[i] == nil {
			key, val, err := itr.mRdrs[i].ReadKV(itr.ctx)

			if err != nil && err != io.EOF {
				return nil, err
			}

			if err != io.EOF {
				itr.current[i] = &kv{key, val}
			}
		}
	}

	if itr.current[0] == nil && itr.current[1] == nil {
		// If both kvps are nil then we're done
		itr.isDone = true
		// matches behavior of AsyncDiffer (which should be fixed to use io.EOF)
		return nil, nil
	} else if itr.current[0] == nil {
		// one kvp is nil so keep returning the other until this map is exhausted
		toKV := itr.current[1]
		itr.current[1] = nil
		return kvDiff(nil, toKV)
	} else if itr.current[1] == nil {
		// one kvp is nil so keep returning the other until this map is exhausted
		fromKV := itr.current[0]
		itr.current[0] = nil
		return kvDiff(fromKV, nil)
	}

	if itr.current[0].key.Equals(itr.current[1].key) {
		// keys for both maps are the same.  return the kvps as differences
		fromKV, toKV := itr.current[0], itr.current[1]
		itr.current[0], itr.current[1] = nil, nil
		return kvDiff(fromKV, toKV)
	} else {
		// not the same key, so return the lesser kvp
		isLess, err := itr.current[0].key.Less(ctx, itr.nbf, itr.current[1].key)
		if err != nil {
			return nil, err
		}

		if isLess {
			fromKV := itr.current[0]
			itr.current[0] = nil
			return kvDiff(fromKV, nil)
		} else {
			toKV := itr.current[1]
			itr.current[1] = nil
			return kvDiff(nil, toKV)
		}
	}
}

// inRangeWrapper wraps an "inRange" function so that it may be used with the interface change on noms.ReadRange.
type inRangeWrapper func(context.Context, types.Value) (bool, bool, error)

var _ noms.InRangeCheck = inRangeWrapper(nil)

// Check implements the interface noms.InRangeCheck.
func (i inRangeWrapper) Check(ctx context.Context, vr types.ValueReader, tuple types.Tuple) (valid bool, skip bool, err error) {
	ok, _, err := i(ctx, tuple)
	return ok, false, err
}
