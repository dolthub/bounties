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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package cellwise

import (
	"context"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
	"io"
	"math"
	"time"
)

type kv struct {
	key   types.Value
	value types.Value
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

// Differ is an interface written to match what is already provided by the Dolt github.com/dolthub/dolt/go/libraries/doltcore/diff.AsyncDiffer
type Differ interface {
	// Start initializes the iterator to iterate over 2 maps
	Start(ctx context.Context, from, to types.Map)
	// Close cleans up any resources
	Close() error
	// are not available it will return what is available.  A timeout of 0 returns what is immediately available without waiting.
	// a timeout of -1 will wait indefinitely until the number of diffs are available, or it can return all remaining diffs
	GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error)
}

// dualMapIter is a Differ implementation which will returns every row.  We use this to iterate over every row in the
// case of a schema change
type dualMapIter struct {
	ctx     context.Context
	nbf     *types.NomsBinFormat
	isDone  bool
	mItrs   [2]types.MapIterator
	current [2]*kv
}

// Start initializes the iterator to iterate over 2 maps
func (itr *dualMapIter) Start(ctx context.Context, from, to types.Map) {
	itr.ctx = ctx
	itr.nbf = from.Format()
	fromItr, err := from.Iterator(ctx)

	if err != nil {
		panic(err)
	}

	toItr, err := to.Iterator(ctx)

	if err != nil {
		panic(err)
	}

	itr.mItrs = [2]types.MapIterator{fromItr, toItr}
}

// Close cleans up any resources
func (itr *dualMapIter) Close() error {
	return nil
}

// GetDiffs gets up to the specified number of diffs.  A timeout can be specified and if the requested number of diffs
// are not available it will return what is available.  A timeout of 0 returns what is immediately available without waiting.
// a timeout of -1 will wait indefinitely until the number of diffs are available, or it can return all remaining diffs
func (itr *dualMapIter) GetDiffs(numDiffs int, _ time.Duration) ([]*diff.Difference, bool, error) {
	if numDiffs == 0 {
		numDiffs = math.MaxInt32
	}

	var results []*diff.Difference
	for i := 0; i < numDiffs; i++ {
		diff, err := itr.getDiff()

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

func (itr *dualMapIter) getDiff() (*diff.Difference, error) {
	// update the our current key / value pairs when necessary
	for i := 0; i < 2; i++ {
		if itr.current[i] == nil {
			key, val, err := itr.mItrs[i].Next(itr.ctx)

			if err != nil && err != io.EOF {
				return nil, err
			}

			if err != io.EOF && key != nil {
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
		isLess, err := itr.current[0].key.Less(itr.nbf, itr.current[1].key)
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
