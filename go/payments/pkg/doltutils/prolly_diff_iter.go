// Copyright 2022 Dolthub, Inc.
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

package doltutils

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// ProllyDiffIter iterates over diffs.
type ProllyDiffIter interface {
	Next(ctx context.Context) (tree.Diff, error)
	Close(ctx context.Context) error
}

type prollyDiffIter struct {
	diffs   chan tree.Diff
	errChan chan error
	cancel  context.CancelFunc
}

// NewDiffIterKeyRange creates a ProllyDiffIter that iterates only the diffs
// between |from| and |to| which are also between |start| and |stop|.
func NewDiffIterKeyRange(ctx context.Context, from, to prolly.Map, start, stop val.Tuple) (ProllyDiffIter, error) {
	childCtx, cancel := context.WithCancel(ctx)
	iter := prollyDiffIter{
		diffs:   make(chan tree.Diff, 64),
		errChan: make(chan error),
		cancel:  cancel,
	}

	go func() {
		iter.queueRowsInKeyRange(childCtx, from, to, start, stop)
	}()

	return iter, nil
}

func (itr prollyDiffIter) Next(ctx context.Context) (tree.Diff, error) {
	select {
	case <-ctx.Done():
		return tree.Diff{}, ctx.Err()
	case err := <-itr.errChan:
		return tree.Diff{}, err
	case r, ok := <-itr.diffs:
		if !ok {
			return tree.Diff{}, io.EOF
		}
		return r, nil
	}
}

func (itr prollyDiffIter) Close(ctx context.Context) error {
	itr.cancel()
	return nil
}

func (itr prollyDiffIter) queueRowsInKeyRange(ctx context.Context, from, to prolly.Map, start, stop val.Tuple) {
	err := prolly.DiffMapsKeyRange(ctx, from, to, start, stop, func(ctx context.Context, diff tree.Diff) error {
		itr.diffs <- diff
		return nil
	})
	if err != nil && err != io.EOF {
		select {
		case <-ctx.Done():
		case itr.errChan <- err:
		}
		return
	}
	// we need to drain itr.rows before returning io.EOF
	close(itr.diffs)
}
