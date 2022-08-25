package doltutils

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
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

// NewDiffIter creates a ProllyDiffIter that iterates all diffs between |from|
// and |to|.
func NewDiffIter(ctx context.Context, from, to prolly.Map) (ProllyDiffIter, error) {
	childCtx, cancel := context.WithCancel(ctx)
	iter := prollyDiffIter{
		diffs:   make(chan tree.Diff, 64),
		errChan: make(chan error),
		cancel:  cancel,
	}

	go func() {
		iter.queueRows(childCtx, from, to)
	}()

	return iter, nil
}

// NewDiffIterRange creates a ProllyDiffIter that iterates only the diffs
// between |from| and |to| which are also within |rng|.
func NewDiffIterRange(ctx context.Context, from, to prolly.Map, rng prolly.Range) (ProllyDiffIter, error) {
	childCtx, cancel := context.WithCancel(ctx)
	iter := prollyDiffIter{
		diffs:   make(chan tree.Diff, 64),
		errChan: make(chan error),
		cancel:  cancel,
	}

	go func() {
		iter.queueRowsInRange(childCtx, from, to, rng)
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

func (itr prollyDiffIter) queueRows(ctx context.Context, from, to prolly.Map) {
	err := prolly.DiffMaps(ctx, from, to, func(ctx context.Context, diff tree.Diff) error {
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

func (itr prollyDiffIter) queueRowsInRange(ctx context.Context, from, to prolly.Map, rng prolly.Range) {
	err := prolly.RangeDiffMaps(ctx, from, to, rng, func(ctx context.Context, diff tree.Diff) error {
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
