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

type rowsToDiffIter struct {
	itr      prolly.MapIter
	diffType tree.DiffType
}

// NewRowAsDiffIter returns a |ProllyDiffIter| that returns a diff for every
// key-value pair in |m|. The returned diff has the same type as |diffType|.
// tree.ModifiedDiff is not supported.
func NewRowAsDiffIter(ctx context.Context, m prolly.Map, diffType tree.DiffType) (ProllyDiffIter, error) {
	if diffType == tree.ModifiedDiff {
		panic("tree.ModifiedDiff not supported")
	}

	itr, err := m.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	return &rowsToDiffIter{itr: itr, diffType: diffType}, nil
}

// NewRowAsDiffIterRange is similar to NewRowAsDiffIter except that it only
// considers rows within |rng|.
func NewRowAsDiffIterRange(ctx context.Context, m prolly.Map, rng prolly.Range, diffType tree.DiffType) (ProllyDiffIter, error) {
	if diffType == tree.ModifiedDiff {
		panic("tree.ModifiedDiff not supported")
	}

	itr, err := m.IterRange(ctx, rng)
	if err != nil {
		return nil, err
	}
	return &rowsToDiffIter{itr: itr, diffType: diffType}, nil
}

// Next implements ProllyDiffIter.
func (itr *rowsToDiffIter) Next(ctx context.Context) (tree.Diff, error) {
	k, v, err := itr.itr.Next(ctx)
	if err != nil {
		return tree.Diff{}, err
	}

	var d tree.Diff
	if itr.diffType == tree.AddedDiff {
		d = tree.Diff{
			Key:  tree.Item(k),
			From: nil,
			To:   tree.Item(v),
			Type: tree.AddedDiff,
		}
	} else {
		d = tree.Diff{
			Key:  tree.Item(k),
			From: tree.Item(v),
			To:   nil,
			Type: tree.RemovedDiff,
		}
	}

	return d, nil
}

// Close implements ProllyDiffIter.
func (itr *rowsToDiffIter) Close(ctx context.Context) error {
	return nil
}
