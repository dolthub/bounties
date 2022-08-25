package prolly_cellwise

import (
	"context"
	"io"

	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"go.uber.org/zap"
)

type diffAttributor struct {
	tblSch           schema.Schema
	table, prevTable *doltdb.Table
	commitIdx        int16
	shardMgr         *prollyShardManager
	shard            AttributionShard
	logger           *zap.Logger
	format           *types.NomsBinFormat
	kd               val.TupleDesc
	attVd            val.TupleDesc

	newData       int
	unchangedData int
	modifiedData  int
	total         int
}

func newDiffAttributor(
	ctx context.Context,
	table, prevTable *doltdb.Table,
	commitIdx int16,
	shardMgr *prollyShardManager,
	shard AttributionShard,
	logger *zap.Logger) (*diffAttributor, error) {

	if table == nil && prevTable == nil {
		panic("|table| and |prevTable| were both nil")
	}

	var format *types.NomsBinFormat
	var tblSch schema.Schema
	var err error
	if table != nil {
		format = table.Format()
		tblSch, err = table.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		format = prevTable.Format()
		tblSch, err = prevTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	kd, attVd := getAttribDescriptorsFromTblSchema(tblSch)

	return &diffAttributor{
		tblSch:    tblSch,
		table:     table,
		prevTable: prevTable,
		commitIdx: commitIdx,
		shardMgr:  shardMgr,
		shard:     shard,
		logger:    logger,
		format:    format,
		kd:        kd,
		attVd:     attVd,
	}, nil
}

func (dA *diffAttributor) attributeDiffs(ctx context.Context, diffIter doltutils.ProllyDiffIter, attItr prolly.MapIter) error {
	defer dA.incAndLog(nil)

	attKey, attVal, err := attItr.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}

	diff, err := diffIter.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}

	for len(diff.Key) != 0 && len(attKey) != 0 {
		cmp := dA.kd.Compare(val.Tuple(diff.Key), attKey)

		switch {
		case cmp < 0:
			// Don't have an attribution for this diff (Add one)
			err = dA.createAttrib(ctx, diff)
			if err != nil {
				return err
			}

			diff, err = diffIter.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}

		case cmp > 0:
			// No diff for this attribution (no-op)
			err = dA.copyAttrib(ctx, attKey, attVal)
			if err != nil {
				return err
			}

			attKey, attVal, err = attItr.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}

		case cmp == 0:
			// A previous attribution was found for this diff
			err = dA.updateAttrib(ctx, diff, attVal)
			if err != nil {
				return err
			}

			diff, err = diffIter.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}

			attKey, attVal, err = attItr.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}
		}
	}

	for len(diff.Key) != 0 {
		dA.incAndLog(&dA.newData)
		err = dA.createAttrib(ctx, diff)
		if err != nil {
			return err
		}
		diff, err = diffIter.Next(ctx)
		if err != nil && err != io.EOF {
			return err
		}
	}

	for len(attKey) != 0 {
		dA.incAndLog(&dA.unchangedData)
		err = dA.copyAttrib(ctx, attKey, attVal)
		if err != nil {
			return err
		}
		attKey, attVal, err = attItr.Next(ctx)
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

func (dA *diffAttributor) createAttrib(ctx context.Context, diff tree.Diff) error {
	dA.incAndLog(&dA.newData)

	if diff.Type == tree.RemovedDiff {
		return nil
	}

	ra := make(prollyRowAtt, dA.attVd.Count())
	err := ra.updateFromDiff(dA.kd, dA.commitIdx, diff)
	if err != nil {
		return err
	}
	return dA.shardMgr.addRowAtt(ctx, val.Tuple(diff.Key), ra, nil)
}

func (dA *diffAttributor) copyAttrib(ctx context.Context, attKey, attVal val.Tuple) error {
	dA.incAndLog(&dA.unchangedData)

	ra := make(prollyRowAtt, dA.attVd.Count())
	for i := 0; i < dA.attVd.Count(); i++ {
		v, ok := dA.attVd.GetInt16(i, attVal)
		if ok {
			ra[i] = &v
		}
	}

	return dA.shardMgr.addRowAtt(ctx, attKey, ra, attVal)
}

func (dA *diffAttributor) updateAttrib(ctx context.Context, diff tree.Diff, attVal val.Tuple) error {
	dA.incAndLog(&dA.modifiedData)

	if diff.Type == tree.RemovedDiff {
		// Don't send an attribution to the output shard if a row was deleted
		return nil
	}

	ra := prollyRowAttFromValue(dA.attVd, attVal)

	err := ra.updateFromDiff(dA.kd, dA.commitIdx, diff)
	if err != nil {
		return err
	}

	return dA.shardMgr.addRowAtt(ctx, val.Tuple(diff.Key), ra, nil)
}

func (dA *diffAttributor) incAndLog(p *int) {
	if p != nil {
		dA.total++
		*p = *p + 1
	}

	if p == nil || dA.total%50_000 == 0 {
		dA.logger.Info("attribution update",
			zap.Int("new", dA.newData),
			zap.Int("unchanged", dA.unchangedData),
			zap.Int("modified", dA.modifiedData))
	}
}
