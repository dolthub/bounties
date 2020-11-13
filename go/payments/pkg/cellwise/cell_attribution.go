package cellwise

import (
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// cellAtt stores the current owner of the cell, and it's past values so if the value reverts to a previous value that
// it held sometime previously during the attribution period it will be attributed to the original commit
type cellAtt struct {
	// PastValues stores the past values that this cell has had during the attribution period
	PastValues map[hash.Hash]int16
	// CurrentOwner is the commit index of thee attribution period that is attributed with the change
	CurrentOwner int16
}

func newCellAtt(commitIdx int16) *cellAtt {
	return &cellAtt{CurrentOwner: commitIdx}
}

// Update updates the attribution state for a cell and is called whenever a value is changed. It returns the commit index
// that this change is now owned by as well as the index of the commit that owned it previously
func (att *cellAtt) Update(nbf *types.NomsBinFormat, commitIdx int16, oldVal, newVal types.Value) (int16, int16, error) {
	initialOwner := att.CurrentOwner

	if newVal == nil {
		return -1, att.CurrentOwner, att.Delete(nbf, oldVal)
	}

	newHash, err := newVal.Hash(nbf)

	if err != nil {
		return 0, 0, err
	}

	var oldHash hash.Hash
	if oldVal != nil {
		oldHash, err = oldVal.Hash(nbf)

		if err != nil {
			return 0, 0, err
		}
	}

	// Need to look at past values of the cell if they exist so if a value is reverted to a value it previously held
	// during the attribution period, the original commit that initially set this value receives the credit.
	if att.PastValues != nil {
		prevCommitAtNewHash, hasPrevCommitAtNewHash := att.PastValues[newHash]
		_, hasPrevCommitAtOldHash := att.PastValues[oldHash]

		// only update the past values if the value being replaced isn't already in the history
		if !hasPrevCommitAtOldHash {
			att.addPastValue(att.CurrentOwner, oldHash)
		}

		if hasPrevCommitAtNewHash {
			att.CurrentOwner = prevCommitAtNewHash
			return att.CurrentOwner, initialOwner, nil
		}
	} else {
		att.addPastValue(att.CurrentOwner, oldHash)
	}

	att.CurrentOwner = commitIdx
	return commitIdx, initialOwner, nil
}

func (att *cellAtt) addPastValue(commitIdx int16, h hash.Hash) {
	if !h.IsEmpty() {
		if att.PastValues == nil {
			att.PastValues = make(map[hash.Hash]int16)
		}

		att.PastValues[h] = commitIdx
	}
}

// Delete saves the past value of a cell so if it is reverted to that value in the future it can be given proper
// attribution
func (att *cellAtt) Delete(nbf *types.NomsBinFormat, oldVal types.Value)  error {
	oldHash, err := oldVal.Hash(nbf)

	if err != nil {
		return err
	}

	if att.PastValues == nil {
		att.PastValues = make(map[hash.Hash]int16)
	}

	att.PastValues[oldHash] = att.CurrentOwner
	att.CurrentOwner = -1
	return nil
}
