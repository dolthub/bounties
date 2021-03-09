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

func (att *cellAtt) AsValue(nbf *types.NomsBinFormat, buffs *rowAttEncodingBuffers) (types.Value, error) {
	buffs.cellVals = append(buffs.cellVals, types.Int(att.CurrentOwner))

	for h, n := range att.PastValues {
		hCopy := h
		buffs.cellVals = append(buffs.cellVals, types.InlineBlob(hCopy[:]))
		buffs.cellVals = append(buffs.cellVals, types.Int(n))
	}

	t, err := types.NewTuple(nbf, buffs.cellVals...)

	if err != nil {
		return nil, err
	}

	buffs.cellVals = buffs.cellVals[:0]
	return t, nil
}

func cellAttFromValue(v types.Value) (*cellAtt, error) {
	t := v.(types.Tuple)
	itr, err := t.Iterator()

	if err != nil {
		return nil, err
	}

	_, ownerVal, err := itr.Next()

	if err != nil {
		return nil, err
	}

	pastValues := make(map[hash.Hash]int16)
	for itr.HasMore() {
		_, hashBlobVal, err := itr.Next()

		if err != nil {
			return nil, err
		}

		hashBlob := hashBlobVal.(types.InlineBlob)
		h := hash.New(hashBlob)

		_, prevOwnerVal, err := itr.Next()

		if err != nil {
			return nil, err
		}

		pastValues[h] = int16(prevOwnerVal.(types.Int))
	}

	return &cellAtt{
		CurrentOwner: int16(ownerVal.(types.Int)),
		PastValues:   pastValues,
	}, nil
}

func newCellAtt(commitIdx int16) *cellAtt {
	return &cellAtt{CurrentOwner: commitIdx}
}

// Update updates the attribution state for a cell and is called whenever a value is changed. It returns the commit index
// that this change is now owned by as well as the index of the commit that owned it previously
func (att *cellAtt) Update(nbf *types.NomsBinFormat, commitIdx int16, oldVal, newVal types.Value) (owner int16, prevOwner int16, err error) {
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
	if len(att.PastValues) > 0 {
		prevCommitAtNewHash, hasPrevCommitAtNewHash := att.PastValues[newHash]

		if att.CurrentOwner != -1 && !oldHash.IsEmpty() {
			_, hasPrevCommitAtOldHash := att.PastValues[oldHash]

			// only update the past values if the value being replaced isn't already in the history
			if !hasPrevCommitAtOldHash {
				att.addPastValue(att.CurrentOwner, oldHash)
			}
		}

		if hasPrevCommitAtNewHash {
			att.CurrentOwner = prevCommitAtNewHash
			delete(att.PastValues, newHash)

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
func (att *cellAtt) Delete(nbf *types.NomsBinFormat, oldVal types.Value) error {
	oldHash, err := oldVal.Hash(nbf)

	if err != nil {
		return err
	}

	att.addPastValue(att.CurrentOwner, oldHash)
	att.CurrentOwner = -1
	return nil
}
