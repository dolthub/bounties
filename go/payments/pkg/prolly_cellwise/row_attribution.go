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

package prolly_cellwise

import (
	"bytes"
	"fmt"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"

	"strings"
)

// rowAtt is the attribution object for a row.  It maps from column index to commit idx.
type prollyRowAtt []*int16

func prollyRowAttFromValue(d val.TupleDesc, tuple val.Tuple) prollyRowAtt {
	att := make([]*int16, tuple.Count())
	for i := 0; i < tuple.Count(); i++ {
		if v, ok := d.GetInt16(i, tuple); ok {
			att[i] = &v
		}
	}
	return att
}

// updateFromDiff updates attribution based on a change to the row
func (ra prollyRowAtt) updateFromDiff(kd val.TupleDesc, commitIdx int16, difference tree.Diff) error {
	numPks := kd.Count()

	if difference.Type == tree.RemovedDiff {
		// Clear all owners
		for i := range ra {
			ra[i] = nil
		}
		return nil
	} else if difference.Type == tree.AddedDiff {
		// Set all pks as current owner
		ci := commitIdx
		for i := 0; i < numPks; i++ {
			ra[i] = &ci
		}

		v := val.Tuple(difference.To)
		if numPks+v.Count() != len(ra) {
			return fmt.Errorf("number of columns changed")
		}

		// Set non-null fields as current owner
		for i := 0; i < v.Count(); i++ {
			if !v.FieldIsNull(i) {
				ra[numPks+i] = &ci
			} else {
				ra[numPks+i] = nil
			}
		}

		return nil
	}

	old := val.Tuple(difference.From)
	new := val.Tuple(difference.To)

	if old.Count() != new.Count() {
		return fmt.Errorf("number of columns changed for key: %s", kd.Format(val.Tuple(difference.Key)))
	}

	if numPks+old.Count() != len(ra) {
		return fmt.Errorf("number of cell owners and cell count is different")
	}

	for i := 0; i < old.Count(); i++ {
		if bytes.Compare(old.GetField(i), new.GetField(i)) == 0 {
			// Skip any cells that haven't changed
			continue
		}

		if new.FieldIsNull(i) {
			// Clear current owner
			ra[numPks+i] = nil
		} else {
			// Set owner to current
			ci := commitIdx
			ra[numPks+i] = &ci
		}
	}

	return nil
}

func (ra prollyRowAtt) asTuple(b *val.TupleBuilder, pool pool.BuffPool) val.Tuple {
	for i, owner := range ra {
		if owner != nil {
			b.PutInt16(i, *owner)
		}
	}
	return b.Build(pool)
}

// String returns a string which can be printed to aid debugging
func (ra prollyRowAtt) String() string {
	sb := strings.Builder{}
	for i, currOwner := range ra {
		sb.WriteString(fmt.Sprintf("%d: %d, ", i, currOwner))
	}

	return sb.String()
}
