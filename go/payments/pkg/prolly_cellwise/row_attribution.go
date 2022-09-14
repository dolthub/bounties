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
	att := make([]*int16, d.Count())
	for i := 0; i < d.Count(); i++ {
		if v, ok := d.GetInt16(i, tuple); ok {
			v2 := v
			att[i] = &v2
		}
	}
	return att
}

// updateFromDiff updates attribution based on a change to the row
func (ra prollyRowAtt) updateFromDiff(kd, vd val.TupleDesc, commitIdx int16, difference tree.Diff) error {
	numPks := kd.Count()

	if numPks+vd.Count() != len(ra) {
		return fmt.Errorf("number of cells and cell owners are different")
	}

	if difference.Type == tree.AddedDiff {
		// Set all pks as current owner
		ci := commitIdx
		for i := 0; i < numPks; i++ {
			ra[i] = &ci
		}

		v := val.Tuple(difference.To)

		// Set non-null fields as current owner
		for i := 0; i < vd.Count(); i++ {
			if !vd.IsNull(i, v) {
				ra[numPks+i] = &ci
			} else {
				ra[numPks+i] = nil
			}
		}

		return nil
	}

	old := val.Tuple(difference.From)
	new := val.Tuple(difference.To)

	for i := 0; i < vd.Count(); i++ {
		if bytes.Compare(vd.GetField(i, old), vd.GetField(i, new)) == 0 {
			// Skip any cells that haven't changed
			continue
		}

		if vd.IsNull(i, new) {
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
