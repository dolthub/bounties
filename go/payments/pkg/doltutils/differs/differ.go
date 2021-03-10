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

package differs

import (
	"time"

	"github.com/dolthub/dolt/go/store/diff"
)

// Differ is an interface written to match what is already provided by the Dolt github.com/dolthub/dolt/go/libraries/doltcore/diff.AsyncDiffer
type Differ interface {
	// Close cleans up any resources
	Close() error
	// GetDiffs gets up to the specified number of diffs.  A timeout can be specified and if the requested number of diffs
	// are not available it will return what is available.  A timeout of 0 returns what is immediately available without waiting.
	// a timeout of -1 will wait indefinitely until the number of diffs are available, or it can return all remaining diffs
	GetDiffs(numDiffs int, timeout time.Duration) (diffs []*diff.Difference, more bool, err error)
}
