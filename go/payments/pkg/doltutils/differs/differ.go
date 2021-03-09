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
