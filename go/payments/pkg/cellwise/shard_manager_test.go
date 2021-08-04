package cellwise

import (
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func pushN(n int, q *attQueue) int {
	for i := 0; i < n; i++ {
		q.Push(types.NullValue, types.NullValue, nil)
	}

	return n
}

func popN(n int, q *attQueue) int {
	for i := 0; i < n; i++ {
		arps := q.Pop()

		if arps == nil {
			return i
		}
	}

	return n
}

func TestAttQueue(t *testing.T) {
	const queueSize = 100
	q := newAttQueue(100)
	require.Equal(t, 0, q.Size())

	var n int
	n = pushN(queueSize, q)
	require.Equal(t, queueSize, n)
	require.Equal(t, queueSize, q.Size())

	n = popN(queueSize, q)
	require.Equal(t, queueSize, n)
	require.Equal(t, 0, q.Size())

	n = pushN(1, q)
	require.Equal(t, 1, n)
	require.Equal(t, 1, q.Size())

	n = popN(1, q)
	require.Equal(t, 1, n)
	require.Equal(t, 0, q.Size())

	n = pushN(40, q)
	require.Equal(t, 40, n)
	require.Equal(t, 40, q.Size())

	n = popN(15, q)
	require.Equal(t, 15, n)
	require.Equal(t, 25, q.Size())

	n = pushN(70, q)
	require.Equal(t, 70, n)
	require.Equal(t, 95, q.Size())

	n = pushN(5, q)
	require.Equal(t, 5, n)
	require.Equal(t, 100, q.Size())

	n = popN(q.Size(), q)
	require.Equal(t, 100, n)
	require.Equal(t, 0, q.Size())
}
