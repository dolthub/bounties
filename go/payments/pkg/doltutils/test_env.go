package doltutils

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dolthub/bounties/go/payments/pkg/attteststate"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/require"
)

const (
	testUsername = "Test User"
	testEmail    = "test@fake.horse"
)

func GetTestEnv(ctx context.Context, t *testing.T) *env.DoltEnv {
	const (
		homeDir         = "/Users/madam"
		relativeTestDir = "databases/test"
	)

	testDir := filepath.Join(homeDir, relativeTestDir)
	hdp := func() (string, error) { return homeDir, nil }
	fs := filesys.NewInMemFS([]string{testDir}, nil, testDir)
	dEnv := env.Load(ctx, hdp, fs, doltdb.InMemDoltDB, "")
	require.NoError(t, dEnv.DBLoadError)
	require.NoError(t, dEnv.CfgLoadErr)
	require.Error(t, dEnv.RSLoadErr)

	err := dEnv.InitDBAndRepoState(ctx, types.Format_DOLT, testUsername, testEmail, "main", time.Now())
	require.NoError(t, err)

	return dEnv
}

func CreateMeta(t *testing.T) []*datas.CommitMeta {
	meta := make([]*datas.CommitMeta, attteststate.GetNumCommits())
	for i := 0; i < len(meta); i++ {
		var err error
		meta[i], err = datas.NewCommitMeta(testUsername, testEmail, fmt.Sprintf("Commit %d", i))
		require.NoError(t, err)
	}

	return meta
}
