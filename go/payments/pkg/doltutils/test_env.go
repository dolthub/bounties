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
