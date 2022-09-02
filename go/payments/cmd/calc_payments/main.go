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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dolthub/bounties/go/payments/pkg/prolly_cellwise"
	"github.com/pkg/profile"
	"go.uber.org/zap"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/cellwise"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type options struct {
	repoDir     string
	startHash   hash.Hash
	endHash     hash.Hash
	buildDir    string
	profile     string
	profileHash hash.Hash
}

func startProfiling(profileType string) func() {
	switch profileType {
	case "cpu":
		fmt.Println("cpu profiling enabled.")
		return profile.Start(profile.CPUProfile).Stop
	case "mem":
		fmt.Println("mem profiling enabled.")
		return profile.Start(profile.MemProfile).Stop
	case "blocking":
		fmt.Println("block profiling enabled")
		return profile.Start(profile.BlockProfile).Stop
	case "trace":
		fmt.Println("trace profiling enabled")
		return profile.Start(profile.TraceProfile).Stop
	default:
		panic("Unexpected prof flag: " + profileType)
	}
}

func errExit(message string) {
	fmt.Fprintln(os.Stderr, message+"\n")
	os.Exit(1)
}

func main() {
	ctx := context.Background()

	methodStr := flag.String("method", "", "The method used to calculate payments.  Supported options: 'cellwise', 'prolly'.")
	repoDir := flag.String("repo-dir", "./", "Directory of the repository.")
	startHash := flag.String("start", "", "Commit hash representing the start of a bounty before any contributions are made.")
	endHash := flag.String("end", "", "Last commit hash included in the payment calculation.")
	buildDir := flag.String("build-dir", "", "directory where build files are output.")
	profileType := flag.String("profile", "", "options are (cpu,mem,blocking,trace).")
	profileHashStr := flag.String("profile-hash", "", "commit hash to limit profiling to.")
	flag.Parse()

	if len(*methodStr) == 0 {
		errExit("Missing required parameter '-method'.")
	}

	var start hash.Hash
	var ok bool
	if len(*startHash) == 0 {
		errExit("Missing required parameter '-start'.")
	} else if start, ok = hash.MaybeParse(*startHash); !ok {
		errExit(fmt.Sprintf("Invalid hash: '%s'", *startHash))
	}

	var end hash.Hash
	if len(*endHash) == 0 {
		errExit("Missing required parameter '-end'.")
	} else if end, ok = hash.MaybeParse(*endHash); !ok {
		errExit(fmt.Sprintf("Invalid hash: '%s'", *endHash))
	}

	var profileHash hash.Hash
	if len(*profileHashStr) != 0 {
		profileHash, ok = hash.MaybeParse(*endHash)
		if !ok {
			errExit(fmt.Sprintf("Invalid hash: '%s'", *endHash))
		}
	}

	absPath, err := validateDirectory(*repoDir)
	if err != nil {
		errExit(err.Error())
	}

	err = os.Chdir(absPath)
	if err != nil {
		errExit(fmt.Sprintf("Could not change path to '%s'", *repoDir))
	}

	absBuildDir, err := validateDirectory(*buildDir)
	if err != nil {
		errExit(err.Error())
	}

	opts := options{
		repoDir:     absPath,
		startHash:   start,
		endHash:     end,
		buildDir:    absBuildDir,
		profile:     *profileType,
		profileHash: profileHash,
	}

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "")

	if dEnv.CfgLoadErr != nil {
		errExit(fmt.Sprintf("Failed to load dolt configuration: %v", dEnv.CfgLoadErr))
	} else if dEnv.DBLoadError != nil {
		errExit(fmt.Sprintf("Failed to load dolt database: %v", dEnv.DBLoadError))
	} else if dEnv.RSLoadErr != nil {
		errExit(fmt.Sprintf("Failed to load dolt repo state: %v", dEnv.RSLoadErr))
	}

	shardStore, err := att.NewFilesysShardStore(filepath.Join(opts.buildDir, opts.startHash.String()))
	if err != nil {
		errExit(fmt.Sprintf("Failed to create local shardstore using the directory '%s': %v", opts.buildDir, err))
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		errExit(fmt.Sprintf("Failed to create logger: %s", err.Error()))
	}

	var method att.AttributionMethod
	switch *methodStr {
	case "cellwise":
		shardParams := cellwise.CWAttShardParams{
			RowsPerShard:       100_000,
			SubdivideDiffsSize: 1_000_000,
		}
		method = cellwise.NewCWAtt(logger, dEnv.DoltDB, opts.startHash, shardStore, shardParams)
	case "prolly":
		shardParams := prolly_cellwise.ProllyAttShardParams{
			RowsPerShard: 100_000,
		}
		method = prolly_cellwise.NewMethod(logger, dEnv.DoltDB, opts.startHash, shardStore, shardParams)
	default:
		errExit(fmt.Sprintf("Unknown --method '%s'", *methodStr))
	}

	err = calcAttribution(ctx, method, dEnv.DoltDB, opts)
	if err != nil {
		errExit(fmt.Sprintf("Error occurred while running calculations: %v", err))
	}
}

func calcAttribution(ctx context.Context, method att.AttributionMethod, ddb *doltdb.DoltDB, opts options) error {
	// mergeCommits will be ordered from least recent to most recent
	mergeCommits, err := doltutils.GetMergeCommitsBetween(ctx, ddb, opts.startHash, opts.endHash)
	if err != nil {
		return err
	}

	if opts.profile != "" && opts.profileHash.IsEmpty() {
		stopProfFunc := startProfiling(opts.profile)
		defer stopProfFunc()
	}

	prevCommitIdx, summary, err := readLatestSummary(ctx, method, mergeCommits)
	if err != nil {
		return err
	}

	var prevCommit *doltdb.Commit
	if prevCommitIdx >= 0 {
		prevCommit = mergeCommits[prevCommitIdx]
	} else {
		cs, err := doltdb.NewCommitSpec(opts.startHash.String())
		if err != nil {
			return err
		}
		prevCommit, err = ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return err
		}

		summary = method.EmptySummary(ctx)
	}

	fmt.Println("Processing", len(mergeCommits)-prevCommitIdx-1, "merge commits.")
	for commitIdx := prevCommitIdx + 1; commitIdx < len(mergeCommits); commitIdx++ {
		start := time.Now()

		commit := mergeCommits[commitIdx]
		commitHash, err := commit.HashOf()
		if err != nil {
			return err
		}

		var prevCommitHash hash.Hash
		if prevCommit != nil {
			prevCommitHash, err = prevCommit.HashOf()
			if err != nil {
				return err
			}
		}

		fmt.Println("Processing:", commitHash.String(), "parent:", prevCommitHash.String())

		shardInfo, err := method.CollectShards(ctx, commit, prevCommit, summary)
		if err != nil {
			return err
		}

		var results []att.ShardResult
		for _, shard := range shardInfo {
			result, err := method.ProcessShard(ctx, int16(commitIdx), commit, prevCommit, shard)
			if err != nil {
				return err
			}

			results = append(results, result)
		}

		summary, err = method.ProcessResults(ctx, commitHash, summary, results)
		if err != nil {
			return err
		}

		_, err = method.WriteSummary(ctx, summary)
		if err != nil {
			return err
		}

		printSummaryInfo(ctx, summary, mergeCommits[:commitIdx+1])
		fmt.Printf("Processed commit %s (%d/%d) in %s\n", commitHash.String(), commitIdx+1, len(mergeCommits), time.Since(start))

		prevCommit = commit
	}

	fmt.Println("Done!")

	return nil
}

func readLatestSummary(ctx context.Context, method att.AttributionMethod, mergeCommits []*doltdb.Commit) (int, att.Summary, error) {
	for i := len(mergeCommits) - 1; i >= 0; i-- {
		cm := mergeCommits[i]
		h, err := cm.HashOf()
		if err != nil {
			return 0, nil, err
		}

		summary, err := method.ReadSummary(ctx, h.String()+".summary")
		if err == att.ErrSummaryDoesntExist {
			continue
		} else if err != nil {
			return 0, nil, err
		}

		return i, summary, nil
	}

	return -1, nil, nil
}

func printSummaryInfo(ctx context.Context, summary att.Summary, commits []*doltdb.Commit) {
	commitToCount, err := summary.CommitToCount(ctx)
	if err != nil {
		panic(err)
	}

	for i, commit := range commits {
		h, err := commit.HashOf()
		if err != nil {
			panic(err)
		}

		count, ok := commitToCount[h]

		if !ok {
			panic("failed to find count for " + h.String())
		}

		fmt.Printf("%02d. %s: %d\n", i, h.String(), count)
	}
}

func validateDirectory(dir string) (string, error) {
	if dir == "" {
		return "", nil
	}

	absPath, err := filepath.Abs(dir)
	if err != nil {
		errExit(fmt.Sprintf("Invalid path '%s'", dir))
	}

	stat, err := os.Stat(absPath)
	if err != nil {
		return "", fmt.Errorf("Invalid dir '%s'", dir)
	} else if !stat.IsDir() {
		return "", fmt.Errorf("'%s' is a file.  Not a directory initialized as a valid dolt repo.", dir)
	}

	return absPath, err
}
