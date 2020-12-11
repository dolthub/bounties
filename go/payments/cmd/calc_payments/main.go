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
	"io/ioutil"
	"os"
	"path/filepath"
	
	"github.com/dolthub/bounties/go/payments/pkg/cellwise"
	"github.com/dolthub/bounties/go/payments/pkg/doltutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

func errExit(message string) {
	fmt.Fprintln(os.Stderr, message+"\n")
	os.Exit(1)
}

func main() {
	ctx := context.Background()

	method := flag.String("method", "", "The method used to calculate payments.  Supported options: 'cellwise'")
	repoDir := flag.String("repo-dir", "./", "Directory of the repository being examened")
	startHash := flag.String("start", "", "Commit hash representing the start of a bounty before any contributions are made.")
	endHash := flag.String("end", "", "Last commit hash included in the payment calculation.")
	incremental := flag.String("incremental", "", "When a directory is provided, incremental attribution is enabled and state files are read from and written to the provided directory.")
	flag.Parse()

	if len(*method) == 0 {
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
		errExit("Missing required parameter '-start'.")
	} else if end, ok = hash.MaybeParse(*endHash); !ok {
		errExit(fmt.Sprintf("Invalid hash: '%s'", *endHash))
	}

	absPath, err := validateDirectory(*repoDir)

	if err != nil {
		errExit(err.Error())
	}

	err = os.Chdir(absPath)

	if err != nil {
		errExit(fmt.Sprintf("Could not change path to '%s'", *repoDir))
	}

	absIncremental, err := validateDirectory(*incremental)

	if err != nil {
		errExit(err.Error())
	}

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "")

	if dEnv.CfgLoadErr != nil {
		errExit(fmt.Sprintf("Failed to load dolt configuration: %v", dEnv.CfgLoadErr))
	} else if dEnv.DBLoadError != nil {
		errExit(fmt.Sprintf("Failed to load dolt database: %v", dEnv.DBLoadError))
	} else if dEnv.DocsLoadErr != nil {
		errExit(fmt.Sprintf("Failed to load docs: %v", dEnv.DocsLoadErr))
	} else if dEnv.RSLoadErr != nil {
		errExit(fmt.Sprintf("Failed to load dolt repo state: %v", dEnv.RSLoadErr))
	}

	switch *method {
	case "cellwise":
		err = calcCellwisePayments(ctx, dEnv, start, end, absIncremental)
	default:
		errExit(fmt.Sprintf("Unknown --method '%s'", *method))
	}

	if err != nil {
		errExit(fmt.Sprintf("Error occurred while running calculations: %v", err))
	}
}

func calcCellwisePayments(ctx context.Context, dEnv *env.DoltEnv, start, end hash.Hash, incrementalDir string) error {
	mergeCommits, err := doltutils.GetMergeCommitsBetween(ctx, dEnv.DoltDB, start, end)

	if err != nil {
		return err
	}

	i, att, err := readIncremental(start, mergeCommits, incrementalDir)

	if err != nil {
		return err
	}

	for ; i >= 0; i-- {
		currHash, err := mergeCommits[i].HashOf()

		if err != nil {
			return err
		}

		err = att.Update(ctx, dEnv.DoltDB, start, currHash)

		if err != nil {
			return err
		}

		err = updateIncrementalDir(att, incrementalDir)

		if err != nil {
			return err
		}

		fmt.Printf("Attribution as of %s:\n", currHash.String())
		nameToCommitToCounts := att.GetCounts()
		for commit, count := range nameToCommitToCounts {
			fmt.Printf("\t%s: %d\n", commit.String(), count)
		}
	}

	return nil
}

func readIncremental(start hash.Hash, mergeCommits []*doltdb.Commit, incrementalDir string) (int, *cellwise.DatabaseAttribution, error) {
	if incrementalDir != "" {
		for i := 0; i < len(mergeCommits); i++ {
			commit := mergeCommits[i]
			h, err := commit.HashOf()

			if err != nil {
				return 0, nil, err
			}

			incrementalFile := filepath.Join(incrementalDir, start.String()+"_"+h.String())
			stat, err := os.Stat(incrementalFile)

			if err == nil {
				if stat.IsDir() {
					return 0, nil, fmt.Errorf("A directory exists at '%s'", incrementalFile)
				}

				data, err := ioutil.ReadFile(incrementalFile)

				if err != nil {
					return 0, nil, err
				}

				att, err := cellwise.Deserialize(data)

				if err != nil {
					return 0, nil, err
				}

				return i - 1, att, nil
			}
		}
	}

	return len(mergeCommits) - 1, cellwise.NewDatabaseAttribution(start), nil
}

func updateIncrementalDir(att *cellwise.DatabaseAttribution, incrementalDir string) error {
	if incrementalDir != "" {
		data, err := cellwise.Serialize(att)

		if err != nil {
			return err
		}

		start := att.AttribStartPoint.String()
		current := att.Commits[len(att.Commits)-1].String()
		incrementalFile := filepath.Join(incrementalDir, start+"_"+current)
		return ioutil.WriteFile(incrementalFile, data, os.ModePerm)
	}

	return nil
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
		return "", fmt.Errorf("Invalid repo dir '%s'", dir)
	} else if !stat.IsDir() {
		return "", fmt.Errorf("'%s' is a file.  Not a directory initialized as a valid dolt repo.", dir)
	}

	return absPath, err
}
